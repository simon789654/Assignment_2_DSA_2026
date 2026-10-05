// =====================================================================
// ADMIN SERVICE (:8087)
// Reports on restaurant statistics and delivery performance.
//
// CQRS read model: consumes order, kitchen, payment and delivery events into
// its own "orderFacts" / "deliveryFacts" collections, then computes reports
// from those. It never queries other services' databases.
// =====================================================================
import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerinax/prometheus as _;

configurable string kafkaUrl = "localhost:29092";
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable int httpPort = 8087;
configurable string restaurantUrl = "http://localhost:8082";
configurable string deliveryUrl = "http://localhost:8085";

type OrderFact record {|
    string orderId;
    string restaurantId;
    string customerId;
    decimal total;
    decimal surgeFee;
    string status;
    string createdAt;
    string? confirmedAt;
    string? preparingAt;
    string? readyAt;
    string? deliveredAt;
|};

type DeliveryFact record {|
    string deliveryId;
    string orderId;
    string driverId;
    float durationMinutes;
    string completedAt;
|};

type RestaurantReport record {|
    string restaurantId;
    string name;
    int ordersCount;
    int cancelledCount;
    decimal revenue;
    float avgPrepMinutes;
|};

type DriverReport record {|
    string driverId;
    string name;
    int deliveries;
    float avgDeliveryMinutes;
|};

type Summary record {|
    int totalOrders;
    int delivered;
    int cancelled;
    int inProgress;
    decimal revenue;
    float avgEndToEndMinutes;
    map<int> byStatus;
|};

type AnyEvent record {
    string orderId;
    string? restaurantId = ();
    string? customerId = ();
    string? status = ();
    decimal? total = ();
    decimal? deliveryFee = ();
    string? deliveryId = ();
    string? driverId = ();
    float? durationMinutes = ();
    string? timestamp = ();
};

final mongodb:Client mongoClient = check new ({connection: {serverAddress: {host: mongoHost, port: mongoPort}}});
final mongodb:Collection orderFacts = check collection("orderFacts");
final mongodb:Collection deliveryFacts = check collection("deliveryFacts");

function collection(string name) returns mongodb:Collection|error {
    mongodb:Database database = check mongoClient->getDatabase("admin_db");
    return database->getCollection(name);
}

final http:Client restaurantClient = check new (restaurantUrl, {timeout: 3});
final http:Client deliveryClient = check new (deliveryUrl, {timeout: 3});

function now() returns string => time:utcToString(time:utcNow());

function minutesBetween(string? fromTs, string? toTs) returns float? {
    if fromTs is () || toTs is () {
        return ();
    }
    time:Utc|error a = time:utcFromString(fromTs);
    time:Utc|error b = time:utcFromString(toTs);
    if a is error || b is error {
        return ();
    }
    return <float>time:utcDiffSeconds(b, a) / 60.0;
}

function avg(float[] xs) returns float {
    if xs.length() == 0 {
        return 0.0;
    }
    float total = 0.0;
    foreach float x in xs {
        total += x;
    }
    return float:round(total / <float>xs.length() * 10.0) / 10.0;
}

// Display names via REST (falls back to ids if the other service is down)
function namesFrom(http:Client c, string path, string idField) returns map<string> {
    json|error list = c->get(path);
    map<string> names = {};
    if list is json[] {
        foreach json item in list {
            if item is map<json> {
                json id = item[idField];
                json name = item["name"];
                if id is string && name is string {
                    names[id] = name;
                }
            }
        }
    }
    return names;
}

function allOrderFacts() returns OrderFact[]|error {
    stream<OrderFact, error?> s = check orderFacts->find({}, {}, {_id: 0});
    return from OrderFact f in s select f;
}

service /reports on new http:Listener(httpPort) {

    resource function get restaurants() returns RestaurantReport[]|error {
        OrderFact[] facts = check allOrderFacts();
        map<string> names = namesFrom(restaurantClient, "/restaurants", "restaurantId");
        map<OrderFact[]> byRestaurant = {};
        foreach OrderFact f in facts {
            OrderFact[] group = byRestaurant[f.restaurantId] ?: [];
            group.push(f);
            byRestaurant[f.restaurantId] = group;
        }
        RestaurantReport[] report = [];
        foreach [string, OrderFact[]] [rid, group] in byRestaurant.entries() {
            decimal revenue = 0;
            int cancelled = 0;
            float[] prep = [];
            foreach OrderFact f in group {
                if f.status == "CANCELLED" {
                    cancelled += 1;
                } else if f.confirmedAt is string {
                    revenue += f.total;
                }
                float? m = minutesBetween(f.preparingAt, f.readyAt);
                if m is float {
                    prep.push(m);
                }
            }
            report.push({restaurantId: rid, name: names[rid] ?: rid, ordersCount: group.length() - cancelled,
                cancelledCount: cancelled, revenue, avgPrepMinutes: avg(prep)});
        }
        return from RestaurantReport r in report order by r.revenue descending select r;
    }

    resource function get drivers() returns DriverReport[]|error {
        stream<DeliveryFact, error?> s = check deliveryFacts->find({}, {}, {_id: 0});
        DeliveryFact[] facts = check from DeliveryFact f in s select f;
        map<string> names = namesFrom(deliveryClient, "/drivers", "driverId");
        map<float[]> durations = {};
        foreach DeliveryFact f in facts {
            float[] d = durations[f.driverId] ?: [];
            d.push(f.durationMinutes);
            durations[f.driverId] = d;
        }
        DriverReport[] report = from [string, float[]] [driverId, d] in durations.entries()
            select {driverId, name: names[driverId] ?: driverId, deliveries: d.length(), avgDeliveryMinutes: avg(d)};
        return from DriverReport r in report order by r.deliveries descending select r;
    }

    resource function get summary() returns Summary|error {
        OrderFact[] facts = check allOrderFacts();
        map<int> byStatus = {};
        decimal revenue = 0;
        float[] endToEnd = [];
        foreach OrderFact f in facts {
            byStatus[f.status] = (byStatus[f.status] ?: 0) + 1;
            if f.status != "CANCELLED" && f.confirmedAt is string {
                revenue += f.total;
            }
            float? m = minutesBetween(f.createdAt, f.deliveredAt);
            if m is float {
                endToEnd.push(m);
            }
        }
        int delivered = byStatus["DELIVERED"] ?: 0;
        int cancelled = byStatus["CANCELLED"] ?: 0;
        return {totalOrders: facts.length(), delivered, cancelled,
            inProgress: facts.length() - delivered - cancelled, revenue, avgEndToEndMinutes: avg(endToEnd), byStatus};
    }
}

// ---------------------------------------------------------------- Kafka consumer (read model)
listener kafka:Listener eventListener = new (kafkaUrl, {
    groupId: "admin-service",
    topics: ["orders.created", "orders.status-changed", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false
});

service kafka:Service on eventListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
            string raw = check string:fromBytes(rec.value);
            AnyEvent|error e = raw.fromJsonStringWithType(AnyEvent);
            if e is error {
                log:printError("Malformed event", e, topic = topic);
                continue;
            }
            error? err = project(topic, e);
            if err is error {
                log:printError("Projection failed", err, topic = topic, orderId = e.orderId);
            }
        }
        check caller->commit();
    }
}

function project(string topic, AnyEvent e) returns error? {
    string ts = e.timestamp ?: now();
    if topic == "orders.created" {
        int existingCount = check orderFacts->countDocuments({orderId: e.orderId});
        if existingCount > 0 {
            return; // idempotent
        }
        OrderFact f = {orderId: e.orderId, restaurantId: e.restaurantId ?: "unknown", customerId: e.customerId ?: "unknown",
            total: e.total ?: 0, surgeFee: e.deliveryFee ?: 0, status: "CREATED", createdAt: ts,
            confirmedAt: (), preparingAt: (), readyAt: (), deliveredAt: ()};
        check orderFacts->insertOne(f);
    } else if topic == "orders.status-changed" {
        // Different topics have no ordering guarantee between them, so a status change can
        // arrive before orders.created. Create the fact on first sight in that case.
        int existingCount = check orderFacts->countDocuments({orderId: e.orderId});
        if existingCount == 0 {
            check project("orders.created", e);
        }
        string status = e.status ?: "UNKNOWN";
        map<json> changes = {status};
        match status {
            "CONFIRMED" => {
                changes["confirmedAt"] = ts;
            }
            "PREPARING" => {
                changes["preparingAt"] = ts;
            }
            "READY" => {
                changes["readyAt"] = ts;
            }
            "DELIVERED" => {
                changes["deliveredAt"] = ts;
            }
        }
        _ = check orderFacts->updateOne({orderId: e.orderId}, {set: changes});
    } else if topic == "delivery.completed" {
        string? deliveryId = e.deliveryId;
        string? driverId = e.driverId;
        if deliveryId is () || driverId is () {
            return;
        }
        int existingCount = check deliveryFacts->countDocuments({deliveryId});
        if existingCount > 0 {
            return;
        }
        DeliveryFact f = {deliveryId, orderId: e.orderId, driverId, durationMinutes: e.durationMinutes ?: 0.0,
            completedAt: ts};
        check deliveryFacts->insertOne(f);
    }
}
