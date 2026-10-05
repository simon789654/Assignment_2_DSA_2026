// =====================================================================
// DELIVERY SERVICE (:8085)
// Driver management, optimised dispatch, real-time delivery tracking.
//
// Consumes: orders.status-changed (READY -> create delivery + dispatch a driver)
// Produces: delivery.assigned, delivery.picked-up, delivery.completed,
//           drivers.availability (for surge pricing), driver.location (simulator.bal)
//
// Dispatch: every AVAILABLE driver is ranked by A* travel time to the restaurant
// (routing.bal). The fastest one is claimed with an atomic Mongo update
// (status AVAILABLE -> BUSY), so two concurrent dispatches can never grab the
// same driver. If nobody is free the delivery stays PENDING and a background
// loop retries every few seconds.
// =====================================================================
import ballerina/http;
import ballerina/lang.runtime;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerinax/prometheus as _;

configurable string kafkaUrl = "localhost:29092";
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable int httpPort = 8085;
configurable string restaurantUrl = "http://localhost:8082";
// true = drivers automatically pick up and complete when the simulation arrives (great for demos)
configurable boolean autoDrive = true;

const AVAILABLE = "AVAILABLE";
const BUSY = "BUSY";
const OFFLINE = "OFFLINE";

const PENDING = "PENDING";
const ASSIGNED = "ASSIGNED";
const PICKED_UP = "PICKED_UP";
const DELIVERED = "DELIVERED";

// ---------------------------------------------------------------- types
type Driver record {|
    string driverId;
    string name;
    string phone;
    string vehicle;
    string status;      // AVAILABLE | BUSY | OFFLINE
    string homeNodeId;
|};

type NewDriver record {|
    string driverId?;
    string name;
    string phone;
    string vehicle = "Motorbike";
    string homeNodeId = "cbd";
|};

type AvailabilityUpdate record {|
    string status;      // AVAILABLE | OFFLINE
|};

type Delivery record {|
    string deliveryId;
    string orderId;
    string customerId;
    string restaurantId;
    string? driverId;
    string status;      // PENDING | ASSIGNED | PICKED_UP | DELIVERED
    string restaurantNodeId;
    string customerNodeId;
    Route? routeToRestaurant;
    Route? routeToCustomer;
    float etaMinutes;
    string createdAt;
    string? assignedAt;
    string? pickedUpAt;
    string? deliveredAt;
|};

type OrderEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    string status;
    string? deliveryNodeId = ();
};

type DeliveryEvent record {|
    string eventId;
    string eventType;
    string deliveryId;
    string orderId;
    string customerId;
    string restaurantId;
    string driverId;
    float etaMinutes;
    float? durationMinutes;
    string timestamp;
|};

// ---------------------------------------------------------------- clients
final kafka:Producer producer = check new (kafkaUrl, {clientId: "delivery-service", acks: kafka:ACKS_ALL});

final mongodb:Client mongoClient = check new ({connection: {serverAddress: {host: mongoHost, port: mongoPort}}});
final mongodb:Collection drivers = check collection("drivers");
final mongodb:Collection deliveries = check collection("deliveries");

function collection(string name) returns mongodb:Collection|error {
    mongodb:Database database = check mongoClient->getDatabase("delivery_db");
    return database->getCollection(name);
}

final http:Client restaurantClient = check new (restaurantUrl, {timeout: 5});

// ---------------------------------------------------------------- helpers
function now() returns string => time:utcToString(time:utcNow());

function findDrivers(map<json> filter) returns Driver[]|error {
    stream<Driver, error?> s = check drivers->find(filter, {}, {_id: 0});
    return from Driver d in s select d;
}

function findDriver(string driverId) returns Driver?|error {
    return drivers->findOne({driverId}, {}, {_id: 0});
}

function findDeliveries(map<json> filter) returns Delivery[]|error {
    stream<Delivery, error?> s = check deliveries->find(filter, {}, {_id: 0});
    return from Delivery d in s select d;
}

function findDelivery(map<json> filter) returns Delivery?|error {
    return deliveries->findOne(filter, {}, {_id: 0});
}

function emit(string topic, Delivery d, float? durationMinutes = ()) returns error? {
    DeliveryEvent event = {eventId: uuid:createType4AsString(), eventType: topic, deliveryId: d.deliveryId,
        orderId: d.orderId, customerId: d.customerId, restaurantId: d.restaurantId, driverId: d.driverId ?: "",
        etaMinutes: d.etaMinutes, durationMinutes, timestamp: now()};
    check producer->send({topic, key: d.orderId.toBytes(), value: event.toJsonString().toBytes()});
}

// Feeds the Order Service's surge pricing
function publishAvailability() returns error? {
    int available = check drivers->countDocuments({status: AVAILABLE});
    check producer->send({topic: "drivers.availability", key: "drivers".toBytes(),
        value: {available, timestamp: now()}.toJsonString().toBytes()});
}

function restaurantNode(string restaurantId) returns string {
    json|error r = restaurantClient->get(string `/restaurants/${restaurantId}`);
    if r is map<json> {
        json node = r["nodeId"];
        if node is string && NODE_BY_ID.hasKey(node) {
            return node;
        }
    }
    log:printWarn("Restaurant location unknown, defaulting to CBD", restaurantId = restaurantId);
    return "cbd";
}

// ---------------------------------------------------------------- dispatch
function tryAssign(Delivery d) returns boolean|error {
    Driver[] free = check findDrivers({status: AVAILABLE});
    if free.length() == 0 {
        return false;
    }
    // Rank every free driver by A* travel time to the restaurant
    [string, Route][] ranked = [];
    foreach Driver drv in free {
        DriverLocation? loc = getDriverLocation(drv.driverId);
        string startNode = loc is DriverLocation ? nearestNode(loc.lat, loc.lng) : drv.homeNodeId;
        Route r = check shortestRoute(startNode, d.restaurantNodeId);
        ranked.push([drv.driverId, r]);
    }
    ranked = from [string, Route] c in ranked order by c[1].etaMinutes ascending select c;

    foreach [string, Route] [driverId, toRestaurant] in ranked {
        // Atomic claim: only succeeds if the driver is still AVAILABLE
        mongodb:UpdateResult claim = check drivers->updateOne({driverId, status: AVAILABLE}, {set: {status: BUSY}});
        if claim.matchedCount == 0 {
            continue;
        }
        Route toCustomer = check shortestRoute(d.restaurantNodeId, d.customerNodeId);
        float eta = toRestaurant.etaMinutes + toCustomer.etaMinutes;
        mongodb:UpdateResult upd = check deliveries->updateOne({deliveryId: d.deliveryId, status: PENDING}, {
            set: {driverId, status: ASSIGNED, routeToRestaurant: toRestaurant.toJson(),
                routeToCustomer: toCustomer.toJson(), etaMinutes: eta, assignedAt: now()}
        });
        if upd.matchedCount == 0 {
            // someone else already assigned this delivery -> release the driver
            _ = check drivers->updateOne({driverId}, {set: {status: AVAILABLE}});
            return true;
        }
        Delivery assigned = d.clone();
        assigned.driverId = driverId;
        assigned.status = ASSIGNED;
        assigned.routeToRestaurant = toRestaurant;
        assigned.routeToCustomer = toCustomer;
        assigned.etaMinutes = eta;
        assigned.assignedAt = now();
        check emit("delivery.assigned", assigned);
        check publishAvailability();
        log:printInfo("Driver assigned", orderId = d.orderId, driverId = driverId, etaMinutes = eta);

        string deliveryId = d.deliveryId;
        function () returns error? onArrive = autoDrive
            ? function() returns error? {
                Delivery|http:Conflict|error r = pickup(deliveryId);
                if r is error {
                    return r;
                }
            }
            : function() returns error? {};
        _ = start simulateTrip(driverId, deliveryId, toRestaurant, "TO_RESTAURANT", onArrive);
        return true;
    }
    return false;
}

function assignPending() returns error? {
    Delivery[] waiting = check findDeliveries({status: PENDING});
    foreach Delivery d in waiting {
        boolean assigned = check tryAssign(d);
        if !assigned {
            return; // no drivers left, stop for now
        }
    }
}

function pendingLoop() {
    while true {
        runtime:sleep(5);
        error? e = assignPending();
        if e is error {
            log:printError("Pending dispatch failed", e);
        }
    }
}

function pickup(string deliveryId) returns Delivery|http:Conflict|error {
    mongodb:UpdateResult res = check deliveries->updateOne({deliveryId, status: ASSIGNED},
        {set: {status: PICKED_UP, pickedUpAt: now()}});
    if res.matchedCount == 0 {
        return <http:Conflict>{body: {message: "Delivery is not waiting for pickup"}};
    }
    Delivery? d = check findDelivery({deliveryId});
    if d is () {
        return error("Delivery vanished");
    }
    check emit("delivery.picked-up", d);
    string? driverId = d.driverId;
    Route? toCustomer = d.routeToCustomer;
    if driverId is string && toCustomer is Route {
        function () returns error? onArrive = autoDrive
            ? function() returns error? {
                Delivery|http:Conflict|error r = complete(deliveryId);
                if r is error {
                    return r;
                }
            }
            : function() returns error? {};
        _ = start simulateTrip(driverId, deliveryId, toCustomer, "TO_CUSTOMER", onArrive);
    }
    return d;
}

function complete(string deliveryId) returns Delivery|http:Conflict|error {
    mongodb:UpdateResult res = check deliveries->updateOne({deliveryId, status: PICKED_UP},
        {set: {status: DELIVERED, deliveredAt: now()}});
    if res.matchedCount == 0 {
        return <http:Conflict>{body: {message: "Delivery has not been picked up"}};
    }
    Delivery? d = check findDelivery({deliveryId});
    if d is () {
        return error("Delivery vanished");
    }
    // Simulated duration: real seconds scaled by the simulator's time-lapse factor
    float? realMinutes = ();
    string? assignedAt = d.assignedAt;
    string? deliveredAt = d.deliveredAt;
    if assignedAt is string && deliveredAt is string {
        time:Utc|error a = time:utcFromString(assignedAt);
        time:Utc|error b = time:utcFromString(deliveredAt);
        if a is time:Utc && b is time:Utc {
            realMinutes = <float>time:utcDiffSeconds(b, a) * SIM_SPEEDUP / 60.0;
        }
    }
    string? driverId = d.driverId;
    if driverId is string {
        stopTrip(driverId);
        _ = check drivers->updateOne({driverId, status: BUSY}, {set: {status: AVAILABLE}});
        check setDriverPhase(driverId, "IDLE");
    }
    check emit("delivery.completed", d, realMinutes);
    check publishAvailability();
    error? e = assignPending(); // a driver just became free
    if e is error {
        log:printError("Re-dispatch failed", e);
    }
    return d;
}

// Resume simulations for deliveries that were in progress when the service restarted
function resumeActive() returns error? {
    Delivery[] active = check findDeliveries({status: {"$in": [ASSIGNED, PICKED_UP]}});
    foreach Delivery d in active {
        string? driverId = d.driverId;
        Route? leg = d.status == ASSIGNED ? d.routeToRestaurant : d.routeToCustomer;
        if driverId is string && leg is Route {
            string deliveryId = d.deliveryId;
            boolean toRestaurant = d.status == ASSIGNED;
            function () returns error? onArrive = function() returns error? {
                if !autoDrive {
                    return;
                }
                Delivery|http:Conflict|error r = toRestaurant ? pickup(deliveryId) : complete(deliveryId);
                if r is error {
                    return r;
                }
            };
            _ = start simulateTrip(driverId, deliveryId, leg, toRestaurant ? "TO_RESTAURANT" : "TO_CUSTOMER", onArrive);
        }
    }
}

function init() returns error? {
    int existingCount = check drivers->countDocuments({});
    if existingCount == 0 {
        Driver[] seed = [
            {driverId: "d1", name: "Tangeni", phone: "+264814000001", vehicle: "Motorbike", status: AVAILABLE, homeNodeId: "cbd"},
            {driverId: "d2", name: "Ndapewa", phone: "+264814000002", vehicle: "Motorbike", status: AVAILABLE, homeNodeId: "katutura"},
            {driverId: "d3", name: "Johannes", phone: "+264814000003", vehicle: "Car", status: AVAILABLE, homeNodeId: "olympia"},
            {driverId: "d4", name: "Selma", phone: "+264814000004", vehicle: "Motorbike", status: AVAILABLE, homeNodeId: "khomasdal"},
            {driverId: "d5", name: "Petrus", phone: "+264814000005", vehicle: "Bicycle", status: AVAILABLE, homeNodeId: "academia"}
        ];
        foreach Driver drv in seed {
            check drivers->insertOne(drv);
        }
        log:printInfo("Seeded demo drivers");
    }
    foreach Driver drv in check findDrivers({}) {
        check placeDriver(drv.driverId, drv.homeNodeId);
        if drv.status == OFFLINE {
            check setDriverPhase(drv.driverId, OFFLINE);
        }
    }
    check publishAvailability();
    check resumeActive();
    _ = start pendingLoop();
}

// ---------------------------------------------------------------- REST API
service / on new http:Listener(httpPort) {

    // --- routing (bonus: route optimisation)
    resource function get routes/nodes() returns Node[] {
        return allNodes();
    }

    resource function get routes(string 'from, string to) returns Route|http:BadRequest {
        Route|error r = shortestRoute('from, to);
        return r is error ? <http:BadRequest>{body: {message: r.message()}} : r;
    }

    // --- drivers
    resource function get drivers() returns Driver[]|error {
        return findDrivers({});
    }

    resource function post drivers(NewDriver req) returns http:Created|http:BadRequest|http:Conflict|error {
        if !NODE_BY_ID.hasKey(req.homeNodeId) {
            return <http:BadRequest>{body: {message: "Unknown homeNodeId (see GET /routes/nodes)"}};
        }
        string id = req?.driverId ?: uuid:createType4AsString();
        if check findDriver(id) is Driver {
            return <http:Conflict>{body: {message: "Driver already exists"}};
        }
        Driver drv = {driverId: id, name: req.name, phone: req.phone, vehicle: req.vehicle, status: AVAILABLE,
            homeNodeId: req.homeNodeId};
        check drivers->insertOne(drv);
        check placeDriver(id, req.homeNodeId);
        check publishAvailability();
        check assignPending();
        return <http:Created>{body: drv};
    }

    resource function get drivers/locations() returns DriverLocation[] {
        return getDriverLocations();
    }

    resource function patch drivers/[string driverId]/availability(AvailabilityUpdate req)
            returns Driver|http:NotFound|http:Conflict|http:BadRequest|error {
        if req.status != AVAILABLE && req.status != OFFLINE {
            return <http:BadRequest>{body: {message: "status must be AVAILABLE or OFFLINE"}};
        }
        Driver? drv = check findDriver(driverId);
        if drv is () {
            return <http:NotFound>{body: {message: "Driver not found"}};
        }
        if drv.status == BUSY {
            return <http:Conflict>{body: {message: "Driver is on a delivery"}};
        }
        _ = check drivers->updateOne({driverId, status: drv.status}, {set: {status: req.status}});
        check setDriverPhase(driverId, req.status == AVAILABLE ? "IDLE" : OFFLINE);
        check publishAvailability();
        if req.status == AVAILABLE {
            check assignPending();
        }
        Driver updated = drv.clone();
        updated.status = req.status;
        return updated;
    }

    // --- deliveries
    resource function get deliveries(string? driverId, string? status) returns Delivery[]|error {
        map<json> filter = {};
        if driverId is string {
            filter["driverId"] = driverId;
        }
        if status is string {
            filter["status"] = status;
        }
        return findDeliveries(filter);
    }

    resource function get deliveries/'order/[string orderId]() returns Delivery|http:NotFound|error {
        Delivery? d = check findDelivery({orderId});
        return d is () ? <http:NotFound>{body: {message: "No delivery for this order yet"}} : d;
    }

    resource function patch deliveries/[string deliveryId]/pickup() returns Delivery|http:Conflict|error {
        return pickup(deliveryId);
    }

    resource function patch deliveries/[string deliveryId]/complete() returns Delivery|http:Conflict|error {
        return complete(deliveryId);
    }
}

// ---------------------------------------------------------------- Kafka consumer
listener kafka:Listener orderListener = new (kafkaUrl, {
    groupId: "delivery-service",
    topics: ["orders.status-changed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false
});

service kafka:Service on orderListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string raw = check string:fromBytes(rec.value);
            OrderEvent|error e = raw.fromJsonStringWithType(OrderEvent);
            if e is error {
                log:printError("Malformed order event", e);
                continue;
            }
            if e.status != "READY" {
                continue;
            }
            error? err = createDelivery(e);
            if err is error {
                log:printError("Could not create delivery", err, orderId = e.orderId);
            }
        }
        check caller->commit();
    }
}

function createDelivery(OrderEvent e) returns error? {
    // Idempotent: one delivery per order even if READY is redelivered
    int existingCount = check deliveries->countDocuments({orderId: e.orderId});
    if existingCount > 0 {
        return;
    }
    string? requested = e.deliveryNodeId;
    string customerNode = requested is string && NODE_BY_ID.hasKey(requested) ? requested : "cbd";
    Delivery d = {deliveryId: uuid:createType4AsString(), orderId: e.orderId, customerId: e.customerId,
        restaurantId: e.restaurantId, driverId: (), status: PENDING, restaurantNodeId: restaurantNode(e.restaurantId),
        customerNodeId: customerNode, routeToRestaurant: (), routeToCustomer: (), etaMinutes: 0.0,
        createdAt: now(), assignedAt: (), pickedUpAt: (), deliveredAt: ()};
    check deliveries->insertOne(d);
    boolean assigned = check tryAssign(d);
    if !assigned {
        log:printWarn("No driver available, delivery queued", orderId = e.orderId);
    }
}
