// =====================================================================
// NOTIFICATION SERVICE (:8086)
// Turns domain events into alerts for customers, restaurants and drivers.
// Channels are simulated (logged + stored): SMS for customers,
// EMAIL for restaurants, PUSH for drivers.
//
// Consumes (own consumer group, so it sees every event):
//   orders.created, orders.status-changed, payments.failed, payments.refunded,
//   delivery.assigned
// =====================================================================
import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerinax/prometheus as _;

configurable string kafkaUrl = "localhost:29092";
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable int httpPort = 8086;

type Notification record {|
    string notificationId;
    string recipientType;   // CUSTOMER | RESTAURANT | DRIVER
    string recipientId;
    string channel;         // SMS | EMAIL | PUSH
    string orderId;
    string message;
    string sentAt;
|};

// One loose shape for every incoming event; fields missing in a given event are nil.
type DomainEvent record {
    string orderId;
    string? customerId = ();
    string? restaurantId = ();
    string? driverId = ();
    string? status = ();
    decimal? total = ();
    decimal? amount = ();
    float? etaMinutes = ();
    string? reason = ();
};

final mongodb:Client mongoClient = check new ({connection: {serverAddress: {host: mongoHost, port: mongoPort}}});
final mongodb:Collection notifications = check collection("notifications");

function collection(string name) returns mongodb:Collection|error {
    mongodb:Database database = check mongoClient->getDatabase("notification_db");
    return database->getCollection(name);
}

function now() returns string => time:utcToString(time:utcNow());

function shortId(string id) returns string => id.length() > 8 ? id.substring(0, 8) : id;

function money(decimal? d) returns string => d is decimal ? string `N$${d}` : "";

function send(string recipientType, string? recipientId, string orderId, string message) returns error? {
    if recipientId is () {
        return;
    }
    string channel = recipientType == "CUSTOMER" ? "SMS" : recipientType == "RESTAURANT" ? "EMAIL" : "PUSH";
    Notification n = {notificationId: uuid:createType4AsString(), recipientType, recipientId, channel,
        orderId, message, sentAt: now()};
    check notifications->insertOne(n);
    log:printInfo(string `[${channel} -> ${recipientType} ${recipientId}] ${message}`);
}

function handleEvent(string topic, DomainEvent e) returns error? {
    string o = "#" + shortId(e.orderId);
    match topic {
        "orders.created" => {
            check send("CUSTOMER", e.customerId, e.orderId, string `Order ${o} received (${money(e.total)}). Processing payment...`);
        }
        "payments.failed" => {
            check send("CUSTOMER", e.customerId, e.orderId, string `Payment for ${o} failed: ${e.reason ?: "unknown error"}.`);
        }
        "payments.refunded" => {
            check send("CUSTOMER", e.customerId, e.orderId, string `Refund of ${money(e.amount)} issued for ${o}.`);
        }
        "delivery.assigned" => {
            int eta = <int>float:round(e.etaMinutes ?: 0.0);
            check send("DRIVER", e.driverId, e.orderId, string `New delivery ${o}: collect from restaurant ${e.restaurantId ?: ""}.`);
            check send("CUSTOMER", e.customerId, e.orderId, string `Driver ${e.driverId ?: ""} assigned to ${o}. ETA ~${eta} min.`);
        }
        "orders.status-changed" => {
            match e.status {
                "CONFIRMED" => {
                    check send("CUSTOMER", e.customerId, e.orderId, string `Payment received. ${o} sent to the restaurant.`);
                    check send("RESTAURANT", e.restaurantId, e.orderId, string `New order ${o} (${money(e.total)}) - please start preparing.`);
                }
                "PREPARING" => {
                    check send("CUSTOMER", e.customerId, e.orderId, string `The kitchen is preparing ${o}.`);
                }
                "READY" => {
                    check send("CUSTOMER", e.customerId, e.orderId, string `${o} is ready. Finding the nearest driver...`);
                }
                "OUT_FOR_DELIVERY" => {
                    check send("CUSTOMER", e.customerId, e.orderId, string `${o} is on its way!`);
                }
                "DELIVERED" => {
                    check send("CUSTOMER", e.customerId, e.orderId, string `${o} delivered. Enjoy your meal!`);
                    check send("RESTAURANT", e.restaurantId, e.orderId, string `${o} was delivered to the customer.`);
                }
                "CANCELLED" => {
                    check send("CUSTOMER", e.customerId, e.orderId, string `${o} was cancelled${e.reason is string ? ": " + <string>e.reason : ""}.`);
                    check send("RESTAURANT", e.restaurantId, e.orderId, string `${o} was cancelled.`);
                }
            }
        }
    }
}

service /notifications on new http:Listener(httpPort) {
    // GET /notifications?recipientId=c1&recipientType=CUSTOMER
    resource function get .(string? recipientId, string? recipientType) returns Notification[]|error {
        map<json> filter = {};
        if recipientId is string {
            filter["recipientId"] = recipientId;
        }
        if recipientType is string {
            filter["recipientType"] = recipientType;
        }
        stream<Notification, error?> s = check notifications->find(filter, {}, {_id: 0});
        Notification[] all = check from Notification n in s select n;
        return from Notification n in all order by n.sentAt ascending select n;
    }
}

listener kafka:Listener eventListener = new (kafkaUrl, {
    groupId: "notification-service",
    topics: ["orders.created", "orders.status-changed", "payments.failed", "payments.refunded", "delivery.assigned"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false
});

service kafka:Service on eventListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
            string raw = check string:fromBytes(rec.value);
            DomainEvent|error e = raw.fromJsonStringWithType(DomainEvent);
            if e is error {
                log:printError("Malformed event", e, topic = topic);
                continue;
            }
            error? err = handleEvent(topic, e);
            if err is error {
                log:printError("Notification failed", err, topic = topic);
            }
        }
        check caller->commit();
    }
}
