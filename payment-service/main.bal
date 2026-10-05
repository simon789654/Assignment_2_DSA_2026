// =====================================================================
// PAYMENT SERVICE (:8084)
// Simulates card payments for new orders and refunds for cancelled paid orders.
//
// Consumes: orders.created          -> charge -> payments.completed | payments.failed
//           orders.status-changed   -> CANCELLED after payment -> payments.refunded
// Rules of the simulation (easy to demonstrate in the defence):
//   - orders above CARD_LIMIT are declined ("insufficient funds")
//   - customer id "c-declined" is always declined
//   - optional random failureRate (default 0) simulates bank errors
// =====================================================================
import ballerina/http;
import ballerina/lang.runtime;
import ballerina/log;
import ballerina/random;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerinax/prometheus as _;

configurable string kafkaUrl = "localhost:29092";
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable int httpPort = 8084;
configurable float failureRate = 0.0; // e.g. BAL_CONFIG_VAR_FAILURERATE=0.1 for random bank errors

const decimal CARD_LIMIT = 2000;

type Payment record {|
    string paymentId;
    string orderId;
    string customerId;
    decimal amount;
    string method;
    string status;      // COMPLETED, FAILED, REFUNDED
    string? reason;
    string processedAt;
|};

type OrderEvent record {
    string eventId;
    string orderId;
    string customerId;
    string restaurantId;
    string status;
    decimal total;
};

type PaymentEvent record {|
    string eventId;
    string eventType;
    string paymentId;
    string orderId;
    string customerId;
    string restaurantId;
    decimal amount;
    string? reason;
    string timestamp;
|};

final kafka:Producer producer = check new (kafkaUrl, {clientId: "payment-service", acks: kafka:ACKS_ALL});

final mongodb:Client mongoClient = check new ({connection: {serverAddress: {host: mongoHost, port: mongoPort}}});
final mongodb:Collection payments = check collection("payments");

function collection(string name) returns mongodb:Collection|error {
    mongodb:Database database = check mongoClient->getDatabase("payment_db");
    return database->getCollection(name);
}

function now() returns string => time:utcToString(time:utcNow());

function emit(string topic, Payment p, string restaurantId) returns error? {
    PaymentEvent event = {eventId: uuid:createType4AsString(), eventType: topic, paymentId: p.paymentId,
        orderId: p.orderId, customerId: p.customerId, restaurantId, amount: p.amount, reason: p.reason,
        timestamp: now()};
    check producer->send({topic, key: p.orderId.toBytes(), value: event.toJsonString().toBytes()});
}

// ---------------------------------------------------------------- payment logic
function charge(OrderEvent ord) returns error? {
    // Idempotency: never charge the same order twice (Kafka delivers at-least-once)
    int existingCount = check payments->countDocuments({orderId: ord.orderId});
    if existingCount > 0 {
        log:printInfo("Duplicate orders.created ignored", orderId = ord.orderId);
        return;
    }
    runtime:sleep(<decimal>(0.5 + random:createDecimal() * 1.5)); // simulate the bank round-trip

    string? reason = ();
    if ord.total > CARD_LIMIT {
        reason = string `Insufficient funds (card limit N$${CARD_LIMIT})`;
    } else if ord.customerId == "c-declined" {
        reason = "Card declined by issuer";
    } else if random:createDecimal() < failureRate {
        reason = "Bank timeout";
    }

    Payment p = {paymentId: uuid:createType4AsString(), orderId: ord.orderId, customerId: ord.customerId,
        amount: ord.total, method: "CARD", status: reason is () ? "COMPLETED" : "FAILED", reason,
        processedAt: now()};
    check payments->insertOne(p);
    check emit(reason is () ? "payments.completed" : "payments.failed", p, ord.restaurantId);
    log:printInfo("Payment processed", orderId = p.orderId, status = p.status, amount = p.amount);
}

function refundIfPaid(OrderEvent ord) returns error? {
    mongodb:UpdateResult res = check payments->updateOne({orderId: ord.orderId, status: "COMPLETED"},
        {set: {status: "REFUNDED", processedAt: now()}});
    if res.matchedCount == 0 {
        return; // never paid (or already refunded)
    }
    Payment? p = check payments->findOne({orderId: ord.orderId}, {}, {_id: 0});
    if p is Payment {
        check emit("payments.refunded", p, ord.restaurantId);
        log:printInfo("Payment refunded", orderId = p.orderId);
    }
}

// ---------------------------------------------------------------- REST API
service /payments on new http:Listener(httpPort) {
    resource function get .(string? status) returns Payment[]|error {
        map<json> filter = status is string ? {status} : {};
        stream<Payment, error?> s = check payments->find(filter, {}, {_id: 0});
        return from Payment p in s select p;
    }

    resource function get [string orderId]() returns Payment|http:NotFound|error {
        Payment? p = check payments->findOne({orderId}, {}, {_id: 0});
        return p is () ? <http:NotFound>{body: {message: "No payment for this order"}} : p;
    }
}

// ---------------------------------------------------------------- Kafka consumer
listener kafka:Listener orderListener = new (kafkaUrl, {
    groupId: "payment-service",
    topics: ["orders.created", "orders.status-changed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false
});

service kafka:Service on orderListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
            string raw = check string:fromBytes(rec.value);
            OrderEvent|error event = raw.fromJsonStringWithType(OrderEvent);
            if event is error {
                log:printError("Malformed order event", event, topic = topic);
                continue;
            }
            error? e = ();
            if topic == "orders.created" {
                e = charge(event);
            } else if event.status == "CANCELLED" {
                e = refundIfPaid(event);
            }
            if e is error {
                log:printError("Payment handling failed", e, orderId = event.orderId);
            }
        }
        check caller->commit();
    }
}
