// =====================================================================
// CUSTOMER SERVICE (:8081)
// User accounts, delivery addresses and historical order data.
//
// Order history is a local read model built from Kafka events
// (orders.created, orders.status-changed), so this service never queries
// the Order Service's database: each service owns its own data.
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
configurable int httpPort = 8081;

type Address record {|
    string label;      // e.g. "Home", "Work"
    string street;
    string nodeId;     // delivery map location (delivery-service/routing.bal)
|};

type Customer record {|
    string customerId;
    string name;
    string email;
    string phone;
    Address[] addresses;
    string createdAt;
|};

type NewCustomer record {|
    string customerId?;
    string name;
    string email;
    string phone;
    Address[] addresses = [];
|};

type OrderHistoryEntry record {|
    string customerId;
    string orderId;
    string restaurantId;
    string status;
    decimal total;
    string updatedAt;
|};

type OrderEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    string status;
    decimal total;
    string timestamp;
};

final mongodb:Client mongoClient = check new ({connection: {serverAddress: {host: mongoHost, port: mongoPort}}});
final mongodb:Collection customers = check collection("customers");
final mongodb:Collection history = check collection("orderHistory");

function collection(string name) returns mongodb:Collection|error {
    mongodb:Database database = check mongoClient->getDatabase("customer_db");
    return database->getCollection(name);
}

function now() returns string => time:utcToString(time:utcNow());

function findCustomer(string customerId) returns Customer?|error {
    return customers->findOne({customerId}, {}, {_id: 0});
}

function init() returns error? {
    int existingCount = check customers->countDocuments({});
    if existingCount > 0 {
        return;
    }
    Customer[] seed = [
        {customerId: "c1", name: "Demo Customer", email: "c1@example.na", phone: "+264811111111",
            addresses: [{label: "Home", street: "13 Jackson Kaujeua St", nodeId: "cbd"}], createdAt: now()},
        {customerId: "c2", name: "Second Customer", email: "c2@example.na", phone: "+264812222222",
            addresses: [{label: "Home", street: "Wanaheda Ext 2", nodeId: "wanaheda"}], createdAt: now()},
        {customerId: "c-declined", name: "Declined Card (test)", email: "declined@example.na",
            phone: "+264813333333", addresses: [{label: "Home", street: "Olympia", nodeId: "olympia"}],
            createdAt: now()}
    ];
    foreach Customer c in seed {
        check customers->insertOne(c);
    }
    log:printInfo("Seeded demo customers");
}

service /customers on new http:Listener(httpPort) {

    resource function get .() returns Customer[]|error {
        stream<Customer, error?> s = check customers->find({}, {}, {_id: 0});
        return from Customer c in s select c;
    }

    resource function post .(NewCustomer req) returns http:Created|http:Conflict|error {
        string id = req?.customerId ?: uuid:createType4AsString();
        if check findCustomer(id) is Customer {
            return <http:Conflict>{body: {message: "Customer already exists"}};
        }
        Customer c = {customerId: id, name: req.name, email: req.email, phone: req.phone,
            addresses: req.addresses, createdAt: now()};
        check customers->insertOne(c);
        return <http:Created>{body: c};
    }

    resource function get [string customerId]() returns Customer|http:NotFound|error {
        Customer? c = check findCustomer(customerId);
        return c is () ? <http:NotFound>{body: {message: "Customer not found"}} : c;
    }

    resource function post [string customerId]/addresses(Address address) returns Customer|http:NotFound|error {
        mongodb:UpdateResult res = check customers->updateOne({customerId}, {"push": {addresses: address.toJson()}});
        if res.matchedCount == 0 {
            return <http:NotFound>{body: {message: "Customer not found"}};
        }
        Customer? c = check findCustomer(customerId);
        return c is () ? <http:NotFound>{body: {message: "Customer not found"}} : c;
    }

    resource function get [string customerId]/orders() returns OrderHistoryEntry[]|error {
        stream<OrderHistoryEntry, error?> s = check history->find({customerId}, {}, {_id: 0});
        OrderHistoryEntry[] entries = check from OrderHistoryEntry e in s select e;
        return from OrderHistoryEntry e in entries order by e.updatedAt descending select e;
    }
}

// ---------------------------------------------------------------- Kafka consumer
listener kafka:Listener orderListener = new (kafkaUrl, {
    groupId: "customer-service",
    topics: ["orders.created", "orders.status-changed"],
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
            // Upsert keyed by orderId; only apply newer events (out-of-order safe)
            OrderHistoryEntry entry = {customerId: e.customerId, orderId: e.orderId, restaurantId: e.restaurantId,
                status: e.status, total: e.total, updatedAt: e.timestamp};
            OrderHistoryEntry|error? existing = history->findOne({orderId: e.orderId}, {}, {_id: 0});
            if existing is OrderHistoryEntry && existing.updatedAt > e.timestamp {
                continue;
            }
            mongodb:UpdateResult|error res = history->updateOne({orderId: e.orderId}, {set: entry.toJson()},
                {upsert: true});
            if res is error {
                log:printError("History update failed", res, orderId = e.orderId);
            }
        }
        check caller->commit();
    }
}
