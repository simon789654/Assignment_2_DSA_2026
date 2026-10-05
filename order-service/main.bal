// =====================================================================
// ORDER SERVICE (reference implementation / pattern for the other services)
//
// Owns the central order state machine:
//   CREATED -> CONFIRMED -> PREPARING -> READY -> OUT_FOR_DELIVERY -> DELIVERED
//   (CANCELLED allowed from CREATED or CONFIRMED)
//
// Patterns shown here that every other service reuses:
//   1. configurable values overridden by Docker env vars (BAL_CONFIG_VAR_*)
//   2. Kafka producer keyed by orderId (per-order ordering across partitions)
//   3. Kafka listener with manual commit + per-service consumer group
//   4. MongoDB collection access (database-per-service)
//   5. Idempotent event handling via state-machine validation
// =====================================================================
import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;
import ballerinax/prometheus as _; // exposes /metrics on :9797 for Prometheus (bonus)

configurable string kafkaUrl = "localhost:29092";
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable int httpPort = 8083;
configurable string restaurantUrl = "http://localhost:8082";

// ---------------------------------------------------------------- types
public enum OrderStatus {
    CREATED, CONFIRMED, PREPARING, READY, OUT_FOR_DELIVERY, DELIVERED, CANCELLED
}

public type OrderItem record {|
    string menuItemId;
    string name;
    int quantity;
    decimal unitPrice;
|};

public type StatusChange record {|
    OrderStatus status;
    string at;
    string reason?;
|};

public type Order record {|
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal subtotal;
    decimal deliveryFee;
    decimal surgeMultiplier;
    decimal total;
    string deliveryAddress;
    string? deliveryNodeId; // map node used by Delivery Service for routing
    OrderStatus status;
    StatusChange[] history;
    string createdAt;
|};

type OrderLine record {
    string menuItemId;
    int quantity;
};

type NewOrder record {|
    string customerId;
    string restaurantId;
    OrderLine[] items;
    string deliveryAddress;
    string? deliveryNodeId = ();
|};

type CancelRequest record {|
    string reason;
|};

// Event this service publishes (the "contract" other services consume).
public type OrderEvent record {|
    string eventId;
    string eventType;
    string orderId;
    string customerId;
    string restaurantId;
    OrderStatus status;
    OrderItem[] items;
    decimal total;
    string deliveryAddress;
    string? deliveryNodeId;
    string? reason;
    string timestamp;
|};

// Events consumed from other services. Open record: extra fields are ignored,
// so producers can add fields without breaking this consumer.
type InboundEvent record {
    string eventId;
    string orderId;
    string? reason = (); // producers send null when there is no reason
};

// ---------------------------------------------------------------- state machine
final readonly & map<OrderStatus[]> TRANSITIONS = {
    CREATED: [CONFIRMED, CANCELLED],
    CONFIRMED: [PREPARING, CANCELLED],
    PREPARING: [READY],
    READY: [OUT_FOR_DELIVERY],
    OUT_FOR_DELIVERY: [DELIVERED],
    DELIVERED: [],
    CANCELLED: []
};

// Which incoming topic moves the order into which state.
final readonly & map<OrderStatus> TOPIC_TO_STATUS = {
    "payments.completed": CONFIRMED,
    "payments.failed": CANCELLED,
    "kitchen.preparing": PREPARING,
    "kitchen.ready": READY,
    "delivery.picked-up": OUT_FOR_DELIVERY,
    "delivery.completed": DELIVERED
};

// ---------------------------------------------------------------- clients
final kafka:Producer producer = check new (kafkaUrl, {
    clientId: "order-service",
    acks: kafka:ACKS_ALL,
    retryCount: 3,
    enableIdempotence: true
});

// Synchronous REST call to the Restaurant Service to validate menu, stock and opening hours
final http:Client restaurantClient = check new (restaurantUrl, {timeout: 5});

type RestaurantInfo record {
    string restaurantId;
    boolean isOpen;
};

type MenuItem record {
    string menuItemId;
    string name;
    decimal price;
    int stock;
    boolean available;
};

final mongodb:Client mongoClient = check new ({
    connection: {serverAddress: {host: mongoHost, port: mongoPort}}
});

final mongodb:Collection orders = check initCollection();

function initCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase("order_db");
    return db->getCollection("orders");
}

// ---------------------------------------------------------------- helpers
function now() returns string => time:utcToString(time:utcNow());

function findOrder(string orderId) returns Order?|error {
    // projection {_id: 0} drops Mongo's internal id so the doc fits the Order record
    return orders->findOne({orderId}, {}, {_id: 0});
}

function publish(string topic, Order o) returns error? {
    OrderEvent event = {
        eventId: uuid:createType4AsString(),
        eventType: topic,
        orderId: o.orderId,
        customerId: o.customerId,
        restaurantId: o.restaurantId,
        status: o.status,
        items: o.items,
        total: o.total,
        deliveryAddress: o.deliveryAddress,
        deliveryNodeId: o.deliveryNodeId,
        reason: o.history.length() > 0 ? o.history[o.history.length() - 1]?.reason : (),
        timestamp: now()
    };
    // Key = orderId: all events for one order land on the same partition,
    // so consumers always see them in order.
    check producer->send({topic, key: o.orderId.toBytes(), value: event.toJsonString().toBytes()});
}

// Validates and applies a transition, then announces it on orders.status-changed.
function transition(string orderId, OrderStatus next, string? reason = ()) returns Order|error {
    Order? current = check findOrder(orderId);
    if current is () {
        return error(string `Order ${orderId} not found`);
    }
    OrderStatus[] allowed = TRANSITIONS[current.status] ?: [];
    if allowed.indexOf(next) is () {
        return error(string `Illegal transition ${current.status} -> ${next}`);
    }
    StatusChange change = {status: next, at: now()};
    if reason is string {
        change.reason = reason;
    }
    // Filtering on the old status = optimistic concurrency control:
    // if another instance changed the order first, nothing matches.
    mongodb:UpdateResult res = check orders->updateOne(
        {orderId, status: current.status},
        {set: {status: next}, "push": {history: change.toJson()}}
    );
    if res.matchedCount == 0 {
        return error(string `Order ${orderId} was modified concurrently`);
    }
    current.status = next;
    current.history.push(change);
    check publish("orders.status-changed", current);
    return current;
}

function parse(byte[] value) returns InboundEvent|error {
    string s = check string:fromBytes(value);
    return s.fromJsonStringWithType(InboundEvent);
}

// ---------------------------------------------------------------- REST API
service /orders on new http:Listener(httpPort) {

    // Place an order -> CREATED, emits orders.created (Payment Service picks it up)
    resource function post .(NewOrder req) returns http:Created|http:BadRequest|error {
        if req.items.length() == 0 {
            return <http:BadRequest>{body: {message: "Order must contain at least one item"}};
        }
        // Validate against the Restaurant Service: never trust prices sent by the client
        RestaurantInfo|error restaurant = restaurantClient->get(string `/restaurants/${req.restaurantId}`);
        if restaurant is error {
            return <http:BadRequest>{body: {message: "Unknown restaurant (or restaurant service unavailable)"}};
        }
        if !restaurant.isOpen {
            return <http:BadRequest>{body: {message: "Restaurant is currently closed"}};
        }
        MenuItem[] menu = check restaurantClient->get(string `/restaurants/${req.restaurantId}/menu`);
        map<MenuItem> menuById = map from MenuItem m in menu select [m.menuItemId, m];
        OrderItem[] pricedItems = [];
        decimal subtotal = 0;
        foreach OrderLine item in req.items {
            MenuItem? m = menuById[item.menuItemId];
            if m is () || !m.available || item.quantity <= 0 || m.stock < item.quantity {
                return <http:BadRequest>{body: {message: string `Item ${item.menuItemId} is unavailable or out of stock`}};
            }
            pricedItems.push({menuItemId: m.menuItemId, name: m.name, quantity: item.quantity, unitPrice: m.price});
            subtotal += m.price * <decimal>item.quantity;
        }
        PriceQuote price = quote(subtotal); // surge pricing (pricing.bal)
        recordOrderPlaced();
        string ts = now();
        Order o = {
            orderId: uuid:createType4AsString(),
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            items: pricedItems,
            subtotal,
            deliveryFee: price.deliveryFee,
            surgeMultiplier: price.surgeMultiplier,
            total: price.total,
            deliveryAddress: req.deliveryAddress,
            deliveryNodeId: req.deliveryNodeId,
            status: CREATED,
            history: [{status: CREATED, at: ts}],
            createdAt: ts
        };
        check orders->insertOne(o);
        check publish("orders.created", o);
        return <http:Created>{body: o};
    }

    // GET /orders/quote?subtotal=120.00 -> live surge-priced quote shown before checkout
    resource function get quote(decimal subtotal) returns PriceQuote {
        return quote(subtotal);
    }

    resource function get [string orderId]() returns Order|http:NotFound|error {
        Order? o = check findOrder(orderId);
        return o ?: <http:NotFound>{body: {message: "Order not found"}};
    }

    // GET /orders?customerId=...&restaurantId=...&status=...
    resource function get .(string? customerId, string? restaurantId, string? status) returns Order[]|error {
        map<json> filter = {};
        if customerId is string {
            filter["customerId"] = customerId;
        }
        if restaurantId is string {
            filter["restaurantId"] = restaurantId;
        }
        if status is string {
            filter["status"] = status;
        }
        stream<Order, error?> result = check orders->find(filter, {}, {_id: 0});
        return from Order o in result select o;
    }

    resource function post [string orderId]/cancel(CancelRequest req) returns Order|http:Conflict|error {
        Order|error result = transition(orderId, CANCELLED, req.reason);
        if result is error {
            return <http:Conflict>{body: {message: result.message()}};
        }
        return result;
    }
}

// ---------------------------------------------------------------- Kafka consumer
listener kafka:Listener eventListener = new (kafkaUrl, {
    groupId: "order-service",
    topics: TOPIC_TO_STATUS.keys(),
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false
});

service kafka:Service on eventListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
            OrderStatus? next = TOPIC_TO_STATUS[topic];
            if next is () {
                continue;
            }
            InboundEvent|error event = parse(rec.value);
            if event is error {
                // TODO (group): forward to events.dlq instead of only logging
                log:printError("Skipping malformed event", event, topic = topic);
                continue;
            }
            Order|error result = transition(event.orderId, next, event.reason);
            if result is error {
                // Duplicates / out-of-order redeliveries fail validation here,
                // which is what makes this consumer idempotent.
                log:printWarn("Transition rejected", orderId = event.orderId, reason = result.message());
            }
        }
        // Commit only after the whole batch is handled (at-least-once delivery).
        check caller->commit();
    }
}

// ---------------------------------------------------------------- driver supply for surge pricing
// Separate consumer group so this instance sees every availability update.
listener kafka:Listener availabilityListener = new (kafkaUrl, {
    groupId: "order-service-pricing",
    topics: ["drivers.availability"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service kafka:Service on availabilityListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string raw = check string:fromBytes(rec.value);
            DriverAvailability|error update = raw.fromJsonStringWithType(DriverAvailability);
            if update is DriverAvailability {
                setAvailableDrivers(update.available);
            }
        }
    }
}
