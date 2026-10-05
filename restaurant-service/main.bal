// =====================================================================
// RESTAURANT SERVICE (:8082)
// Digital menus, real-time inventory, opening hours and the kitchen board.
//
// Consumes: orders.status-changed (CONFIRMED -> reserve stock + kitchen ticket,
//                                  CANCELLED -> release stock)
// Produces: kitchen.preparing, kitchen.ready
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
configurable int httpPort = 8082;

// ---------------------------------------------------------------- types
type OpeningHours record {|
    string open;   // "HH:MM" Windhoek time
    string close;  // "HH:MM"; a close earlier than open means "past midnight"
|};

type Restaurant record {|
    string restaurantId;
    string name;
    string cuisine;
    string nodeId;   // location on the delivery map (see delivery-service/routing.bal)
    OpeningHours openingHours;
    boolean manuallyClosed;
|};

type RestaurantView record {|
    *Restaurant;
    boolean isOpen;
|};

type MenuItem record {|
    string menuItemId;
    string restaurantId;
    string name;
    decimal price;
    int stock;
    boolean available;
|};

type NewRestaurant record {|
    string name;
    string cuisine = "General";
    string nodeId = "cbd";
    OpeningHours openingHours = {open: "00:00", close: "23:59"};
|};

type RestaurantUpdate record {|
    OpeningHours openingHours?;
    boolean manuallyClosed?;
|};

type NewMenuItem record {|
    string name;
    decimal price;
    int stock = 50;
|};

type MenuItemUpdate record {|
    decimal price?;
    int stock?;
    boolean available?;
|};

type OrderItem record {
    string menuItemId;
    string name;
    int quantity;
};

type KitchenTicket record {|
    string orderId;
    string restaurantId;
    OrderItem[] items;
    string status;   // CONFIRMED, PREPARING, READY, CANCELLED
    string updatedAt;
|};

type OrderEvent record {
    string eventId;
    string orderId;
    string restaurantId;
    string status;
    OrderItem[] items;
};

type KitchenEvent record {|
    string eventId;
    string eventType;
    string orderId;
    string restaurantId;
    string timestamp;
|};

// ---------------------------------------------------------------- clients
final kafka:Producer producer = check new (kafkaUrl, {clientId: "restaurant-service", acks: kafka:ACKS_ALL});

final mongodb:Client mongoClient = check new ({connection: {serverAddress: {host: mongoHost, port: mongoPort}}});
final mongodb:Collection restaurants = check collection("restaurants");
final mongodb:Collection menuItems = check collection("menuItems");
final mongodb:Collection tickets = check collection("kitchenTickets");

function collection(string name) returns mongodb:Collection|error {
    mongodb:Database database = check mongoClient->getDatabase("restaurant_db");
    return database->getCollection(name);
}

// ---------------------------------------------------------------- helpers
function now() returns string => time:utcToString(time:utcNow());

function minutesOf(string hhmm) returns int {
    int|error h = int:fromString(hhmm.substring(0, 2));
    int|error m = int:fromString(hhmm.substring(3, 5));
    return (h is int ? h : 0) * 60 + (m is int ? m : 0);
}

function isOpenNow(Restaurant r) returns boolean {
    if r.manuallyClosed {
        return false;
    }
    time:Civil c = time:utcToCivil(time:utcAddSeconds(time:utcNow(), 7200)); // Windhoek = UTC+2
    int nowMin = c.hour * 60 + c.minute;
    int open = minutesOf(r.openingHours.open);
    int close = minutesOf(r.openingHours.close);
    return open <= close ? (nowMin >= open && nowMin <= close) : (nowMin >= open || nowMin <= close);
}

function view(Restaurant r) returns RestaurantView => {...r, isOpen: isOpenNow(r)};

function findRestaurant(string restaurantId) returns Restaurant?|error {
    return restaurants->findOne({restaurantId}, {}, {_id: 0});
}

function emit(string topic, string orderId, string restaurantId) returns error? {
    KitchenEvent event = {eventId: uuid:createType4AsString(), eventType: topic, orderId, restaurantId, timestamp: now()};
    check producer->send({topic, key: orderId.toBytes(), value: event.toJsonString().toBytes()});
}

function adjustStock(OrderItem[] items, int sign) returns error? {
    foreach OrderItem item in items {
        _ = check menuItems->updateOne({menuItemId: item.menuItemId}, {inc: {stock: sign * item.quantity}});
    }
}

// Seed demo data the first time the service starts
function init() returns error? {
    int existingCount = check restaurants->countDocuments({});
    if existingCount > 0 {
        return;
    }
    Restaurant[] seed = [
        {restaurantId: "r1", name: "Kapana King", cuisine: "Namibian street food", nodeId: "katutura",
            openingHours: {open: "00:00", close: "23:59"}, manuallyClosed: false},
        {restaurantId: "r2", name: "Eros Grill House", cuisine: "Grill", nodeId: "eros",
            openingHours: {open: "00:00", close: "23:59"}, manuallyClosed: false},
        {restaurantId: "r3", name: "Klein Windhoek Pizzeria", cuisine: "Pizza", nodeId: "klein",
            openingHours: {open: "00:00", close: "23:59"}, manuallyClosed: false},
        {restaurantId: "r4", name: "Maerua Sushi Bar", cuisine: "Sushi", nodeId: "maerua",
            openingHours: {open: "11:00", close: "21:00"}, manuallyClosed: false}
    ];
    [string, string, decimal][] menu = [
        ["r1", "Kapana Platter", 65], ["r1", "Vetkoek & Mince", 35], ["r1", "Oshifima & Beef Stew", 55],
        ["r1", "Cold Drink", 15], ["r2", "Game Burger", 120], ["r2", "Oryx Steak", 185], ["r2", "Chips", 30],
        ["r3", "Margherita", 95], ["r3", "Biltong Pizza", 130], ["r3", "Garlic Bread", 40],
        ["r4", "Salmon Roses (8)", 140], ["r4", "California Roll (8)", 110]
    ];
    foreach Restaurant r in seed {
        check restaurants->insertOne(r);
    }
    int i = 1;
    foreach [string, string, decimal] [rid, name, price] in menu {
        MenuItem item = {menuItemId: string `m${i}`, restaurantId: rid, name, price, stock: 50, available: true};
        check menuItems->insertOne(item);
        i += 1;
    }
    log:printInfo("Seeded demo restaurants and menus");
}

// ---------------------------------------------------------------- REST API
service / on new http:Listener(httpPort) {

    resource function get restaurants() returns RestaurantView[]|error {
        stream<Restaurant, error?> s = check restaurants->find({}, {}, {_id: 0});
        return from Restaurant r in s select view(r);
    }

    resource function post restaurants(NewRestaurant req) returns http:Created|error {
        Restaurant r = {restaurantId: uuid:createType4AsString(), name: req.name, cuisine: req.cuisine,
            nodeId: req.nodeId, openingHours: req.openingHours, manuallyClosed: false};
        check restaurants->insertOne(r);
        return <http:Created>{body: view(r)};
    }

    resource function get restaurants/[string restaurantId]() returns RestaurantView|http:NotFound|error {
        Restaurant? r = check findRestaurant(restaurantId);
        return r is () ? <http:NotFound>{body: {message: "Restaurant not found"}} : view(r);
    }

    // Update opening hours / temporarily close the kitchen
    resource function patch restaurants/[string restaurantId](RestaurantUpdate req)
            returns RestaurantView|http:NotFound|error {
        map<json> changes = {};
        OpeningHours? hours = req?.openingHours;
        if hours is OpeningHours {
            changes["openingHours"] = hours.toJson();
        }
        boolean? closed = req?.manuallyClosed;
        if closed is boolean {
            changes["manuallyClosed"] = closed;
        }
        if changes.length() > 0 {
            _ = check restaurants->updateOne({restaurantId}, {set: changes});
        }
        Restaurant? r = check findRestaurant(restaurantId);
        return r is () ? <http:NotFound>{body: {message: "Restaurant not found"}} : view(r);
    }

    resource function get restaurants/[string restaurantId]/menu() returns MenuItem[]|error {
        stream<MenuItem, error?> s = check menuItems->find({restaurantId}, {}, {_id: 0});
        return from MenuItem m in s select m;
    }

    resource function post restaurants/[string restaurantId]/menu(NewMenuItem req)
            returns http:Created|http:NotFound|error {
        Restaurant? r = check findRestaurant(restaurantId);
        if r is () {
            return <http:NotFound>{body: {message: "Restaurant not found"}};
        }
        MenuItem item = {menuItemId: uuid:createType4AsString(), restaurantId, name: req.name,
            price: req.price, stock: req.stock, available: true};
        check menuItems->insertOne(item);
        return <http:Created>{body: item};
    }

    // Real-time inventory: change price, stock or availability
    resource function patch restaurants/[string restaurantId]/menu/[string menuItemId](MenuItemUpdate req)
            returns MenuItem|http:NotFound|error {
        map<json> changes = {};
        decimal? price = req?.price;
        if price is decimal {
            changes["price"] = price;
        }
        int? stock = req?.stock;
        if stock is int {
            changes["stock"] = stock;
        }
        boolean? available = req?.available;
        if available is boolean {
            changes["available"] = available;
        }
        if changes.length() > 0 {
            _ = check menuItems->updateOne({restaurantId, menuItemId}, {set: changes});
        }
        MenuItem? m = check menuItems->findOne({restaurantId, menuItemId}, {}, {_id: 0});
        return m is () ? <http:NotFound>{body: {message: "Menu item not found"}} : m;
    }

    resource function get kitchen/[string restaurantId]/tickets() returns KitchenTicket[]|error {
        stream<KitchenTicket, error?> s = check tickets->find({restaurantId}, {}, {_id: 0});
        return from KitchenTicket t in s select t;
    }

    resource function patch kitchen/orders/[string orderId]/'start() returns KitchenTicket|http:Conflict|error {
        return advanceTicket(orderId, "CONFIRMED", "PREPARING", "kitchen.preparing");
    }

    resource function patch kitchen/orders/[string orderId]/ready() returns KitchenTicket|http:Conflict|error {
        return advanceTicket(orderId, "PREPARING", "READY", "kitchen.ready");
    }
}

function advanceTicket(string orderId, string expected, string next, string topic)
        returns KitchenTicket|http:Conflict|error {
    mongodb:UpdateResult res = check tickets->updateOne({orderId, status: expected},
        {set: {status: next, updatedAt: now()}});
    if res.matchedCount == 0 {
        return <http:Conflict>{body: {message: string `Order ${orderId} is not in state ${expected}`}};
    }
    KitchenTicket? t = check tickets->findOne({orderId}, {}, {_id: 0});
    if t is () {
        return error("Ticket vanished");
    }
    check emit(topic, orderId, t.restaurantId);
    return t;
}

// ---------------------------------------------------------------- Kafka consumer
listener kafka:Listener orderListener = new (kafkaUrl, {
    groupId: "restaurant-service",
    topics: ["orders.status-changed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false
});

service kafka:Service on orderListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string raw = check string:fromBytes(rec.value);
            OrderEvent|error event = raw.fromJsonStringWithType(OrderEvent);
            if event is error {
                log:printError("Malformed order event", event);
                continue;
            }
            error? e = handleOrderEvent(event);
            if e is error {
                log:printError("Failed to handle order event", e, orderId = event.orderId);
            }
        }
        check caller->commit();
    }
}

function handleOrderEvent(OrderEvent event) returns error? {
    if event.status == "CONFIRMED" {
        // Idempotent: a redelivered event must not reserve stock twice
        int existingCount = check tickets->countDocuments({orderId: event.orderId});
        if existingCount > 0 {
            return;
        }
        check adjustStock(event.items, -1);
        KitchenTicket ticket = {orderId: event.orderId, restaurantId: event.restaurantId, items: event.items,
            status: "CONFIRMED", updatedAt: now()};
        check tickets->insertOne(ticket);
        log:printInfo("New kitchen ticket", orderId = event.orderId, restaurantId = event.restaurantId);
    } else if event.status == "CANCELLED" {
        mongodb:UpdateResult res = check tickets->updateOne({orderId: event.orderId, status: "CONFIRMED"},
            {set: {status: "CANCELLED", updatedAt: now()}});
        if res.matchedCount > 0 {
            check adjustStock(event.items, 1); // release reserved stock
        }
    }
}
