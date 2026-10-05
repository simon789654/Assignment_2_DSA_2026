// =====================================================================
// ROUTE OPTIMISATION (bonus) - A* search over a simplified Windhoek road graph
//
// - Nodes are suburbs / landmarks (approximate coordinates, good enough for a simulation).
// - Each road has a speed limit; edge cost = travel time in minutes, so A* finds the
//   FASTEST route, not just the shortest one.
// - Heuristic = straight-line (haversine) distance at the maximum road speed.
//   It never overestimates the true travel time, so A* is guaranteed optimal.
// - bestDriver() runs A* from every available driver to the restaurant and picks the
//   one with the lowest ETA (optimised dispatch).
//
// Drop this file into delivery-service/ next to your main.bal.
// =====================================================================

public type Node record {|
    string id;
    string name;
    float lat;
    float lng;
|};

type Road record {|
    string a;
    string b;
    int speedKmh;
|};

type Arc record {|
    string to;
    float km;
    float minutes;
|};

public type Route record {|
    string[] nodeIds;
    float[][] path;      // [[lat, lng], ...] - draw directly as a map polyline
    float distanceKm;
    float etaMinutes;
|};

public type DriverPosition record {
    string driverId;
    float lat;
    float lng;
};

const float EARTH_RADIUS_KM = 6371.0;
const float ROAD_FACTOR = 1.3;      // real roads are ~30% longer than a straight line
const float MAX_SPEED_KMH = 80.0;   // used by the heuristic (must be >= every road speed)

final readonly & Node[] NODES = [
    {id: "cbd", name: "Windhoek CBD", lat: -22.5700, lng: 17.0836},
    {id: "nust", name: "NUST Campus", lat: -22.5650, lng: 17.0780},
    {id: "klein", name: "Klein Windhoek", lat: -22.5730, lng: 17.1000},
    {id: "ludwigsdorf", name: "Ludwigsdorf", lat: -22.5640, lng: 17.1080},
    {id: "eros", name: "Eros", lat: -22.5520, lng: 17.0960},
    {id: "northind", name: "Northern Industrial", lat: -22.5400, lng: 17.0750},
    {id: "katutura", name: "Katutura", lat: -22.5180, lng: 17.0570},
    {id: "wanaheda", name: "Wanaheda", lat: -22.5050, lng: 17.0480},
    {id: "khomasdal", name: "Khomasdal", lat: -22.5420, lng: 17.0490},
    {id: "dorado", name: "Dorado Park", lat: -22.5350, lng: 17.0330},
    {id: "hochland", name: "Hochland Park", lat: -22.5650, lng: 17.0560},
    {id: "pionierspark", name: "Pionierspark", lat: -22.5980, lng: 17.0640},
    {id: "southind", name: "Southern Industrial", lat: -22.5900, lng: 17.0820},
    {id: "maerua", name: "Maerua Mall", lat: -22.5830, lng: 17.0880},
    {id: "olympia", name: "Olympia", lat: -22.5950, lng: 17.0950},
    {id: "academia", name: "Academia", lat: -22.6080, lng: 17.0780},
    {id: "kleinekuppe", name: "Kleine Kuppe", lat: -22.6230, lng: 17.0920}
];

final readonly & Road[] ROADS = [
    {a: "cbd", b: "nust", speedKmh: 50},
    {a: "cbd", b: "klein", speedKmh: 60},
    {a: "cbd", b: "maerua", speedKmh: 60},
    {a: "cbd", b: "northind", speedKmh: 60},
    {a: "cbd", b: "hochland", speedKmh: 60},
    {a: "nust", b: "khomasdal", speedKmh: 60},
    {a: "nust", b: "northind", speedKmh: 50},
    {a: "klein", b: "ludwigsdorf", speedKmh: 50},
    {a: "klein", b: "eros", speedKmh: 60},
    {a: "klein", b: "maerua", speedKmh: 60},
    {a: "klein", b: "olympia", speedKmh: 60},
    {a: "eros", b: "northind", speedKmh: 60},
    {a: "eros", b: "ludwigsdorf", speedKmh: 50},
    {a: "northind", b: "katutura", speedKmh: 60},
    {a: "katutura", b: "wanaheda", speedKmh: 50},
    {a: "katutura", b: "khomasdal", speedKmh: 60},
    {a: "khomasdal", b: "dorado", speedKmh: 60},
    {a: "khomasdal", b: "hochland", speedKmh: 60},
    {a: "wanaheda", b: "dorado", speedKmh: 50},
    {a: "hochland", b: "pionierspark", speedKmh: 60},
    {a: "pionierspark", b: "southind", speedKmh: 60},
    {a: "southind", b: "maerua", speedKmh: 60},
    {a: "southind", b: "academia", speedKmh: 60},
    {a: "maerua", b: "olympia", speedKmh: 60},
    {a: "academia", b: "olympia", speedKmh: 50},
    {a: "academia", b: "kleinekuppe", speedKmh: 70},
    {a: "olympia", b: "kleinekuppe", speedKmh: 60}
];

final readonly & map<Node> NODE_BY_ID = buildNodeIndex();
final readonly & map<Arc[]> GRAPH = buildGraph();

function buildNodeIndex() returns readonly & map<Node> {
    map<Node> index = {};
    foreach Node n in NODES {
        index[n.id] = n;
    }
    return index.cloneReadOnly();
}

function buildGraph() returns readonly & map<Arc[]> {
    map<Arc[]> graph = {};
    foreach Node n in NODES {
        graph[n.id] = [];
    }
    foreach Road r in ROADS {
        float km = distanceKm(NODE_BY_ID.get(r.a), NODE_BY_ID.get(r.b)) * ROAD_FACTOR;
        float minutes = km / <float>r.speedKmh * 60.0;
        graph.get(r.a).push({to: r.b, km, minutes});
        graph.get(r.b).push({to: r.a, km, minutes}); // roads are two-way
    }
    return graph.cloneReadOnly();
}

// ---------------------------------------------------------------- geometry
isolated function toRad(float degrees) returns float => degrees * float:PI / 180.0;

public isolated function haversineKm(float lat1, float lng1, float lat2, float lng2) returns float {
    float dLat = toRad(lat2 - lat1);
    float dLng = toRad(lng2 - lng1);
    float h = float:pow(float:sin(dLat / 2.0), 2.0)
        + float:cos(toRad(lat1)) * float:cos(toRad(lat2)) * float:pow(float:sin(dLng / 2.0), 2.0);
    return 2.0 * EARTH_RADIUS_KM * float:asin(float:sqrt(h));
}

isolated function distanceKm(Node a, Node b) returns float => haversineKm(a.lat, a.lng, b.lat, b.lng);

isolated function heuristicMinutes(Node origin, Node goal) returns float =>
    distanceKm(origin, goal) / MAX_SPEED_KMH * 60.0;

isolated function round2(float f) returns float => float:round(f * 100.0) / 100.0;

// ---------------------------------------------------------------- public API
public isolated function allNodes() returns Node[] => NODES;

public isolated function nearestNode(float lat, float lng) returns string {
    string best = NODES[0].id;
    float bestKm = float:Infinity;
    foreach Node n in NODES {
        float km = haversineKm(lat, lng, n.lat, n.lng);
        if km < bestKm {
            bestKm = km;
            best = n.id;
        }
    }
    return best;
}

isolated function findArc(string fromId, string toId) returns Arc? {
    foreach Arc arc in GRAPH.get(fromId) {
        if arc.to == toId {
            return arc;
        }
    }
    return ();
}

// A* search: returns the fastest route between two nodes.
public isolated function shortestRoute(string fromId, string toId) returns Route|error {
    if !NODE_BY_ID.hasKey(fromId) || !NODE_BY_ID.hasKey(toId) {
        return error(string `Unknown node: ${fromId} or ${toId}`);
    }
    Node goal = NODE_BY_ID.get(toId);
    map<float> gScore = {[fromId]: 0.0};   // best known time from start
    map<string> cameFrom = {};
    map<boolean> closed = {};
    string[] open = [fromId];

    while open.length() > 0 {
        // Pick the open node with the lowest f = g + h.
        // A linear scan is fine for a city-sized graph; use a binary heap for thousands of nodes.
        int bestIdx = 0;
        float bestF = float:Infinity;
        foreach int i in 0 ..< open.length() {
            float f = gScore.get(open[i]) + heuristicMinutes(NODE_BY_ID.get(open[i]), goal);
            if f < bestF {
                bestF = f;
                bestIdx = i;
            }
        }
        string current = open.remove(bestIdx);
        if current == toId {
            return buildRoute(cameFrom, toId);
        }
        closed[current] = true;

        foreach Arc arc in GRAPH.get(current) {
            if closed.hasKey(arc.to) {
                continue;
            }
            float tentative = gScore.get(current) + arc.minutes;
            if !gScore.hasKey(arc.to) || tentative < gScore.get(arc.to) {
                gScore[arc.to] = tentative;
                cameFrom[arc.to] = current;
                if open.indexOf(arc.to) is () {
                    open.push(arc.to);
                }
            }
        }
    }
    return error(string `No route from ${fromId} to ${toId}`);
}

isolated function buildRoute(map<string> cameFrom, string goalId) returns Route {
    string[] ids = [goalId];
    string cursor = goalId;
    while cameFrom.hasKey(cursor) {
        cursor = cameFrom.get(cursor);
        ids.unshift(cursor);
    }
    float km = 0.0;
    float minutes = 0.0;
    foreach int i in 1 ..< ids.length() {
        Arc? arc = findArc(ids[i - 1], ids[i]);
        if arc is Arc {
            km += arc.km;
            minutes += arc.minutes;
        }
    }
    float[][] path = from string id in ids
        let Node n = NODE_BY_ID.get(id)
        select [n.lat, n.lng];
    return {nodeIds: ids, path, distanceKm: round2(km), etaMinutes: round2(minutes)};
}

// Optimised dispatch: the available driver who can reach the restaurant fastest.
public isolated function bestDriver(DriverPosition[] available, string restaurantNodeId)
        returns [string, Route]|error {
    [string, Route]? best = ();
    foreach DriverPosition d in available {
        Route r = check shortestRoute(nearestNode(d.lat, d.lng), restaurantNodeId);
        if best is () || r.etaMinutes < best[1].etaMinutes {
            best = [d.driverId, r];
        }
    }
    if best is () {
        return error("No available drivers");
    }
    return best;
}
