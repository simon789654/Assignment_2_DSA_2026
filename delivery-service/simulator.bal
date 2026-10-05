// =====================================================================
// DRIVER LOCATION SIMULATION (bonus)
//
// Moves a driver along a Route (from routing.bal), interpolating between road nodes
// once per second. Every position update is:
//   - stored in memory (served by GET /drivers/locations for the map UI)
//   - published to Kafka topic `driver.location` (keyed by driverId)
//
// Requirements in YOUR delivery-service main.bal:
//   - a `configurable string kafkaUrl` (this file reuses it)
//   - start a trip without blocking the request/consumer:
//         _ = start simulateTrip(driverId, deliveryId, route, "TO_RESTAURANT", onArrive);
//   - register drivers on the map:  check placeDriver(driverId, nodeId);
// =====================================================================
import ballerina/lang.runtime;
import ballerina/log;
import ballerina/time;
import ballerinax/kafka;

public type DriverLocation record {|
    string driverId;
    string? deliveryId;
    float lat;
    float lng;
    string phase;      // IDLE | TO_RESTAURANT | TO_CUSTOMER
    float progress;    // 0.0 - 1.0 along the current route
    string updatedAt;
|};

const decimal TICK_SECONDS = 1;
// 1 real second = 20 simulated seconds, so a 15-minute trip takes ~45s in a demo.
const float SIM_SPEEDUP = 20.0;

isolated map<DriverLocation> driverLocations = {};
// driverId -> token of the trip currently allowed to move that driver
isolated map<string> activeTrips = {};

public isolated function stopTrip(string driverId) {
    lock {
        _ = activeTrips.removeIfHasKey(driverId);
    }
}

isolated function claimTrip(string driverId, string token) {
    lock {
        activeTrips[driverId] = token;
    }
}

isolated function isActiveTrip(string driverId, string token) returns boolean {
    lock {
        return activeTrips[driverId] == token;
    }
}

public isolated function getDriverLocation(string driverId) returns DriverLocation? {
    lock {
        return driverLocations[driverId].clone();
    }
}

final kafka:Producer locationProducer = check new (kafkaUrl, {clientId: "delivery-location-sim"});

isolated function simNow() returns string => time:utcToString(time:utcNow());

public isolated function getDriverLocations() returns DriverLocation[] {
    lock {
        return driverLocations.toArray().clone();
    }
}

public function updateDriverLocation(DriverLocation loc) returns error? {
    readonly & DriverLocation frozen = loc.cloneReadOnly();
    lock {
        driverLocations[frozen.driverId] = frozen;
    }
    check locationProducer->send({
        topic: "driver.location",
        key: frozen.driverId.toBytes(),
        value: frozen.toJsonString().toBytes()
    });
}

// Keep the driver where they are but change the phase (IDLE / OFFLINE).
public function setDriverPhase(string driverId, string phase) returns error? {
    DriverLocation? loc = getDriverLocation(driverId);
    if loc is DriverLocation {
        check updateDriverLocation({driverId, deliveryId: (), lat: loc.lat, lng: loc.lng, phase,
            progress: 0.0, updatedAt: simNow()});
    }
}

// Put an idle driver on the map at a road node (e.g. when the driver registers).
public function placeDriver(string driverId, string nodeId) returns error? {
    Node? n = NODE_BY_ID[nodeId];
    if n is () {
        return error(string `Unknown node ${nodeId}`);
    }
    check updateDriverLocation({
        driverId, deliveryId: (), lat: n.lat, lng: n.lng,
        phase: "IDLE", progress: 0.0, updatedAt: simNow()
    });
}

// Drive along `route`; call `onArrive` at the end (e.g. emit delivery.completed).
public function simulateTrip(string driverId, string deliveryId, Route route, string phase,
        (function () returns error?)? onArrive = ()) returns error? {
    // A newer trip (or stopTrip) for the same driver cancels this one.
    string token = deliveryId + ":" + phase;
    claimTrip(driverId, token);
    float[][] path = route.path;
    float totalMinutes = route.etaMinutes > 0.0 ? route.etaMinutes : 1.0;
    float elapsedMinutes = 0.0;

    foreach int i in 1 ..< path.length() {
        Arc? arc = findArc(route.nodeIds[i - 1], route.nodeIds[i]);
        float segMinutes = arc is Arc ? arc.minutes : 1.0;
        int steps = int:max(1, <int>(segMinutes * 60.0 / SIM_SPEEDUP));

        foreach int s in 1 ... steps {
            if !isActiveTrip(driverId, token) {
                return; // cancelled
            }
            float t = <float>s / <float>steps;
            float lat = path[i - 1][0] + (path[i][0] - path[i - 1][0]) * t;
            float lng = path[i - 1][1] + (path[i][1] - path[i - 1][1]) * t;
            float progress = (elapsedMinutes + segMinutes * t) / totalMinutes;
            error? e = updateDriverLocation({
                driverId, deliveryId, lat, lng, phase,
                progress: float:min(progress, 1.0), updatedAt: simNow()
            });
            if e is error {
                log:printWarn("Location publish failed", driverId = driverId, err = e.message());
            }
            runtime:sleep(TICK_SECONDS);
        }
        elapsedMinutes += segMinutes;
    }
    if !isActiveTrip(driverId, token) {
        return;
    }
    stopTrip(driverId);
    log:printInfo("Trip leg finished", driverId = driverId, deliveryId = deliveryId, phase = phase);
    if onArrive is function () returns error? {
        check onArrive();
    }
}
