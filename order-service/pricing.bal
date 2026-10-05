// =====================================================================
// SURGE PRICING (bonus)
//
// deliveryFee = BASE_DELIVERY_FEE x multiplier, where the multiplier grows with:
//   1. demand/supply ratio: orders placed in the last 15 min vs available drivers
//      (driver count arrives via the `drivers.availability` Kafka topic,
//       published by the Delivery Service whenever a driver changes status)
//   2. peak meal times in Windhoek (12:00-14:00, 18:00-21:00, UTC+2)
//   3. no drivers available at all
// The multiplier is capped at MAX_MULTIPLIER so prices stay sane.
//
// Note for the defence: demand and supply are kept in memory per instance.
// With several Order Service replicas you would move this state to Redis or
// compute it in one dedicated pricing consumer.
// =====================================================================
import ballerina/time;

const decimal BASE_DELIVERY_FEE = 25.00; // NAD
const decimal MAX_MULTIPLIER = 2.5;
const decimal DEMAND_WINDOW_SECONDS = 900;
const decimal RATIO_THRESHOLD = 2.0; // surge starts above 2 orders per driver
const decimal RATIO_STEP = 0.25;     // +0.25x per extra order-per-driver
const decimal PEAK_SURCHARGE = 0.2;
const decimal NO_DRIVER_SURCHARGE = 1.0;

public type PriceQuote record {|
    decimal subtotal;
    decimal baseDeliveryFee;
    decimal surgeMultiplier;
    decimal deliveryFee;
    decimal total;
    int recentOrders;
    int availableDrivers;
    string reason;
|};

type DriverAvailability record {
    int available;
};

// -1 = unknown (no availability event received yet) -> no supply-based surge
isolated int availableDrivers = -1;
isolated time:Utc[] recentOrderTimes = [];

isolated function recordOrderPlaced() {
    time:Utc nowUtc = time:utcNow();
    lock {
        recentOrderTimes.push(nowUtc);
    }
}

isolated function setAvailableDrivers(int count) {
    lock {
        availableDrivers = count;
    }
}

isolated function recentDemand() returns int {
    time:Utc cutoff = time:utcAddSeconds(time:utcNow(), -DEMAND_WINDOW_SECONDS);
    lock {
        // drop timestamps that fell out of the sliding window
        time:Utc[] kept = [];
        foreach time:Utc t in recentOrderTimes {
            if time:utcDiffSeconds(t, cutoff) > 0d {
                kept.push(t);
            }
        }
        recentOrderTimes = kept.cloneReadOnly();
        return recentOrderTimes.length();
    }
}

isolated function isPeakHour() returns boolean {
    int hour = (time:utcToCivil(time:utcNow()).hour + 2) % 24; // Windhoek = UTC+2
    return (hour >= 12 && hour < 14) || (hour >= 18 && hour < 21);
}

isolated function quote(decimal subtotal) returns PriceQuote {
    int demand = recentDemand();
    int drivers;
    lock {
        drivers = availableDrivers;
    }

    decimal multiplier = 1.0;
    string[] reasons = [];

    if drivers == 0 {
        multiplier += NO_DRIVER_SURCHARGE;
        reasons.push("no drivers available");
    } else if drivers > 0 {
        decimal ratio = <decimal>demand / <decimal>drivers;
        if ratio > RATIO_THRESHOLD {
            multiplier += (ratio - RATIO_THRESHOLD) * RATIO_STEP;
            reasons.push(string `high demand (${demand} orders / ${drivers} drivers)`);
        }
    }
    if isPeakHour() {
        multiplier += PEAK_SURCHARGE;
        reasons.push("peak meal time");
    }

    multiplier = decimal:round(decimal:min(multiplier, MAX_MULTIPLIER), 2);
    decimal fee = decimal:round(BASE_DELIVERY_FEE * multiplier, 2);
    return {
        subtotal,
        baseDeliveryFee: BASE_DELIVERY_FEE,
        surgeMultiplier: multiplier,
        deliveryFee: fee,
        total: subtotal + fee,
        recentOrders: demand,
        availableDrivers: drivers,
        reason: reasons.length() == 0 ? "normal pricing" : string:'join(", ", ...reasons)
    };
}
