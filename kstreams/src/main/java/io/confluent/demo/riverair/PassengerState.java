package io.confluent.demo.riverair;

/**
 * One passenger's current connection risk. Same columns and rule as Flink's passenger_state
 * (terraform/airline-demo/sql/24-serving-passenger-state.sql).
 */
public record PassengerState(
        String inboundFlightId,
        String inboundSeat,
        String connectingFlightId,
        String connectingSeat,
        String finalDestination,
        Integer connectionMinutes,
        String risk) {

    static final int HIGH_RISK_BELOW_MINUTES = 45;

    static PassengerState of(InboundLeg leg, Flight onward) {
        Itinerary it = leg.itinerary();
        Flight inbound = leg.inbound();
        Integer minutes = onward == null ? null : minutesBetween(inbound.estimatedTime(), onward.estimatedTime());
        String risk;
        if (it.connectingFlightId() == null) {
            risk = "NO_CONNECTION";
        } else if (minutes != null && minutes < HIGH_RISK_BELOW_MINUTES) {
            risk = "HIGH";
        } else {
            risk = "OK"; // as in Flink, a connecting flight we haven't seen yet counts as OK
        }
        return new PassengerState(
                it.inboundFlightId(),
                it.inboundSeat(),
                it.connectingFlightId(),
                it.connectingSeat(),
                onward != null && onward.destination() != null ? onward.destination() : inbound.destination(),
                minutes,
                risk);
    }

    boolean isHigh() {
        return "HIGH".equals(risk);
    }

    /** Flink's TIMESTAMPDIFF(MINUTE, from, to): whole minutes, truncated toward zero. */
    static Integer minutesBetween(Long from, Long to) {
        if (from == null || to == null) return null;
        return (int) ((to - from) / 60_000);
    }
}
