package io.confluent.demo.riverair;

/**
 * One flight's status, delay, and HIGH-risk passenger count. Same columns as Flink's flight_impact
 * (terraform/airline-demo/sql/25-serving-flight-impact.sql), plus the estimate.
 */
public record FlightImpact(
        String flightId,
        String origin,
        String destination,
        Long scheduledTime,
        Long estimatedTime,
        String status,
        Integer delayMinutes,
        int affectedPassengers) {

    static FlightImpact of(String flightId, Flight flight, Integer atRisk) {
        return new FlightImpact(
                flightId,
                flight.origin(),
                flight.destination(),
                flight.scheduledTime(),
                flight.estimatedTime(),
                flight.status(),
                PassengerState.minutesBetween(flight.scheduledTime(), flight.estimatedTime()),
                atRisk == null ? 0 : atRisk);
    }
}
