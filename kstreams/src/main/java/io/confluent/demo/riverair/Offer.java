package io.confluent.demo.riverair;

import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;

/** One row of the passenger_recommendations topic, keyed by offer id (for example P-0928-417-081-O1). */
public record Offer(
        String passengerId,
        String recommendedFlightId,
        String hotelName,
        String status,
        BigDecimal hotelCost,
        Long impactedAt,
        Long recommendedAt) {

    static Offer from(GenericRecord value) {
        return new Offer(
                Flight.text(value, "passenger_id"),
                Flight.text(value, "recommended_flight_id"),
                Flight.text(value, "hotel_name"),
                Flight.text(value, "status"),
                Hotel.decimal(value, "hotel_cost"),
                Flight.millis(value, "impacted_at"),
                Flight.millis(value, "recommended_at"));
    }

    boolean isBooked() {
        return "BOOKED".equals(status);
    }
}
