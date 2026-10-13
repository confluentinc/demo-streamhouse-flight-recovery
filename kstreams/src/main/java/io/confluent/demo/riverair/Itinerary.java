package io.confluent.demo.riverair;

import org.apache.avro.generic.GenericRecord;

/** One row of the passenger_itineraries topic. The connecting columns are null when the trip ends at SFO. */
public record Itinerary(String inboundFlightId, String inboundSeat, String connectingFlightId, String connectingSeat) {

    static Itinerary from(GenericRecord value) {
        return new Itinerary(
                Flight.text(value, "inbound_flight_id"),
                Flight.text(value, "inbound_seat"),
                Flight.text(value, "connecting_flight_id"),
                Flight.text(value, "connecting_seat"));
    }
}
