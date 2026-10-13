package io.confluent.demo.riverair;

/** Just what the at-risk count needs from a passenger: their inbound flight and whether they're HIGH risk. */
public record FlightRisk(String inboundFlightId, boolean high) {

    static FlightRisk of(PassengerState p) {
        return p == null ? null : new FlightRisk(p.inboundFlightId(), p.isHigh());
    }
}
