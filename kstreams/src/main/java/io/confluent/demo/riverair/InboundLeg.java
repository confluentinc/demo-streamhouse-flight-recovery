package io.confluent.demo.riverair;

/** An itinerary joined to its inbound flight, before the connecting flight is joined. */
public record InboundLeg(Itinerary itinerary, Flight inbound) {
}
