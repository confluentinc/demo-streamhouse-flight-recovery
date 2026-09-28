"use strict";

const $ = (id) => document.getElementById(id);
let current = { flights: [], delays: [], counts: {} };
let openFlight = null;
let selected = null;
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));

// Lightning returns timestamps like "2026-09-25 01:00:00.000000" (UTC); show HH:MM.
function hhmm(value) {
  if (value === null || value === undefined || value === "") return "";
  if (/^\d+$/.test(String(value))) return new Date(Number(value)).toISOString().slice(11, 16);
  const match = String(value).match(/[ T](\d{2}:\d{2})/);
  return match ? match[1] : String(value);
}

const riskClass = (risk) => (risk === "HIGH" ? "MISS" : "other");
const statusClass = (status) => (status === "DELAYED" ? "TIGHT" : "other");

async function request(path, method = "GET") {
  const response = await fetch(path, { method });
  const body = await response.json();
  if (!response.ok) throw new Error(body.detail || response.statusText);
  return body;
}

async function refresh() {
  try {
    current = await request("/api/state");
    render();
    $("status-line").textContent = "Live from Lightning Tables";
  } catch (error) {
    $("status-line").textContent = error.message;
  }
}

function flightButton(id) {
  return `<button class="link" data-flight="${esc(id)}">${esc(id)}</button>`;
}

function bindFlights() {
  document.querySelectorAll("[data-flight]").forEach((button) =>
    button.onclick = () => { openFlight = button.dataset.flight; render(); refreshFlight(); });
}

function render() {
  const counts = current.counts || {};
  $("counts").innerHTML = `<div class="count"><b>${esc(counts.flights ?? 0)}</b><span>Flights today</span></div>
    <div class="count tight"><b>${esc(counts.delayed ?? 0)}</b><span>Delayed</span></div>
    <div class="count miss"><b>${esc(counts.affected ?? 0)}</b><span>Passengers at risk</span></div>
    <div class="count ok"><b>${esc(counts.booked ?? 0)}</b><span>Booked</span></div>`;
  $("flights").innerHTML = current.flights.map((row) => `<tr class="${row.key === openFlight ? "selected" : ""}">
    <td>${flightButton(row.key)}</td><td>${esc(row.origin)} → ${esc(row.destination)}</td>
    <td>${esc(hhmm(row.scheduled_time))}</td>
    <td><span class="risk ${statusClass(row.status)}">${esc(row.status)}</span></td>
    <td>${esc(row.delay_minutes)}</td><td>${esc(row.affected_passengers)}</td></tr>`).join("");
  $("delays").innerHTML = current.delays.map((row) => `<tr class="${row.key === openFlight ? "selected" : ""}">
    <td>${flightButton(row.key)}</td><td>${esc(row.origin)} → ${esc(row.destination)}</td>
    <td>${esc(row.delay_minutes)}</td><td>${esc(row.affected_passengers)}</td></tr>`).join("");
  bindFlights();
}

async function refreshFlight() {
  if (!openFlight) return;
  const flightId = openFlight;
  let body;
  try {
    body = await request(`/api/flight/${encodeURIComponent(flightId)}`);
  } catch (error) {
    $("status-line").textContent = error.message;
    return;
  }
  if (flightId !== openFlight) return;
  const { flight, passengers } = body;
  $("flight-title").textContent = `${flight.key} passengers`;
  $("flight-sub").textContent = `${flight.origin} → ${flight.destination} · ${flight.status} · `
    + `${flight.delay_minutes} min delay · est. ${hhmm(flight.estimated_time)}`;
  $("flight-empty").classList.add("hidden");
  $("flight-table").classList.remove("hidden");
  $("passengers").innerHTML = passengers.map((row) => `<tr class="${row.key === selected ? "selected" : ""}">
    <td><button class="link" data-passenger="${esc(row.key)}">${esc(row.key)}</button></td>
    <td>${esc(row.final_destination ?? "—")}</td>
    <td>${esc(row.connecting_flight_id ?? "—")}</td><td>${esc(row.connection_minutes ?? "—")}</td>
    <td><span class="risk ${riskClass(row.risk)}">${esc(row.risk)}</span></td></tr>`).join("");
  document.querySelectorAll("[data-passenger]").forEach((button) =>
    button.onclick = () => {
      selected = button.dataset.passenger;
      document.querySelectorAll("#passengers tr").forEach((row) => row.classList.remove("selected"));
      button.closest("tr").classList.add("selected");
      renderPassenger();
    });
}

async function renderPassenger() {
  const card = $("passenger-card");
  if (!selected) return;
  const passengerId = selected;
  let body;
  try {
    body = await request(`/api/passenger/${encodeURIComponent(passengerId)}`);
  } catch (error) {
    $("status-line").textContent = error.message;
    return;
  }
  if (passengerId !== selected) return;
  const { passenger, offers } = body;
  card.className = "pax";
  const facts = [
    ["Inbound", passenger.inbound_flight_id],
    ["Connection", passenger.connecting_flight_id ?? "—"],
    ["Final destination", passenger.final_destination],
    ["Connection minutes", passenger.connection_minutes ?? "—"],
  ].map(([label, value]) => `<div class="fact"><span>${esc(label)}</span><b>${esc(value)}</b></div>`).join("");
  const offerCards = offers.length ? offers.map((offer) => `<div class="offer">
      <div class="kicker">${esc(offer.recommended_flight_id)} · ${esc(offer.status)}</div>
      <div class="msg">${offer.hotel_name ? esc(offer.hotel_name) : "No hotel needed"}
        ${offer.hotel_cost !== null && offer.hotel_cost !== undefined && offer.hotel_cost !== ""
          ? `<span class="cost"> · $${esc(offer.hotel_cost)}</span>` : ""}</div>
      <div class="foot-row"><span class="cost">Offered ${esc(hhmm(offer.recommended_at))}</span>
      ${offer.status === "OFFERED" ? `<button class="btn" data-select="${esc(offer.key)}">Select</button>` : ""}
      ${offer.status === "SELECTED" ? `<button class="btn" data-book="${esc(offer.key)}">Book</button>` : ""}</div></div>`).join("")
    : `<div class="pax-empty">No recovery offers.</div>`;
  card.innerHTML = `<div class="pax-id">${esc(passenger.key)}</div>
    <div class="pax-route"><span class="risk ${riskClass(passenger.risk)}">${esc(passenger.risk)}</span></div>
    <div class="facts">${facts}</div>${offerCards}`;
  document.querySelectorAll("[data-select]").forEach((button) =>
    button.onclick = () => act("select", button.dataset.select));
  document.querySelectorAll("[data-book]").forEach((button) =>
    button.onclick = () => act("book", button.dataset.book));
}

async function act(verb, offerId) {
  try {
    const changed = await request(`/api/passenger/${encodeURIComponent(selected)}/${verb}/${encodeURIComponent(offerId)}`, "POST");
    $("status-line").textContent = `${changed.status}: ${changed.hotel_name || "no hotel needed"}`;
    setTimeout(() => { refresh(); renderPassenger(); }, 1000);
  } catch (error) {
    $("status-line").textContent = error.message;
  }
}

$("refresh").onclick = () => { refresh(); refreshFlight(); renderPassenger(); };
refresh();
window.setInterval(refresh, 2500);
window.setInterval(() => { refreshFlight(); renderPassenger(); }, 5000);
