"use strict";

FlightMap.buildMapSvg("#flight-map");

const $ = (id) => document.getElementById(id);
let current = { flights: [], delays: [], counts: {} };
let openFlight = null;
let selected = null;
let lastUpdated = null;
const rowWatermarks = new Map(); // key -> "status|delay_minutes|affected_passengers", for flash-on-change
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));

// Tweens a big number from its last rendered value to `to` so every 2.5s poll
// feels alive rather than silently jumping — the "engaging, real-time" cue.
function animateNumber(el, to, duration = 600) {
  to = Number(to) || 0;
  const from = Number(el.dataset.value || 0);
  if (from === to) { el.textContent = to; el.dataset.value = to; return; }
  const start = performance.now();
  el.dataset.value = to;
  (function step(now) {
    const progress = Math.min(1, (now - start) / duration);
    el.textContent = Math.round(from + (to - from) * (1 - Math.pow(1 - progress, 3)));
    if (progress < 1) requestAnimationFrame(step);
  })(start);
}

// Briefly flashes a row when a watched field changes between polls, so a
// status/delay/at-risk update is visible, not just quietly redrawn.
function flashIfChanged(rowEl, watchKey, watermark) {
  const previous = rowWatermarks.get(watchKey);
  if (previous !== undefined && previous !== watermark) {
    rowEl.classList.remove("flash");
    // eslint-disable-next-line no-unused-expressions
    rowEl.offsetWidth; // restart the CSS animation
    rowEl.classList.add("flash");
  }
  rowWatermarks.set(watchKey, watermark);
}

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
    lastUpdated = Date.now();
    render();
    $("status-line").textContent = "Live from Lightning Tables";
  } catch (error) {
    $("status-line").textContent = error.message;
  }
}

function flightButton(id) {
  return `<button class="link" data-flight="${esc(id)}">${esc(id)}</button>`;
}

function selectFlight(flightId) {
  // Clicking the already-selected flight again (from the table or the map),
  // or an explicit null (the map's "Show all flights" button), deselects.
  if (flightId === null || flightId === openFlight) {
    openFlight = null;
    selected = null;
    $("flight-empty").classList.remove("hidden");
    $("flight-table").classList.add("hidden");
    $("passenger-card").className = "pax-empty";
    $("passenger-card").textContent = "Select a passenger.";
    render();
    return;
  }
  openFlight = flightId;
  selected = null;
  render();
  refreshFlight(true);
}

function bindFlights() {
  document.querySelectorAll("[data-flight]").forEach((button) =>
    button.onclick = () => selectFlight(button.dataset.flight));
}

function render() {
  const counts = current.counts || {};
  if (!$("counts").dataset.built) {
    $("counts").innerHTML = `
      <div class="count"><b id="count-flights" data-value="0">0</b><span>Flights today</span></div>
      <div class="count tight"><b id="count-delayed" data-value="0">0</b><span>Delayed</span></div>
      <div class="count miss"><b id="count-affected" data-value="0">0</b><span>Passengers at risk</span></div>
      <div class="count ok"><b id="count-booked" data-value="0">0</b><span>Booked</span></div>`;
    $("counts").dataset.built = "1";
  }
  animateNumber($("count-flights"), counts.flights ?? 0);
  animateNumber($("count-delayed"), counts.delayed ?? 0);
  animateNumber($("count-affected"), counts.affected ?? 0);
  animateNumber($("count-booked"), counts.booked ?? 0);

  // Selecting a flight (map or table) filters this table down to it, same
  // as the map hiding every other plane — both reflect the one shared
  // openFlight selection, not two independent states.
  const shownFlights = openFlight ? current.flights.filter((row) => row.key === openFlight) : current.flights;
  $("flights-src").innerHTML = openFlight
    ? `Filtered to 1 of ${current.flights.length} · <button class="link" id="flights-show-all">Show all</button>`
    : "Lightning Tables";
  if (openFlight) $("flights-show-all").onclick = () => selectFlight(null);

  $("flights").innerHTML = shownFlights.map((row) => `<tr id="flight-row-${esc(row.key)}" class="${row.key === openFlight ? "selected" : ""}">
    <td>${flightButton(row.key)}</td><td>${esc(row.origin)} → ${esc(row.destination)}</td>
    <td>${esc(hhmm(row.scheduled_time))}</td>
    <td><span class="risk ${statusClass(row.status)}">${esc(row.status)}</span></td>
    <td>${esc(row.delay_minutes)}</td><td>${esc(row.affected_passengers)}</td></tr>`).join("");
  shownFlights.forEach((row) =>
    flashIfChanged($(`flight-row-${row.key}`), `flight:${row.key}`, `${row.status}|${row.delay_minutes}|${row.affected_passengers}`));

  $("delays").innerHTML = current.delays.map((row, index) => `<tr id="delay-row-${esc(row.key)}" class="${[row.key === openFlight ? "selected" : "", index === 0 ? "top-delay" : ""].join(" ").trim()}">
    <td>${flightButton(row.key)}${index === 0 ? '<span class="impact-tag">Biggest impact</span>' : ""}</td>
    <td>${esc(row.origin)} → ${esc(row.destination)}</td>
    <td>${esc(row.delay_minutes)}</td><td>${esc(row.affected_passengers)}</td></tr>`).join("");
  current.delays.forEach((row) =>
    flashIfChanged($(`delay-row-${row.key}`), `delay:${row.key}`, `${row.delay_minutes}|${row.affected_passengers}`));

  bindFlights();
  $("map-show-all").classList.toggle("hidden", !openFlight);
  FlightMap.renderFlightMap("#flight-map", current.flights, { highlightKey: current.delays[0]?.key, focusedKey: openFlight, onSelect: selectFlight });
}

async function refreshFlight(opening = false) {
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
  if (opening) {
    $("flight-table").classList.remove("reveal");
    // eslint-disable-next-line no-unused-expressions
    $("flight-table").offsetWidth; // restart the CSS animation
    $("flight-table").classList.add("reveal");
  }
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

function tickLiveDot() {
  if (!lastUpdated) return;
  $("live-dot").classList.toggle("stale", Date.now() - lastUpdated > 6000);
}

$("refresh").onclick = () => { refresh(); refreshFlight(); renderPassenger(); };
$("map-show-all").onclick = () => selectFlight(null);
refresh();
window.setInterval(refresh, 2500);
window.setInterval(() => { refreshFlight(); renderPassenger(); }, 5000);
window.setInterval(tickLiveDot, 1000);
