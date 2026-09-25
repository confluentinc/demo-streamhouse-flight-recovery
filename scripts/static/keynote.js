"use strict";

const $ = (id) => document.getElementById(id);
let current = { flights: [], risk: [], impact: [], counts: {} };
let selected = null;
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));

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

function render() {
  $("counts").innerHTML = `<div class="count"><b>${current.counts.affected}</b><span>At risk</span></div>
    <div class="count ok"><b>${current.counts.booked}</b><span>Booked</span></div>`;
  const impact = new Map(current.impact.map((row) => [row.key, row.affected_passengers]));
  $("flights").innerHTML = current.flights.map((row) => `<tr><td>${esc(row.key)}</td>
    <td>${esc(row.origin)} → ${esc(row.destination)}</td><td>${esc(row.status)}</td>
    <td>${impact.get(row.key) ?? 0}</td></tr>`).join("");
  $("passengers").innerHTML = current.risk.filter((row) => row.risk === "HIGH").slice(0, 30)
    .map((row) => `<tr><td><button class="btn" data-passenger="${esc(row.key)}">${esc(row.key)}</button></td>
      <td>${esc(row.connecting_flight_id)}</td><td>${row.connection_minutes}</td>
      <td><span class="risk MISS">${esc(row.risk)}</span></td></tr>`).join("");
  document.querySelectorAll("[data-passenger]").forEach((button) =>
    button.onclick = () => { selected = button.dataset.passenger; renderPassenger(); });
  renderPassenger();
}

async function renderPassenger() {
  const card = $("passenger-card");
  if (!selected) return;
  const passengerId = selected;
  let offers;
  try {
    ({ offers } = await request(`/api/passenger/${encodeURIComponent(passengerId)}`));
  } catch (error) {
    $("status-line").textContent = error.message;
    return;
  }
  if (passengerId !== selected) return;
  card.className = "pax";
  card.innerHTML = `<div class="pax-id">${esc(selected)}</div>` + offers.map((offer) =>
    `<div class="offer"><div class="kicker">${esc(offer.recommended_flight_id)} · ${esc(offer.status)}</div>
      <div class="msg">${esc(offer.hotel_id)} · $${esc(offer.hotel_cost)}</div>
      <div class="foot-row">${offer.status === "OFFERED" ? `<button class="btn" data-select="${esc(offer.key)}">Select</button>` : ""}
      ${offer.status === "SELECTED" ? `<button class="btn" data-book="${esc(offer.key)}">Book</button>` : ""}</div></div>`).join("");
  document.querySelectorAll("[data-select]").forEach((button) =>
    button.onclick = () => act("select", button.dataset.select));
  document.querySelectorAll("[data-book]").forEach((button) =>
    button.onclick = () => act("book", button.dataset.book));
}

async function act(verb, offerId) {
  try {
    const changed = await request(`/api/passenger/${encodeURIComponent(selected)}/${verb}/${encodeURIComponent(offerId)}`, "POST");
    $("status-line").textContent = `${changed.status}: ${changed.hotel_id}`;
    setTimeout(refresh, 1000);
  } catch (error) {
    $("status-line").textContent = error.message;
  }
}

$("refresh").onclick = refresh;
refresh();
window.setInterval(refresh, 2500);
