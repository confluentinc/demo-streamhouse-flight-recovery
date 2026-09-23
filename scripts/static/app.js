"use strict";

let state = { passengers: [], counts: {} };
let selectedKey = null;
let refreshing = false;

const REFRESH_INTERVAL_MS = 2500;

const $ = (id) => document.getElementById(id);

function riskClass(risk) {
  return risk === "MISS" || risk === "TIGHT" ? risk : "other";
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
  );
}

function findRecommendationChange(previous, current) {
  if (!previous.length) return null;
  const previousByKey = new Map(previous.map((p) => [p.key, p]));
  const changed = current.filter((p) => {
    const prior = previousByKey.get(p.key);
    return (
      prior?.status === "PROPOSED" &&
      p.status === "PROPOSED" &&
      prior.action &&
      p.action &&
      prior.action !== p.action
    );
  });
  return changed.find((p) => p.key === selectedKey) || changed[0] || null;
}

function showRecommendationChange(passenger, previous) {
  const banner = $("live-update");
  banner.classList.remove("hidden");
  $("live-update-title").textContent =
    `${passenger.connecting_flight} moved. ${passenger.passenger_name ?? passenger.key}'s original connection is possible again.`;
  $("live-update-copy").textContent =
    `Current Passenger State changed from ${previous.risk} to ${passenger.risk}. The agent replaced “${previous.action}” with “${passenger.action}”.`;
  selectedKey = passenger.key;
}

async function loadState({ quiet = false } = {}) {
  if (refreshing) return;
  refreshing = true;
  if (!quiet) $("status-line").textContent = "Querying Lightning Tables…";
  try {
    const resp = await fetch("/api/state");
    if (!resp.ok) throw new Error((await resp.json()).detail || resp.statusText);
    const nextState = await resp.json();
    const changed = findRecommendationChange(state.passengers, nextState.passengers);
    if (changed) {
      const previous = state.passengers.find((p) => p.key === changed.key);
      showRecommendationChange(changed, previous);
    }
    state = nextState;
    render();
    $("status-line").textContent =
      `${state.passengers.length} passengers · live from passenger_journey + passenger_recovery`;
  } catch (err) {
    $("status-line").textContent = "Error: " + err.message;
  } finally {
    refreshing = false;
  }
}

function render() {
  renderCounts();
  renderOps();
  renderPassenger();
}

function renderCounts() {
  const c = state.counts || {};
  $("counts").innerHTML = `
    <div class="count"><b>${c.total ?? 0}</b><span>Passengers</span></div>
    <div class="count miss"><b>${c.miss ?? 0}</b><span>Will miss</span></div>
    <div class="count tight"><b>${c.tight ?? 0}</b><span>Tight</span></div>
    <div class="count ok"><b>${c.executed ?? 0}</b><span>Executed</span></div>`;
}

function renderOps() {
  const body = $("ops-body");
  body.innerHTML = "";
  for (const p of state.passengers) {
    const tr = document.createElement("tr");
    if (p.key === selectedKey) tr.className = "selected";
    tr.onclick = () => selectPassenger(p.key);
    const executed = p.status === "EXECUTED";
    const actionCell = p.action
      ? `<td class="action">${esc(p.action)}</td>`
      : `<td class="action">—</td>`;
    const btnCell = p.recovery_type
      ? executed
        ? `<td><span class="tag-exec">✓ Executed</span></td>`
        : `<td><button class="btn" onclick="event.stopPropagation(); approve('${esc(p.key)}')">Approve</button></td>`
      : `<td></td>`;
    tr.innerHTML = `
      <td><b>${esc(p.passenger_name ?? p.key)}</b><span class="pax-key">${esc(p.key)}</span></td>
      <td>${esc(p.connecting_flight)}</td>
      <td>${esc(p.destination)}</td>
      <td>${p.minutes_to_departure ?? "—"}m</td>
      <td><span class="risk ${riskClass(p.risk)}">${esc(p.risk)}</span></td>
      ${actionCell}
      ${btnCell}`;
    body.appendChild(tr);
  }
}

function renderPassenger() {
  const card = $("pax-card");
  const p = state.passengers.find((x) => x.key === selectedKey);
  if (!p) {
    card.className = "pax-empty";
    card.innerHTML =
      "<p>Select a passenger from the operations view to see their live status and concierge recovery offer.</p>";
    $("pax-src").textContent = "select a passenger";
    return;
  }
  $("pax-src").textContent = "same entity, passenger view";
  card.className = "pax";
  const executed = p.status === "EXECUTED";
  const offer = p.action
    ? `<div class="offer">
         <div class="kicker">${esc(p.recovery_type)} · concierge</div>
         <div class="msg">${esc(p.action)}</div>
         <div class="foot-row">
           <span class="${executed ? "tag-exec" : ""}">${executed ? "✓ Executed" : "Proposed"}</span>
           ${executed ? "" : `<button class="btn" onclick="approve('${esc(p.key)}')">Approve &amp; execute</button>`}
         </div>
       </div>`
    : `<div class="offer"><div class="msg">No recovery action needed.</div></div>`;
  card.innerHTML = `
    <div class="pax-id">${esc(p.passenger_name ?? p.key)}</div>
    <div class="pax-route"><span class="pax-key">${esc(p.key)}</span> · Connection <b>${esc(p.connecting_flight)}</b> → ${esc(p.destination)} · gate ${esc(p.gate)}</div>
    <div class="facts">
      <div class="fact"><span>Risk</span><b class="risk ${riskClass(p.risk)}">${esc(p.risk)}</b></div>
      <div class="fact"><span>Departs in</span><b>${p.minutes_to_departure ?? "—"} min</b></div>
      <div class="fact"><span>Bag</span><b>${esc(p.bag_status)}${p.needs_recheck ? " · recheck" : ""}</b></div>
      <div class="fact"><span>Makes connection</span><b>${p.make_connection ? "Yes" : "No"}</b></div>
    </div>
    ${offer}`;
}

function selectPassenger(key) {
  selectedKey = key;
  render();
}

async function approve(key) {
  $("status-line").textContent = `Executing recovery for ${key}…`;
  try {
    const resp = await fetch(`/api/passenger/${encodeURIComponent(key)}/approve`, { method: "POST" });
    if (!resp.ok) throw new Error((await resp.json()).detail || resp.statusText);
    // Give the write a moment to land in the Lightning-materialized table, then refresh.
    setTimeout(loadState, 1200);
    $("status-line").textContent = `Recovery executed for ${key} — refreshing…`;
  } catch (err) {
    $("status-line").textContent = "Approve failed: " + err.message;
  }
}

if (typeof module !== "undefined") {
  module.exports = { findRecommendationChange, showRecommendationChange };
}

if (typeof window !== "undefined") {
  window.approve = approve;
  $("refresh").onclick = () => loadState();
  loadState().then(() => window.setInterval(() => loadState({ quiet: true }), REFRESH_INTERVAL_MS));
}
