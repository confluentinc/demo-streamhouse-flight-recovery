"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {
  findRecommendationChange,
  showRecommendationChange,
} = require("../scripts/static/app.js");

test("detects a replacement for a proposed recovery", () => {
  const previous = [
    { key: "P-1009", status: "PROPOSED", risk: "MISS", action: "Maya, take JA540." },
  ];
  const current = [
    { key: "P-1009", status: "PROPOSED", risk: "TIGHT", action: "Maya, take the shuttle to JA512." },
  ];

  assert.deepEqual(findRecommendationChange(previous, current), current[0]);
});

test("ignores the status-only change produced by approval", () => {
  const previous = [
    { key: "P-1009", status: "PROPOSED", action: "Maya, take the shuttle to JA512." },
  ];
  const current = [
    { key: "P-1009", status: "EXECUTED", action: "Maya, take the shuttle to JA512." },
  ];

  assert.equal(findRecommendationChange(previous, current), null);
});

test("renders the old and replacement plans in the live-update banner", () => {
  const elements = {
    "live-update": { classList: { remove: (name) => assert.equal(name, "hidden") } },
    "live-update-title": { textContent: "" },
    "live-update-copy": { textContent: "" },
  };
  global.document = { getElementById: (id) => elements[id] };

  showRecommendationChange(
    {
      key: "P-1009",
      passenger_name: "Maya Chen",
      connecting_flight: "JA512",
      risk: "TIGHT",
      action: "Maya, take the shuttle to JA512.",
    },
    { risk: "MISS", action: "Maya, take JA540 tomorrow." },
  );

  assert.match(elements["live-update-title"].textContent, /Maya Chen/);
  assert.match(elements["live-update-title"].textContent, /JA512/);
  assert.match(elements["live-update-copy"].textContent, /MISS/);
  assert.match(elements["live-update-copy"].textContent, /TIGHT/);
  assert.match(elements["live-update-copy"].textContent, /JA540/);
  assert.match(elements["live-update-copy"].textContent, /shuttle/);
  delete global.document;
});
