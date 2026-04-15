(function () {
  const speciesSelect = document.getElementById("speciesSelect");
  const apiStatus = document.getElementById("apiStatus");
  const mapElement = document.getElementById("map");

  const apiBase = window.location.protocol === "file:" ? "http://127.0.0.1:5000" : "";

  if (!mapElement || !speciesSelect || !apiStatus) {
    return;
  }

  const visualizationCache = new Map();
  let speciesListCache = null;
  let currentRouteRows = [];
  let selectedTimelineWeek = "";
  let latestStatusSuffix = "";
  let legendSourceNode = null;

  const urlParams = new URLSearchParams(window.location.search);
  const routeRecentPct = clampInteger(urlParams.get("recent_pct"), 33, 5, 95);

  function clampInteger(rawValue, fallback, minValue, maxValue) {
    const numeric = Number.parseInt(String(rawValue || ""), 10);
    if (!Number.isFinite(numeric)) {
      return fallback;
    }
    return Math.min(maxValue, Math.max(minValue, numeric));
  }

  function formatSourceBreakdown(sourceBreakdown) {
    if (!sourceBreakdown || typeof sourceBreakdown !== "object") {
      return "Sources: n/a";
    }

    const parts = Object.entries(sourceBreakdown)
      .filter(function (entry) {
        return Number(entry[1]) > 0;
      })
      .sort(function (a, b) {
        return Number(b[1]) - Number(a[1]);
      })
      .map(function (entry) {
        return entry[0] + " " + Number(entry[1]).toLocaleString();
      });

    return parts.length ? "Sources: " + parts.join(", ") : "Sources: n/a";
  }

  function announceSpeciesSelection(speciesCode) {
    document.dispatchEvent(
      new CustomEvent("birdbird:species-change", {
        detail: { speciesCode: speciesCode || "" },
      })
    );
  }

  function announceSpeciesData(payload) {
    document.dispatchEvent(
      new CustomEvent("birdbird:species-data", {
        detail: payload,
      })
    );
  }

  function setStatus(text, isError) {
    apiStatus.textContent = text;
    apiStatus.classList.toggle("status-error", Boolean(isError));
  }

  function initializeSplitTooltip() {
    const tip = document.getElementById("splitFormatTip");
    if (!tip) {
      return;
    }

    const historicalPct = 100 - routeRecentPct;
    const detail = "Recent/Historical " + routeRecentPct + "/" + historicalPct;
    const helpText = detail + ". Change via URL query: ?recent_pct=60";

    tip.textContent = routeRecentPct + "/" + historicalPct;
    tip.title = helpText;
    tip.setAttribute("aria-label", helpText);
  }

  function setFallbackOption(text) {
    speciesSelect.innerHTML = "";
    const option = document.createElement("option");
    option.value = "";
    option.textContent = text;
    speciesSelect.appendChild(option);
  }

  if (!window.L) {
    setFallbackOption("Leaflet unavailable");
    setStatus("Leaflet failed to load. Check network and refresh.", true);
    return;
  }

  const map = L.map("map", {
    zoomControl: true,
    preferCanvas: true,
  }).setView([50.2, -5.4], 7);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(map);

  const routeLine = L.polyline([], {
    color: "#0a7f64",
    weight: 3,
    opacity: 0.86,
  }).addTo(map);

  const markerLayer = L.layerGroup().addTo(map);

  const legend = L.control({ position: "bottomright" });
  legend.onAdd = function () {
    const div = L.DomUtil.create("div", "map-legend");
    div.innerHTML =
      "<strong>Legend</strong>" +
      '<div class="map-legend-item"><span class="map-legend-swatch map-legend-route"></span> Migration route</div>' +
      '<div class="map-legend-item"><span class="map-legend-swatch map-legend-stop"></span> Sample stop points</div>';

    legendSourceNode = L.DomUtil.create("div", "map-legend-source", div);
    legendSourceNode.textContent = "Sources: pending";
    return div;
  };
  legend.addTo(map);

  initializeSplitTooltip();

  async function fetchJson(pathWithQuery) {
    const response = await fetch(apiBase + pathWithQuery);
    if (!response.ok) {
      throw new Error("api-not-ok");
    }
    return response.json();
  }

  function populateSpeciesOptions(species) {
    speciesSelect.innerHTML = "";

    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "Choose a species";
    speciesSelect.appendChild(placeholder);

    species.forEach(function (item) {
      const option = document.createElement("option");
      option.value = item.species_code;
      option.textContent = item.common_name + " (" + item.species_code + ")";
      speciesSelect.appendChild(option);
    });
  }

  function clearMap() {
    routeLine.setLatLngs([]);
    markerLayer.clearLayers();
    map.setView([50.2, -5.4], 7);
  }

  function filterRowsByTimeline(rows) {
    if (!selectedTimelineWeek) {
      return rows;
    }
    return rows.filter(function (row) {
      return String(row.week_start || "") <= selectedTimelineWeek;
    });
  }

  function drawRows(rows) {
    const filtered = filterRowsByTimeline(rows);

    const points = filtered
      .map(function (row) {
        const lat = Number(row.latitude);
        const lon = Number(row.longitude);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
          return null;
        }
        return [lat, lon];
      })
      .filter(Boolean);

    routeLine.setLatLngs(points);
    markerLayer.clearLayers();

    if (points.length === 0) {
      map.setView([50.2, -5.4], 7);
      return 0;
    }

    const markerStep = Math.max(1, Math.floor(points.length / 80));
    for (let i = 0; i < points.length; i += markerStep) {
      L.circleMarker(points[i], {
        radius: 3,
        weight: 1,
        color: "#8e4d17",
        fillColor: "#d07a2f",
        fillOpacity: 0.75,
      }).addTo(markerLayer);
    }

    const bounds = L.latLngBounds(points);
    if (bounds.isValid()) {
      map.fitBounds(bounds.pad(0.15));
    }

    return points.length;
  }

  function redrawFromCache() {
    const pointCount = drawRows(currentRouteRows);
    if (!currentRouteRows.length) {
      return;
    }
    setStatus("Loaded " + pointCount + " route points" + latestStatusSuffix + ".", false);
  }

  function applyVisualizationPayload(bundle, options) {
    const detail = bundle.data && typeof bundle.data === "object" ? bundle.data : {};
    const meta = bundle.meta && typeof bundle.meta === "object" ? bundle.meta : {};

    const routeRows = Array.isArray(detail.route_points) ? detail.route_points : [];
    currentRouteRows = routeRows;

    const pointCount = drawRows(routeRows);
    const queryMs = typeof meta.query_ms === "number" ? meta.query_ms : null;
    const cacheHit = Boolean(meta.cache_hit || (options && options.cacheHit));
    const suffix = queryMs === null ? "" : " (query " + queryMs.toFixed(1) + " ms)";
    const cacheText = cacheHit ? " [cache]" : "";
    const sourceText = formatSourceBreakdown(meta.source_breakdown);
    const splitText = Number.isFinite(Number(meta.recent_pct))
      ? " mix " + Number(meta.recent_pct) + "/" + Number(meta.historical_pct || (100 - Number(meta.recent_pct)))
      : "";

    latestStatusSuffix = splitText + " | " + sourceText + cacheText;

    if (legendSourceNode) {
      legendSourceNode.textContent = sourceText;
    }

    setStatus("Loaded " + pointCount + " route points" + suffix + latestStatusSuffix + ".", false);

    announceSpeciesData({
      speciesCode: meta.species_code || speciesSelect.value || "",
      weekly: Array.isArray(detail.weekly) ? detail.weekly : [],
      routePoints: routeRows,
      timelineWeeks: Array.isArray(detail.timeline_weeks) ? detail.timeline_weeks : [],
      meta: meta,
    });
  }

  async function loadVisualizationForSpecies(speciesCode) {
    if (!speciesCode) {
      currentRouteRows = [];
      clearMap();
      setStatus("Choose a species to display migration routes.", false);
      return;
    }

    selectedTimelineWeek = "";
    setStatus("Loading visualisation data...", false);

    const cacheKey = speciesCode + "|recent_pct=" + routeRecentPct;
    const cached = visualizationCache.get(cacheKey);
    if (cached) {
      applyVisualizationPayload(cached, { cacheHit: true });
      return;
    }

    try {
      const payload = await fetchJson(
        "/api/visualization/?species_code="
          + encodeURIComponent(speciesCode)
          + "&limit_weekly=5000&limit_points=3500&max_route_points=1200"
          + "&recent_pct=" + encodeURIComponent(String(routeRecentPct))
      );
      visualizationCache.set(cacheKey, payload);
      applyVisualizationPayload(payload, { cacheHit: false });
    } catch (_) {
      currentRouteRows = [];
      clearMap();
      setStatus("Could not load visualisation data. Ensure backend/app.py is running.", true);
    }
  }

  async function loadSpecies() {
    setStatus("Connecting to API...", false);

    try {
      if (speciesListCache === null) {
        speciesListCache = await fetchJson("/api/species?limit=200");
      }

      const species = Array.isArray(speciesListCache.data) ? speciesListCache.data : [];

      const filtered = species
        .filter(function (item) {
          return item.species_code !== "unknown" && Number(item.observation_count) > 0;
        })
        .sort(function (a, b) {
          return String(a.common_name || "").localeCompare(String(b.common_name || ""));
        });

      if (filtered.length === 0) {
        setFallbackOption("No species available");
        setStatus("Connected, but no usable species records were returned.", true);
        return;
      }

      populateSpeciesOptions(filtered);

      const defaultSpecies = filtered[0].species_code;
      speciesSelect.value = defaultSpecies;
      announceSpeciesSelection(defaultSpecies);
      await loadVisualizationForSpecies(defaultSpecies);
    } catch (_) {
      setFallbackOption("Unable to load species");
      setStatus("Could not reach API. Start backend/app.py and open /ui.", true);
    }
  }

  speciesSelect.addEventListener("change", function (event) {
    const value = event.target && typeof event.target.value === "string" ? event.target.value : "";
    announceSpeciesSelection(value);
    loadVisualizationForSpecies(value);
  });

  document.addEventListener("birdbird:timeline-change", function (event) {
    const week = event && event.detail && typeof event.detail.weekStart === "string"
      ? event.detail.weekStart
      : "";
    selectedTimelineWeek = week;
    if (currentRouteRows.length) {
      redrawFromCache();
    }
  });

  loadSpecies();
})();
