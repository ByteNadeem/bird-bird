(function () {
  const speciesSelect = document.getElementById("speciesSelect");
  const statusNode = document.getElementById("collectiblesStatus");
  const gridNode = document.getElementById("collectiblesGrid");

  if (!speciesSelect || !statusNode || !gridNode) {
    return;
  }

  const apiBase = window.location.protocol === "file:" ? "http://127.0.0.1:5000" : "";
  const cache = new Map();
  const maxCards = 9;

  function setStatus(text, isError) {
    statusNode.textContent = text;
    statusNode.classList.toggle("is-error", Boolean(isError));
  }

  function clearGrid(message) {
    gridNode.innerHTML = "";
    const empty = document.createElement("div");
    empty.className = "collectible-empty";
    empty.textContent = message;
    gridNode.appendChild(empty);
  }

  function formatDate(text) {
    if (!text) {
      return "n/a";
    }
    const parsed = new Date(String(text).replace(" ", "T") + "Z");
    if (Number.isNaN(parsed.getTime())) {
      return String(text);
    }
    return parsed.toLocaleDateString("en-GB", {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  }

  function rarityTier(observationCount) {
    const value = Number(observationCount || 0);
    if (value >= 50000) {
      return "Legendary";
    }
    if (value >= 5000) {
      return "Epic";
    }
    if (value >= 500) {
      return "Rare";
    }
    return "Common";
  }

  function buildMetaLine(label, value) {
    const line = document.createElement("div");
    line.textContent = label + ": " + value;
    return line;
  }

  function buildCard(row) {
    const card = document.createElement("article");
    card.className = "collectible-card";

    const head = document.createElement("div");
    head.className = "collectible-head";

    const name = document.createElement("div");
    name.className = "collectible-name";
    name.textContent = String(row.display_name || "Unknown Bird");

    const tier = document.createElement("span");
    tier.className = "collectible-tier";
    tier.textContent = rarityTier(row.observation_count);

    head.appendChild(name);
    head.appendChild(tier);

    const species = document.createElement("div");
    species.className = "collectible-species";
    const speciesText = String(row.common_name || "Unknown species");
    const speciesCode = String(row.species_code || "unknown");
    species.textContent = speciesText + " (" + speciesCode + ")";

    const meta = document.createElement("div");
    meta.className = "collectible-meta";
    meta.appendChild(buildMetaLine("Observations", Number(row.observation_count || 0).toLocaleString()));
    meta.appendChild(buildMetaLine("Active weeks", Number(row.active_weeks || 0).toLocaleString()));
    meta.appendChild(buildMetaLine("First seen", formatDate(row.first_seen)));
    meta.appendChild(buildMetaLine("Last seen", formatDate(row.last_seen)));
    meta.appendChild(buildMetaLine("Deployment", String(row.deployment_id || "n/a")));

    card.appendChild(head);
    card.appendChild(species);
    card.appendChild(meta);

    return card;
  }

  function renderRows(rows, speciesCode, fromCache) {
    const shownRows = rows.slice(0, maxCards);

    if (!shownRows.length) {
      clearGrid("No collectible profiles are available for this species yet.");
      setStatus("No internal collectible profiles matched this species.", false);
      return;
    }

    gridNode.innerHTML = "";
    const fragment = document.createDocumentFragment();
    shownRows.forEach(function (row) {
      fragment.appendChild(buildCard(row));
    });
    gridNode.appendChild(fragment);

    const cacheText = fromCache ? " [cache]" : "";
    const speciesText = speciesCode ? " for " + speciesCode : "";
    setStatus(
      "Showing " + shownRows.length + " collectible card(s)" + speciesText + cacheText + ".",
      false
    );
  }

  async function fetchCollectibles(speciesCode) {
    if (!speciesCode) {
      clearGrid("Choose a species to preview collectible bird cards.");
      setStatus("Waiting for species selection...", false);
      return;
    }

    const cacheKey = speciesCode;
    if (cache.has(cacheKey)) {
      renderRows(cache.get(cacheKey), speciesCode, true);
      return;
    }

    setStatus("Loading internal collectible profiles...", false);

    const query =
      "/api/internal/collectibles/individuals?limit=50&species_code=" + encodeURIComponent(speciesCode);

    try {
      const response = await fetch(apiBase + query);
      if (!response.ok) {
        throw new Error("collectibles-api-not-ok");
      }

      const payload = await response.json();
      const rows = Array.isArray(payload.data) ? payload.data : [];
      cache.set(cacheKey, rows);
      renderRows(rows, speciesCode, false);
    } catch (_) {
      clearGrid("Collector cards are temporarily unavailable.");
      setStatus("Internal collectibles API unavailable. Keep endpoint private until UI stabilizes.", true);
    }
  }

  document.addEventListener("birdbird:species-change", function (event) {
    const speciesCode = event && event.detail && typeof event.detail.speciesCode === "string"
      ? event.detail.speciesCode
      : "";
    fetchCollectibles(speciesCode);
  });

  setStatus("Waiting for species selection...", false);
  clearGrid("Choose a species to preview collectible bird cards.");

  if (speciesSelect.value) {
    fetchCollectibles(speciesSelect.value);
  }
})();
