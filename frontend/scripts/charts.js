(function () {
  const speciesSelect = document.getElementById("speciesSelect");
  const chartCanvas = document.getElementById("trendChart");
  const chartStatus = document.getElementById("chartStatus");
  const timelineSlider = document.getElementById("timelineSlider");
  const timelineLabel = document.getElementById("timelineLabel");

  if (!speciesSelect || !chartCanvas || !chartStatus || !timelineSlider || !timelineLabel) {
    return;
  }

  function setChartStatus(text, isError) {
    chartStatus.textContent = text;
    chartStatus.classList.toggle("chart-status-error", Boolean(isError));
  }

  if (!window.Chart) {
    setChartStatus("Chart library failed to load. Refresh and try again.", true);
    return;
  }

  const chartContext = chartCanvas.getContext("2d");
  if (!chartContext) {
    setChartStatus("Could not initialize chart canvas.", true);
    return;
  }

  let trendChart = null;
  let cachedRows = [];
  let timelineWeeks = [];
  let animationFrameId = null;

  const palette = ["#0a7f64", "#d07a2f", "#2a6fbb", "#7a4bb8", "#ad2f45", "#557c1f"];

  function isoWeekNumber(dateValue) {
    const date = new Date(Date.UTC(
      dateValue.getUTCFullYear(),
      dateValue.getUTCMonth(),
      dateValue.getUTCDate()
    ));
    const dayNum = date.getUTCDay() || 7;
    date.setUTCDate(date.getUTCDate() + 4 - dayNum);
    const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
    const week = Math.ceil((((date - yearStart) / 86400000) + 1) / 7);
    return week;
  }

  function buildYearComparison(rows) {
    const labels = Array.from({ length: 53 }, function (_, index) {
      const week = String(index + 1).padStart(2, "0");
      return "W" + week;
    });

    const byYear = new Map();

    rows.forEach(function (row) {
      const weekStartText = String(row.week_start || "").trim();
      const count = Number(row.observation_count || 0);
      if (!weekStartText || !Number.isFinite(count)) {
        return;
      }

      const parsedDate = new Date(weekStartText + "T00:00:00Z");
      if (Number.isNaN(parsedDate.getTime())) {
        return;
      }

      const year = String(parsedDate.getUTCFullYear());
      const weekIndex = isoWeekNumber(parsedDate) - 1;
      if (weekIndex < 0 || weekIndex > 52) {
        return;
      }

      if (!byYear.has(year)) {
        byYear.set(year, Array(53).fill(null));
      }

      const target = byYear.get(year);
      const prior = target[weekIndex] || 0;
      target[weekIndex] = prior + count;
    });

    const years = Array.from(byYear.keys()).sort();

    const datasets = years.map(function (year, index) {
      const color = palette[index % palette.length];
      return {
        label: year,
        data: byYear.get(year),
        borderColor: color,
        backgroundColor: color,
        borderWidth: 2,
        pointRadius: 1.5,
        pointHoverRadius: 4,
        tension: 0.25,
        spanGaps: true,
      };
    });

    return { labels: labels, datasets: datasets, years: years };
  }

  function destroyChart() {
    if (trendChart) {
      trendChart.destroy();
      trendChart = null;
    }
  }

  function renderChart(labels, datasets) {
    if (!trendChart) {
      trendChart = new Chart(chartContext, {
        type: "line",
        data: {
          labels: labels,
          datasets: datasets,
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: false,
          interaction: {
            mode: "nearest",
            intersect: false,
          },
          plugins: {
            legend: {
              position: "top",
              labels: {
                usePointStyle: true,
                boxWidth: 8,
              },
            },
            tooltip: {
              callbacks: {
                title: function (items) {
                  if (!items || !items.length) {
                    return "";
                  }
                  return "Week " + items[0].label.replace("W", "");
                },
                label: function (context) {
                  const value = Number(context.parsed.y || 0);
                  return context.dataset.label + ": " + value.toLocaleString() + " observations";
                },
              },
            },
          },
          scales: {
            x: {
              title: {
                display: true,
                text: "ISO week number",
              },
              ticks: {
                maxTicksLimit: 10,
              },
            },
            y: {
              beginAtZero: true,
              title: {
                display: true,
                text: "Observation count",
              },
              ticks: {
                precision: 0,
              },
            },
          },
        },
      });
      return;
    }

    trendChart.data.labels = labels;
    trendChart.data.datasets = datasets;
    trendChart.update("none");
  }

  function emitTimelineChange(weekStart) {
    document.dispatchEvent(
      new CustomEvent("birdbird:timeline-change", {
        detail: { weekStart: weekStart || "" },
      })
    );
  }

  function configureTimeline(weeks) {
    timelineWeeks = weeks;

    if (!weeks.length) {
      timelineSlider.disabled = true;
      timelineSlider.min = "0";
      timelineSlider.max = "0";
      timelineSlider.value = "0";
      timelineLabel.textContent = "No weekly records available.";
      emitTimelineChange("");
      return;
    }

    timelineSlider.disabled = false;
    timelineSlider.min = "0";
    timelineSlider.max = String(weeks.length - 1);
    timelineSlider.value = String(weeks.length - 1);
  }

  function applyTimelineIndex(index, queryMs, cacheHit) {
    if (!timelineWeeks.length || !cachedRows.length) {
      return;
    }

    const safeIndex = Math.max(0, Math.min(index, timelineWeeks.length - 1));
    const selectedWeek = timelineWeeks[safeIndex];

    const filteredRows = cachedRows.filter(function (row) {
      return String(row.week_start || "") <= selectedWeek;
    });

    const built = buildYearComparison(filteredRows);
    if (!built.datasets.length) {
      destroyChart();
      setChartStatus("Weekly trend rows were available but chart rendering failed.", true);
      emitTimelineChange(selectedWeek);
      return;
    }

    renderChart(built.labels, built.datasets);

    const suffix = typeof queryMs === "number" ? " (query " + queryMs.toFixed(1) + " ms)" : "";
    const cacheText = cacheHit ? " [cache]" : "";
    const labelDate = new Date(selectedWeek + "T00:00:00Z");
    const labelText = Number.isNaN(labelDate.getTime())
      ? selectedWeek
      : labelDate.toLocaleDateString("en-GB", { year: "numeric", month: "short", day: "numeric" });

    timelineLabel.textContent =
      "Week " + String(safeIndex + 1) + " of " + String(timelineWeeks.length) + " (up to " + labelText + ")";

    setChartStatus(
      "Showing " + built.years.length + " year line(s) across " + filteredRows.length + " weekly records" + suffix + cacheText + ".",
      false
    );

    emitTimelineChange(selectedWeek);
  }

  function scheduleTimelineUpdate(index) {
    if (animationFrameId !== null) {
      cancelAnimationFrame(animationFrameId);
    }
    animationFrameId = requestAnimationFrame(function () {
      animationFrameId = null;
      applyTimelineIndex(index);
    });
  }

  function loadTrendFromBundle(detail) {
    const weeklyRows = Array.isArray(detail.weekly) ? detail.weekly : [];
    const weeks = Array.isArray(detail.timelineWeeks)
      ? detail.timelineWeeks
      : Array.from(
        new Set(
          weeklyRows
            .map(function (row) {
              return String(row.week_start || "").trim();
            })
            .filter(Boolean)
        )
      ).sort();

    if (!weeklyRows.length || !weeks.length) {
      cachedRows = [];
      configureTimeline([]);
      destroyChart();
      setChartStatus("No weekly trend rows returned for this species.", true);
      return;
    }

    cachedRows = weeklyRows;
    configureTimeline(weeks);

    const meta = detail.meta && typeof detail.meta === "object" ? detail.meta : {};
    const queryMs = typeof meta.query_ms === "number" ? meta.query_ms : undefined;
    const cacheHit = Boolean(meta.cache_hit);

    applyTimelineIndex(weeks.length - 1, queryMs, cacheHit);
  }

  timelineSlider.addEventListener("input", function (event) {
    const value = Number(event.target && event.target.value);
    if (!Number.isFinite(value)) {
      return;
    }
    scheduleTimelineUpdate(value);
  });

  document.addEventListener("birdbird:species-change", function (event) {
    const speciesCode = event && event.detail && typeof event.detail.speciesCode === "string"
      ? event.detail.speciesCode
      : "";
    if (!speciesCode) {
      cachedRows = [];
      timelineWeeks = [];
      configureTimeline([]);
      destroyChart();
      setChartStatus("Choose a species to show weekly trend data.", false);
      return;
    }

    setChartStatus("Loading weekly trend data...", false);
  });

  document.addEventListener("birdbird:species-data", function (event) {
    const detail = event && event.detail && typeof event.detail === "object" ? event.detail : null;
    if (!detail) {
      return;
    }

    const selectedCode = speciesSelect.value;
    if (!selectedCode || detail.speciesCode !== selectedCode) {
      return;
    }

    loadTrendFromBundle(detail);
  });

  setChartStatus("Waiting for species selection...", false);
})();
