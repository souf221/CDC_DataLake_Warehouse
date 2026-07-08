const fmt = new Intl.NumberFormat("fr-FR");
const fmtPct = new Intl.NumberFormat("fr-FR", { maximumFractionDigits: 2 });

async function fetchJson(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path} → HTTP ${res.status}`);
  return res.json();
}

function renderCards(summary) {
  const vaccDate = summary.vaccination_snapshot_date
    ? ` · Vaccination (US) : ${summary.vaccination_snapshot_date}`
    : "";
  document.getElementById("latest-date").textContent = summary.latest_report_date
    ? `Dernière date cas : ${summary.latest_report_date}${vaccDate}`
    : vaccDate;

  const cards = [
    ["Nouveaux cas", summary.total_new_cases],
    ["Décès", summary.total_new_deaths],
    ["États couverts", summary.states_count],
    ["1ères doses (US, cumul)", summary.total_doses],
  ];
  document.getElementById("summary-cards").innerHTML = cards
    .map(([label, value]) => `
      <div class="card">
        <div class="card-label">${label}</div>
        <div class="card-value">${fmt.format(value || 0)}</div>
      </div>`)
    .join("");
}

function lineChart(canvasId, labels, data, color) {
  const ctx = document.getElementById(canvasId);
  return new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [{
        data,
        borderColor: color,
        backgroundColor: color + "33",
        fill: true,
        tension: 0.25,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#93a4c3", maxTicksLimit: 8 } },
        y: { ticks: { color: "#93a4c3" } },
      },
    },
  });
}

function barChart(canvasId, labels, data) {
  const ctx = document.getElementById(canvasId);
  return new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        data,
        backgroundColor: "#5b8def",
      }],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#93a4c3" } },
        y: { ticks: { color: "#93a4c3" } },
      },
    },
  });
}

function renderTableRows(tbodyId, rows, mapper) {
  document.getElementById(tbodyId).innerHTML = rows.map(mapper).join("");
}

async function init() {
  const badge = document.getElementById("status-badge");
  try {
    await fetchJson("/api/health");
    badge.textContent = "Connecté";
    badge.classList.add("ok");

    const [summary, topStates, casesTimeline, deathsTimeline, regional, dashboard] =
      await Promise.all([
        fetchJson("/api/kpi/summary"),
        fetchJson("/api/kpi/top-states?limit=10"),
        fetchJson("/api/kpi/timeline?metric=cases"),
        fetchJson("/api/kpi/timeline?metric=deaths"),
        fetchJson("/api/kpi/regional?limit=15"),
        fetchJson("/api/kpi/dashboard?limit=30"),
      ]);

    renderCards(summary);

    lineChart(
      "cases-chart",
      casesTimeline.map((r) => r.report_date),
      casesTimeline.map((r) => r.value),
      "#3dd6c3",
    );
    lineChart(
      "deaths-chart",
      deathsTimeline.map((r) => r.report_date),
      deathsTimeline.map((r) => r.value),
      "#ff6b7a",
    );
    barChart(
      "states-chart",
      topStates.map((r) => r.state),
      topStates.map((r) => r.total_cases),
    );

    renderTableRows("regional-table", regional, (r) => `
      <tr>
        <td>${r.state}</td>
        <td>${fmt.format(r.total_cases || 0)}</td>
        <td>${fmt.format(r.total_deaths || 0)}</td>
        <td>${fmtPct.format(r.avg_incidence_rate || 0)}</td>
      </tr>`);

    renderTableRows("dashboard-table", dashboard, (r) => `
      <tr>
        <td>${r.report_date || ""}</td>
        <td>${r.state || ""}</td>
        <td>${fmt.format(r.new_cases || 0)}</td>
        <td>${fmt.format(r.new_deaths || 0)}</td>
        <td>${fmt.format(r.doses_administered || 0)}</td>
        <td>${fmtPct.format(r.vaccination_rate || 0)}</td>
      </tr>`);
  } catch (err) {
    badge.textContent = "Erreur API";
    badge.classList.add("err");
    console.error(err);
  }
}

init();
