document.addEventListener("DOMContentLoaded", () => {
  const API = "/api";
  const tableView = document.getElementById("processed-view");
  const testView = document.getElementById("test-view");

  const navTable = document.getElementById("nav-table");
  const navTest = document.getElementById("nav-test");

  const showView = (view) => {
    [tableView, testView].forEach((section) =>
      section.classList.remove("active")
    );
    view.classList.add("active");
  };

  showView(tableView);

  navTable.addEventListener("click", () => showView(tableView));
  navTest.addEventListener("click", () => showView(testView));

  //Processed Data Table
  document.getElementById("load-processed")
    .addEventListener("click", async () => {
      try {
        const res = await fetch(`${API}/processed-data?limit=50&offset=0`);
        if (!res.ok) throw new Error("Failed to fetch processed data");
        const data = await res.json();

        const tbody = document.querySelector("#processed-table tbody");
        tbody.innerHTML = "";

        for (const row of data) {
          const tr = document.createElement("tr");
          tr.innerHTML = `
            <td>${row.appointment_id}</td>
            <td>${row.age}</td>
            <td>${row.wait_days}</td>
            <td>${row.scheduled_hour}</td>
            <td>${row.appointment_weekday}</td>
            <td>${row.gender}</td>
            <td>${row.neighbourhood}</td>
            <td>${row.age_group}</td>
            <td>${row.no_show}</td>
          `;
          tbody.appendChild(tr);
        }
      } catch (err) {
        console.error("Error loading processed data:", err);
      }
    });

  //Random Case + Predict
  let currentCase = null;

  document.getElementById("load-random")
    .addEventListener("click", async () => {
      const res = await fetch(`${API}/test-data/random`);
      currentCase = await res.json();

      const container = document.getElementById("case-data");
      container.innerHTML = "";

      for (const [key, value] of Object.entries(currentCase)) {
        const div = document.createElement("div");
        div.textContent = `${key}: ${value}`;
        container.appendChild(div);
      }

      document.getElementById("predict-btn").disabled = false;
      document.getElementById("prediction-result").textContent = "";
    });

  document.getElementById("predict-btn")
    .addEventListener("click", async () => {
      if (!currentCase) return;

      const payload = {
        age: currentCase.age,
        wait_days: currentCase.wait_days,
        scheduled_hour: currentCase.scheduled_hour,
        appointment_weekday: currentCase.appointment_weekday,
        gender: currentCase.gender,
        neighbourhood: currentCase.neighbourhood,
        age_group: currentCase.age_group
      };

      const res = await fetch(`${API}/predict`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      const result = await res.json();
      const prediction = result.prediction ?? result[0]?.prediction;

      document.getElementById("prediction-result").textContent =
        `Predicted No-Show Probability: ${(prediction * 100).toFixed(1)}%`;
    });
});
