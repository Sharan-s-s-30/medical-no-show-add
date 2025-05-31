document.addEventListener("DOMContentLoaded", () => {
  const API = "/api";
  // View 1: Processed Data Table
  document.getElementById("load-processed")
    .addEventListener("click", async () => {
      const res = await fetch(`${API}/processed-data?limit=50&offset=0`);
      const data = await res.json();
      const tbody = document.querySelector("#processed-table tbody");
      tbody.innerHTML = ""; // clear old rows

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
    });
  // View 2: Random Case + Predict
  let currentCase = null;

  const randomBtn = document.getElementById("load-random");
  const predictBtn = document.getElementById("predict-btn");

  if (randomBtn && predictBtn) {
    randomBtn.addEventListener("click", async () => {
      const res = await fetch(`${API}/test-data/random`);
      currentCase = await res.json();
      const container = document.getElementById("case-data");
      container.innerHTML = "";
      for (const [k, v] of Object.entries(currentCase)) {
        const div = document.createElement("div");
        div.textContent = `${k}: ${v}`;
        container.appendChild(div);
      }
      predictBtn.disabled = false;
      document.getElementById("prediction-result").textContent = "";
    });

    predictBtn.addEventListener("click", async () => {
      if (!currentCase) return;
      const payload = {
        age: currentCase.age,
        wait_days: currentCase.wait_days,
        scheduled_hour: new Date(currentCase.scheduled_day).getHours(),
        appointment_weekday: new Date(currentCase.appointment_day).getDay(),
        gender: currentCase.gender,
        neighbourhood: currentCase.neighbourhood,
        age_group: currentCase.age_group
      };
      const res = await fetch(`${API}/predict`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const [{ prediction }] = await res.json();
      document.getElementById("prediction-result").textContent =
        `Predicted No-Show Probability: ${(prediction * 100).toFixed(1)}%`;
    });
  }
  // View 3: Custom Input Prediction
  const customForm = document.getElementById("custom-form");

  if (customForm) {
    customForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const formData = new FormData(e.target);
      const payload = Object.fromEntries(formData.entries());

      // Convert numeric fields to numbers
      payload.age = parseInt(payload.age);
      payload.wait_days = parseInt(payload.wait_days);
      payload.scheduled_hour = parseInt(payload.scheduled_hour);
      payload.appointment_weekday = parseInt(payload.appointment_weekday);

      const res = await fetch(`${API}/predict`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      const [result] = await res.json();
      document.getElementById("custom-prediction-result").textContent =
        `Predicted No-Show Probability: ${(result.prediction * 100).toFixed(1)}%`;
    });
  }
});
