(function () {
  const APP_BASE = window.__APP_BASE__ || "";
  const STORAGE_KEY = "loggannt.rotas.v1";
  const STORAGE_SELECTION_KEY = "loggannt.rotas.selection";
  const RELOAD_FLAG = "loggannt.ha.synced.once";

  function q(sel, root = document) {
    return root.querySelector(sel);
  }

  function qa(sel, root = document) {
    return Array.from(root.querySelectorAll(sel));
  }

  function apiUrl(path) {
    return `${APP_BASE}${path}`;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function sameJson(a, b) {
    try {
      return JSON.stringify(a) === JSON.stringify(b);
    } catch {
      return false;
    }
  }

  function readJson(key, fallback) {
    try {
      const raw = window.localStorage.getItem(key);
      if (!raw) return fallback;
      return JSON.parse(raw);
    } catch {
      return fallback;
    }
  }

  function writeJson(key, value) {
    window.localStorage.setItem(key, JSON.stringify(value));
  }

  async function syncViewerState(forceSelectId = null) {
    const resp = await fetch(apiUrl("/api/viewer_sync"), { cache: "no-store" });
    if (!resp.ok) {
      throw new Error(`Viewer sync failed: ${resp.status}`);
    }

    const records = await resp.json();
    const existing = readJson(STORAGE_KEY, []);
    const existingSelection = readJson(STORAGE_SELECTION_KEY, []);

    const changed = !sameJson(existing, records);

    if (changed) {
      writeJson(STORAGE_KEY, records);
    }

    if (forceSelectId) {
      writeJson(STORAGE_SELECTION_KEY, [forceSelectId]);
    } else if ((!existingSelection || !existingSelection.length) && records.length) {
      writeJson(STORAGE_SELECTION_KEY, [records[0].id]);
    }

    return changed;
  }


  async function deleteUploadByViewerId(viewerId) {
    const match = /^upload-(\d+)$/.exec((viewerId || "").trim());
    if (!match) {
      throw new Error("Invalid upload id");
    }
    const uploadId = match[1];
    const resp = await fetch(apiUrl(`/api/upload/${uploadId}`), {
      method: "DELETE",
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || `Delete failed: ${resp.status}`);
    }
    return resp.json();
  }

  function hideLegacyPdfSection() {
    const testingHeading = qa("h2,h3,legend").find((el) =>
      (el.textContent || "").trim() === "Testing"
    );
    if (testingHeading) {
      const block = testingHeading.closest(".control-group, .panel-section, section, div");
      if (block) block.style.display = "none";
    }

    const uploadPdfBtn = q("#btnUploadPdf");
    if (uploadPdfBtn) {
      const block = uploadPdfBtn.closest(".control-group, .panel-section, section, div");
      if (block) block.style.display = "none";
    }
  }

  function patchCopy() {
    const intro = qa("p").find((el) =>
      (el.textContent || "").includes("Load an exported rota CSV")
    );
    if (intro) {
      intro.textContent =
        "Upload a rota PDF to review weekly coverage, timelines, and printable PDFs. Saved weeks now come from the Home Assistant database.";
    }

    const footer = q(".footer");
    if (footer) {
      footer.textContent =
        "Server-backed in Home Assistant: rota PDFs are parsed by the add-on, saved in SQLite, and rendered here using the original Loggannt viewer.";
    }

    const hint = qa("p,.control-hint").find((el) =>
      (el.textContent || "").includes("Upload new schedules and manage the saved weeks stored on this device.")
    );
    if (hint) {
      hint.textContent =
        "Upload new rota PDFs and manage the saved weeks stored in Home Assistant.";
    }
  }

  function replaceUploadButton() {
    const oldBtn = q("#btnUpload");
    if (!oldBtn) return;

    const newBtn = oldBtn.cloneNode(true);
    newBtn.id = "btnUploadHaPdf";
    newBtn.textContent = "Upload PDF";
    oldBtn.replaceWith(newBtn);

    let input = q("#haPdfUploadInput");
    if (!input) {
      input = document.createElement("input");
      input.type = "file";
      input.accept = "application/pdf";
      input.id = "haPdfUploadInput";
      input.style.display = "none";
      document.body.appendChild(input);
    }

    newBtn.addEventListener("click", () => {
      input.value = "";
      input.click();
    });

    input.addEventListener("change", async () => {
      const file = input.files && input.files[0];
      if (!file) return;

      const originalLabel = newBtn.textContent;
      newBtn.textContent = "Uploading...";

      try {
        const formData = new FormData();
        formData.append("file", file);

        const resp = await fetch(apiUrl("/api/upload_pdf"), {
          method: "POST",
          body: formData,
        });

        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(text || `Upload failed: ${resp.status}`);
        }

        const data = await resp.json();
        await syncViewerState(data.viewer_id);
        sessionStorage.setItem(RELOAD_FLAG, "1");
        window.location.reload();
      } catch (err) {
        console.error(err);
        alert(`Upload failed: ${err.message}`);
        newBtn.textContent = originalLabel;
      }
    });
  }

  function ensureNotificationStyles() {
    if (document.getElementById("haNotificationSettingsStyles")) return;
    const style = document.createElement("style");
    style.id = "haNotificationSettingsStyles";
    style.textContent = `
      .ha-notification-panel{margin-top:12px;padding-top:12px;border-top:1px solid var(--border)}
      .ha-notification-panel .control-group{display:grid;gap:6px;margin-bottom:10px}
      .ha-notification-panel label{font-size:12px;font-weight:600;color:var(--ink)}
      .ha-notification-panel input,.ha-notification-panel textarea,.ha-notification-panel select{width:100%;border:1px solid var(--border);border-radius:10px;padding:8px 10px;background:#fff;color:var(--ink);font:inherit}
      .ha-notification-panel .actions{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
      .ha-notification-panel .actions .btn-primary{grid-column:span 2}
      .ha-notification-status{font-size:12px;color:var(--muted);margin:6px 0 0}
      .ha-preview{background:#fff;border:1px solid var(--border);border-radius:10px;padding:10px;font-size:12px;white-space:pre-wrap;word-break:break-word;max-height:180px;overflow:auto}
      .subject-pills{display:flex;flex-wrap:wrap;gap:8px}
      .subject-pill{border:1px solid var(--border);border-radius:999px;padding:6px 12px;background:#fff;cursor:pointer;font-size:12px}
      .subject-pill.active{background:var(--accent);color:#fff;border-color:var(--accent)}
      .pairing-popout{border:1px solid var(--border);border-radius:10px;padding:8px;background:var(--surface-soft)}
      .pairing-row{display:grid;grid-template-columns:1fr 1fr auto auto;gap:6px;align-items:center;margin-bottom:6px}
      .pair-critical{display:flex;align-items:center;gap:4px;font-size:11px;white-space:nowrap}
      .ha-debug-log{background:#fff;border:1px solid var(--border);border-radius:10px;padding:10px;font-size:12px;max-height:220px;overflow:auto}
      .ha-debug-log-item{padding:6px 0;border-bottom:1px solid var(--border)}
      .ha-debug-log-item:last-child{border-bottom:none}
    `;
    document.head.appendChild(style);
  }

  function injectNotificationPanel() {
    const panel = q("#savedWeeksPanel");
    if (!panel || q("#haNotificationPanel", panel)) return;

    ensureNotificationStyles();

    const wrapper = document.createElement("section");
    wrapper.className = "ha-notification-panel";
    wrapper.id = "haNotificationPanel";
    wrapper.innerHTML = `
      <h3 class="panel-title">Notification automation</h3>
      <p class="control-hint">Notifications trigger 2 hours before each selected subject's shift start time.</p>
      <div class="control-group">
        <label><input id="notifEnabled" type="checkbox"> Enabled</label>
      </div>
      <div class="control-group">
        <label>Subjects to notify</label>
        <div id="notifSubjectPills" class="subject-pills"></div>
      </div>
      <details class="pairing-popout">
        <summary>Pair subjects to notify services</summary>
        <div id="notifPairingRows"></div>
        <button class="btn btn-secondary" id="notifAddPairing" type="button">Add pair</button>
      </details>
      <div class="control-group">
        <label>Weekdays</label>
        <div class="days-grid" id="notifWeekdays"></div>
      </div>
      <div class="control-group">
        <label for="notifTitle">Title template</label>
        <textarea id="notifTitle" rows="2" maxlength="500"></textarea>
      </div>
      <div class="control-group">
        <label for="notifMessage">Message template</label>
        <textarea id="notifMessage" rows="3" maxlength="2000"></textarea>
      </div>
      <div class="control-group">
        <label for="notifSound">Sound</label>
        <input id="notifSound" type="text" maxlength="120" placeholder="default">
      </div>
      <div class="control-group">
        <label for="notifImage">Image URL/path</label>
        <input id="notifImage" type="text" maxlength="500" placeholder="/media/local/...">
      </div>
      <div class="control-group">
        <label for="notifExtraData">Extra data (JSON)</label>
        <textarea id="notifExtraData" rows="3" placeholder='{"tag":"rota"}'></textarea>
      </div>
      <div class="actions">
        <button class="btn btn-primary" id="notifSave" type="button">Save settings</button>
        <button class="btn btn-secondary" id="notifPreview" type="button">Refresh preview</button>
        <button class="btn btn-secondary" id="notifTest" type="button">Send test</button>
      </div>
      <p class="ha-notification-status" id="notifStatus"></p>
      <div class="ha-preview" id="notifPreviewOutput" aria-live="polite"></div>
      <div class="control-group">
        <label>Dispatch debug log</label>
        <button class="btn btn-secondary" id="notifRefreshDebug" type="button">Refresh debug log</button>
        <div class="ha-debug-log" id="notifDebugLog" aria-live="polite"></div>
      </div>
    `;
    panel.appendChild(wrapper);

    const weekdays = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
    const weekdaysRoot = q("#notifWeekdays", wrapper);
    weekdaysRoot.innerHTML = weekdays
      .map((day) => `<label><input type="checkbox" data-day="${day}"> ${day.toUpperCase()}</label>`)
      .join("");
  }

  function setNotificationStatus(message, isError = false) {
    const status = q("#notifStatus");
    if (!status) return;
    status.textContent = message || "";
    status.style.color = isError ? "#a11" : "var(--muted)";
  }

  function getPairingRows() {
    return qa(".pairing-row", q("#notifPairingRows"));
  }

  function addPairingRow(subject = "", service = "", critical = false, subjects = [], services = []) {
    const root = q("#notifPairingRows");
    if (!root) return;
    const row = document.createElement("div");
    row.className = "pairing-row";
    row.innerHTML = `
      <select class="pair-subject"></select>
      <select class="pair-service"></select>
      <label class="pair-critical"><input type="checkbox" class="pair-critical-input"> Critical</label>
      <button class="btn btn-secondary pair-remove" type="button">✕</button>
    `;
    root.appendChild(row);

    const subjectSelect = q(".pair-subject", row);
    const serviceSelect = q(".pair-service", row);
    const criticalInput = q(".pair-critical-input", row);

    subjectSelect.innerHTML = subjects.length
      ? subjects.map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`).join("")
      : '<option value="">No subjects available</option>';
    serviceSelect.innerHTML = services.length
      ? services.map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`).join("")
      : '<option value="">No notify services available</option>';

    if (subject) subjectSelect.value = subject;
    if (service) serviceSelect.value = service;
    if (criticalInput) criticalInput.checked = Boolean(critical);
    q(".pair-remove", row).addEventListener("click", () => {
      row.remove();
      renderSubjectPills();
    });
    subjectSelect.addEventListener("change", renderSubjectPills);
    serviceSelect.addEventListener("change", renderSubjectPills);
    criticalInput?.addEventListener("change", renderSubjectPills);
  }

  function collectPairingMapsFromForm() {
    const subjectServiceMap = {};
    const subjectCriticalMap = {};
    getPairingRows().forEach((row) => {
      const subject = q(".pair-subject", row)?.value?.trim() || "";
      const service = q(".pair-service", row)?.value?.trim() || "";
      const critical = Boolean(q(".pair-critical-input", row)?.checked);
      if (subject && service) {
        subjectServiceMap[subject] = service;
        subjectCriticalMap[subject] = critical;
      }
    });
    return { subjectServiceMap, subjectCriticalMap };
  }

  function renderSubjectPills(selected = null) {
    const pillsRoot = q("#notifSubjectPills");
    if (!pillsRoot) return;
    const map = collectPairingMapsFromForm().subjectServiceMap;
    const subjects = Object.keys(map);
    const keep = new Set(Array.isArray(selected) ? selected : qa(".subject-pill.active", pillsRoot).map((b) => b.dataset.subject));
    pillsRoot.innerHTML = "";
    subjects.forEach((subject) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "subject-pill";
      btn.dataset.subject = subject;
      btn.textContent = subject;
      if (keep.has(subject)) btn.classList.add("active");
      btn.addEventListener("click", () => btn.classList.toggle("active"));
      pillsRoot.appendChild(btn);
    });
  }

  function getNotificationPayloadFromForm() {
    const extraDataRaw = q("#notifExtraData")?.value?.trim() || "{}";
    let extraData = {};
    if (extraDataRaw) {
      extraData = JSON.parse(extraDataRaw);
      if (!extraData || typeof extraData !== "object" || Array.isArray(extraData)) {
        throw new Error("Extra data must be a JSON object");
      }
    }

    const weekdays = qa("#notifWeekdays input[type='checkbox']:checked").map((el) =>
      (el.dataset.day || "").trim().toLowerCase()
    );

    const pairingMaps = collectPairingMapsFromForm();
    const subjectServiceMap = pairingMaps.subjectServiceMap;
    const subjectNames = qa("#notifSubjectPills .subject-pill.active").map((el) => el.dataset.subject).filter((name) => Boolean(subjectServiceMap[name]));

    return {
      enabled: Boolean(q("#notifEnabled")?.checked),
      subject_names: subjectNames,
      subject_service_map: subjectServiceMap,
      subject_critical_map: pairingMaps.subjectCriticalMap,
      weekdays,
      title_template: q("#notifTitle")?.value || "",
      message_template: q("#notifMessage")?.value || "",
      sound: q("#notifSound")?.value?.trim() || "",
      image_url: q("#notifImage")?.value?.trim() || "",
      extra_data: extraData,
    };
  }

  function applyNotificationSettingsToForm(settings, subjects, services) {
    q("#notifEnabled").checked = Boolean(settings.enabled);
    q("#notifTitle").value = settings.title_template || "";
    q("#notifMessage").value = (settings.message_template || "").replace(/Whole Shift:\s*/g, "");
    q("#notifSound").value = settings.sound || "";
    q("#notifImage").value = settings.image_url || "";
    q("#notifExtraData").value = JSON.stringify(settings.extra_data || {}, null, 2);

    const activeDays = new Set(Array.isArray(settings.weekdays) ? settings.weekdays : []);
    qa("#notifWeekdays input[type='checkbox']").forEach((checkbox) => {
      checkbox.checked = activeDays.has((checkbox.dataset.day || "").toLowerCase());
    });

    const pairings = settings.subject_service_map || {};
    const criticalMap = settings.subject_critical_map || {};
    const root = q("#notifPairingRows");
    if (root) root.innerHTML = "";
    const pairs = Object.entries(pairings);
    if (pairs.length) {
      pairs.forEach(([subject, service]) => addPairingRow(subject, service, Boolean(criticalMap[subject]), subjects, services));
    } else {
      addPairingRow(subjects[0] || "", services[0] || "", false, subjects, services);
    }
    renderSubjectPills(settings.subject_names || []);
  }

  async function refreshNotificationPreview() {
    const previewRoot = q("#notifPreviewOutput");
    if (!previewRoot) return;

    const resp = await fetch(apiUrl("/api/notification_preview"), { cache: "no-store" });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || `Preview failed: ${resp.status}`);
    }

    const payload = await resp.json();
    previewRoot.textContent = JSON.stringify(payload, null, 2);
  }

  async function refreshNotificationDebugLog() {
    const root = q("#notifDebugLog");
    if (!root) return;

    const resp = await fetch(apiUrl("/api/notification_debug_log"), { cache: "no-store" });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || `Debug log failed: ${resp.status}`);
    }

    const payload = await resp.json();
    const events = Array.isArray(payload.events) ? payload.events : [];
    if (!events.length) {
      root.textContent = "No debug events yet.";
      return;
    }

    root.innerHTML = events
      .map((item) => `<div class="ha-debug-log-item"><strong>${escapeHtml(item.event_type || "event")}</strong> · ${escapeHtml(item.subject_name || "")} · ${escapeHtml(item.notify_service || "")}<br><small>${escapeHtml(item.created_at || "")} (trigger ${escapeHtml(item.trigger_at || "n/a")})</small></div>`)
      .join("");
  }

  async function initNotificationPanel() {
    injectNotificationPanel();
    if (!q("#haNotificationPanel")) return;

    const [settingsResp, servicesResp, subjectsResp] = await Promise.all([
      fetch(apiUrl("/api/notification_settings"), { cache: "no-store" }),
      fetch(apiUrl("/api/ha_notify_services"), { cache: "no-store" }),
      fetch(apiUrl("/api/notification_subjects"), { cache: "no-store" }),
    ]);
    if (!settingsResp.ok) throw new Error(`Failed to load notification settings: ${settingsResp.status}`);

    const settings = await settingsResp.json();
    const servicesPayload = servicesResp.ok ? await servicesResp.json() : { services: [] };
    const subjectsPayload = subjectsResp.ok ? await subjectsResp.json() : { subjects: [] };
    const services = Array.isArray(servicesPayload.services) ? servicesPayload.services : [];
    const subjects = Array.isArray(subjectsPayload.subjects) ? subjectsPayload.subjects : [];

    applyNotificationSettingsToForm(settings, subjects, services);
    await refreshNotificationPreview();
    await refreshNotificationDebugLog();

    q("#notifAddPairing")?.addEventListener("click", () => {
      addPairingRow(subjects[0] || "", services[0] || "", false, subjects, services);
      renderSubjectPills();
    });

    q("#notifPreview")?.addEventListener("click", async () => {
      try {
        await refreshNotificationPreview();
        setNotificationStatus("Preview updated.");
      } catch (err) {
        setNotificationStatus(err.message || "Preview failed", true);
      }
    });

    q("#notifRefreshDebug")?.addEventListener("click", async () => {
      try {
        await refreshNotificationDebugLog();
        setNotificationStatus("Debug log updated.");
      } catch (err) {
        setNotificationStatus(err.message || "Debug log failed", true);
      }
    });

    q("#notifSave")?.addEventListener("click", async () => {
      try {
        const payload = getNotificationPayloadFromForm();
        const resp = await fetch(apiUrl("/api/notification_settings"), {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(text || `Save failed: ${resp.status}`);
        }
        await refreshNotificationPreview();
        await refreshNotificationDebugLog();
        setNotificationStatus("Notification settings saved.");
      } catch (err) {
        setNotificationStatus(err.message || "Save failed", true);
      }
    });

    q("#notifTest")?.addEventListener("click", async () => {
      try {
        const resp = await fetch(apiUrl("/api/test_notification"), { method: "POST" });
        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(text || `Test failed: ${resp.status}`);
        }
        const payload = await resp.json();
        await refreshNotificationDebugLog();
        setNotificationStatus(`Test sent (${payload.count || 0}).`);
      } catch (err) {
        setNotificationStatus(err.message || "Test failed", true);
      }
    });
  }

  function wireDeleteButtons() {
    document.addEventListener("click", async (event) => {
      const target = event.target instanceof Element ? event.target : null;
      const deleteButton = target ? target.closest(".settings-week-delete") : null;
      if (!deleteButton) return;

      const item = deleteButton.closest(".settings-week-item");
      const checkbox = item ? item.querySelector('input[name="settingsAllowedWeek"]') : null;
      const viewerId = checkbox ? checkbox.value : "";
      if (!viewerId || !/^upload-\d+$/.test(viewerId)) return;

      event.preventDefault();
      event.stopImmediatePropagation();

      const originalLabel = deleteButton.textContent;
      deleteButton.disabled = true;
      deleteButton.textContent = "Deleting...";

      try {
        await deleteUploadByViewerId(viewerId);
        await syncViewerState();
        window.location.reload();
      } catch (err) {
        console.error(err);
        alert(`Delete failed: ${err.message}`);
        deleteButton.disabled = false;
        deleteButton.textContent = originalLabel;
      }
    }, true);
  }

  async function initialSyncAndMaybeReload() {
    const alreadyReloaded = sessionStorage.getItem(RELOAD_FLAG) === "1";

    const changed = await syncViewerState();

    if (changed && !alreadyReloaded) {
      sessionStorage.setItem(RELOAD_FLAG, "1");
      window.location.reload();
      return true;
    }

    sessionStorage.removeItem(RELOAD_FLAG);
    return false;
  }

  async function init() {
    const reloaded = await initialSyncAndMaybeReload();
    if (reloaded) return;

    hideLegacyPdfSection();
    patchCopy();
    replaceUploadButton();
    wireDeleteButtons();
    try {
      await initNotificationPanel();
    } catch (err) {
      console.error("Failed to initialize notification panel", err);
      setNotificationStatus("Unable to load notification settings from backend.", true);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
