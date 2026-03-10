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
      .ha-notification-panel input,.ha-notification-panel textarea,.ha-notification-panel select{
        width:100%;border:1px solid var(--border);border-radius:10px;padding:8px 10px;background:#fff;color:var(--ink);font:inherit
      }
      .ha-notification-panel .row-inline{display:grid;grid-template-columns:1fr auto;gap:8px;align-items:end}
      .ha-notification-panel .days-grid{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:4px}
      .ha-notification-panel .days-grid label{display:flex;gap:4px;align-items:center;justify-content:center;font-weight:500;border:1px solid var(--border);border-radius:8px;padding:6px 4px;background:#fff}
      .ha-notification-panel .actions{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
      .ha-notification-panel .actions .btn-primary{grid-column:span 2}
      .ha-notification-status{font-size:12px;color:var(--muted);margin:6px 0 0}
      .ha-preview{background:#fff;border:1px solid var(--border);border-radius:10px;padding:10px;font-size:12px;white-space:pre-wrap;word-break:break-word;max-height:180px;overflow:auto}
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
      <p class="control-hint">Configure Home Assistant notification templates from this add-on UI.</p>
      <div class="control-group">
        <label><input id="notifEnabled" type="checkbox"> Enabled</label>
      </div>
      <div class="control-group">
        <label for="notifSubject">Subject name</label>
        <input id="notifSubject" type="text" maxlength="120" placeholder="Nathan">
      </div>
      <div class="control-group row-inline">
        <div>
          <label for="notifService">Notify service</label>
          <select id="notifService"></select>
        </div>
        <button class="btn btn-secondary" id="notifRefreshServices" type="button">Reload</button>
      </div>
      <div class="control-group">
        <label for="notifTime">Notify time</label>
        <input id="notifTime" type="time">
      </div>
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
    `;
    panel.appendChild(wrapper);

    const weekdays = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
    const weekdaysRoot = q("#notifWeekdays", wrapper);
    weekdaysRoot.innerHTML = weekdays
      .map(
        (day) =>
          `<label><input type="checkbox" data-day="${day}"> ${day.toUpperCase()}</label>`
      )
      .join("");
  }

  function setNotificationStatus(message, isError = false) {
    const status = q("#notifStatus");
    if (!status) return;
    status.textContent = message || "";
    status.style.color = isError ? "#a11" : "var(--muted)";
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

    return {
      enabled: Boolean(q("#notifEnabled")?.checked),
      subject_name: q("#notifSubject")?.value?.trim() || "",
      notify_service: q("#notifService")?.value?.trim() || "",
      notify_time: q("#notifTime")?.value || "07:00",
      weekdays,
      title_template: q("#notifTitle")?.value || "",
      message_template: q("#notifMessage")?.value || "",
      sound: q("#notifSound")?.value?.trim() || "",
      image_url: q("#notifImage")?.value?.trim() || "",
      extra_data: extraData,
    };
  }

  function applyNotificationSettingsToForm(settings) {
    q("#notifEnabled").checked = Boolean(settings.enabled);
    q("#notifSubject").value = settings.subject_name || "";
    q("#notifTime").value = settings.notify_time || "07:00";
    q("#notifTitle").value = settings.title_template || "";
    q("#notifMessage").value = settings.message_template || "";
    q("#notifSound").value = settings.sound || "";
    q("#notifImage").value = settings.image_url || "";
    q("#notifExtraData").value = JSON.stringify(settings.extra_data || {}, null, 2);

    const activeDays = new Set(Array.isArray(settings.weekdays) ? settings.weekdays : []);
    qa("#notifWeekdays input[type='checkbox']").forEach((checkbox) => {
      checkbox.checked = activeDays.has((checkbox.dataset.day || "").toLowerCase());
    });
  }

  async function loadNotifyServices(selected) {
    const select = q("#notifService");
    if (!select) return;

    let services = [];
    try {
      const resp = await fetch(apiUrl("/api/ha_notify_services"), { cache: "no-store" });
      if (resp.ok) {
        const payload = await resp.json();
        services = Array.isArray(payload.services) ? payload.services : [];
      }
    } catch (err) {
      console.warn("Failed to load HA notify services", err);
    }

    if (selected && !services.includes(selected)) {
      services = [selected, ...services];
    }

    if (!services.length) {
      services = [selected || "notify.mobile_app_iphone_15_pro"];
    }

    select.innerHTML = services
      .map((service) => `<option value="${escapeHtml(service)}">${escapeHtml(service)}</option>`)
      .join("");

    if (selected) {
      select.value = selected;
    }
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

  async function initNotificationPanel() {
    injectNotificationPanel();
    if (!q("#haNotificationPanel")) return;

    const settingsResp = await fetch(apiUrl("/api/notification_settings"), { cache: "no-store" });
    if (!settingsResp.ok) {
      throw new Error(`Failed to load notification settings: ${settingsResp.status}`);
    }
    const settings = await settingsResp.json();
    applyNotificationSettingsToForm(settings);
    await loadNotifyServices(settings.notify_service || "");
    await refreshNotificationPreview();

    q("#notifRefreshServices")?.addEventListener("click", async () => {
      const selected = q("#notifService")?.value || "";
      await loadNotifyServices(selected);
      setNotificationStatus("Notify services refreshed.");
    });

    q("#notifPreview")?.addEventListener("click", async () => {
      try {
        await refreshNotificationPreview();
        setNotificationStatus("Preview updated.");
      } catch (err) {
        setNotificationStatus(err.message || "Preview failed", true);
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
        setNotificationStatus("Notification settings saved.");
      } catch (err) {
        setNotificationStatus(err.message || "Save failed", true);
      }
    });

    q("#notifTest")?.addEventListener("click", async () => {
      try {
        const resp = await fetch(apiUrl("/api/test_notification"), {
          method: "POST",
        });
        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(text || `Test failed: ${resp.status}`);
        }
        const payload = await resp.json();
        setNotificationStatus(`Test sent via ${payload.sent_via || "notify"}.`);
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
