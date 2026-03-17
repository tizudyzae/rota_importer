(function () {
  const APP_BASE = window.__APP_BASE__ || "";
  const STORAGE_KEY = "loggannt.rotas.v1";
  const STORAGE_SELECTION_KEY = "loggannt.rotas.selection";
  const RELOAD_FLAG = "loggannt.ha.synced.once";

  function debugLog(message, data) {
    if (data === undefined) {
      console.log(`[PeopleSettings] ${message}`);
      return;
    }
    console.log(`[PeopleSettings] ${message}`, data);
  }

  debugLog("ha-bridge script loaded");

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
      #appearanceControls{display:none !important}
      .ha-notification-panel .control-group{display:grid;gap:6px;margin-bottom:10px}
      .ha-notification-panel label{font-size:12px;font-weight:600;color:var(--ink)}
      .ha-notification-panel input,.ha-notification-panel textarea,.ha-notification-panel select{width:100%;border:1px solid var(--border);border-radius:10px;padding:8px 10px;background:#fff;color:var(--ink);font:inherit}
      .ha-notification-panel .actions{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
      .ha-notification-panel .actions .btn-primary{grid-column:span 2}
      .ha-notification-status{font-size:12px;color:var(--muted);margin:6px 0 0}
      .ha-preview{background:#fff;border:1px solid var(--border);border-radius:10px;padding:10px;font-size:12px;white-space:pre-wrap;word-break:break-word;max-height:180px;overflow:auto}
      .person-settings-list{display:grid;gap:8px}
      .person-settings-row{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:8px;padding:8px;border:1px solid var(--border);border-radius:10px;background:#fff}
      .person-settings-row .swatch{width:16px;height:16px;border-radius:50%;border:1px solid var(--border)}
      .person-settings-row .meta{font-size:11px;color:var(--muted)}
      .person-settings-row .btn{padding:6px 10px;font-size:11px}
      .person-modal[hidden]{display:none}
      .person-modal{position:fixed;inset:0;background:rgba(0,0,0,.45);display:flex;align-items:center;justify-content:center;z-index:9999;padding:12px}
      .person-modal-card{width:min(460px,100%);background:#fff;border-radius:12px;border:1px solid var(--border);padding:12px;display:grid;gap:8px}
      .person-modal-actions{display:flex;justify-content:flex-end;gap:8px}
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
      <p class="control-hint">Notifications trigger 2 hours before each selected subject's shift start time. Optional handover notices can also trigger 2 hours before shift end.</p>
      <div class="control-group">
        <label><input id="notifEnabled" type="checkbox"> Enabled</label>
        <label><input id="notifBeforeEndEnabled" type="checkbox"> Add handover alert (2 hours before shift end)</label>
      </div>
      <div class="control-group">
        <label>People settings</label>
        <div id="notifPersonSettings" class="person-settings-list"></div>
      </div>
      <div id="notifPersonModal" class="person-modal" hidden>
        <div class="person-modal-card">
          <h4 id="notifPersonModalTitle">Edit person</h4>
          <div class="control-group">
            <label for="notifPersonAlias">Alias</label>
            <input id="notifPersonAlias" type="text" maxlength="40">
          </div>
          <div class="control-group">
            <label for="notifPersonColor">Line chart colour</label>
            <input id="notifPersonColor" type="color">
          </div>
          <div class="control-group">
            <label for="notifPersonService">Notify service</label>
            <select id="notifPersonService"></select>
            <label><input id="notifPersonEnabled" type="checkbox"> Include in notifications</label>
            <label><input id="notifPersonCritical" type="checkbox"> Critical sound</label>
          </div>
          <div class="person-modal-actions">
            <button class="btn btn-secondary" id="notifPersonCancel" type="button">Cancel</button>
            <button class="btn btn-primary" id="notifPersonSave" type="button">Save</button>
          </div>
        </div>
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

  function hideLegacyAppearanceControls() {
    const appearance = q("#appearanceControls");
    if (!appearance) return;
    appearance.hidden = true;
    appearance.setAttribute("aria-hidden", "true");
    appearance.innerHTML = "";
  }

  function setNotificationStatus(message, isError = false) {
    const status = q("#notifStatus");
    if (!status) return;
    status.textContent = message || "";
    status.style.color = isError ? "#a11" : "var(--muted)";
  }

  const notificationPersonConfig = new Map();
  let notificationSubjects = [];
  let notificationServices = [];
  let activePersonKey = "";

  function personConfigFor(subject) {
    const existing = notificationPersonConfig.get(subject);
    if (existing) return existing;
    const created = { service: "", critical: false, enabled: false };
    notificationPersonConfig.set(subject, created);
    return created;
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

    const { subjectServiceMap, subjectCriticalMap, subjectNames } = buildNotificationPersonMaps();

    return {
      enabled: Boolean(q("#notifEnabled")?.checked),
      notify_before_end_enabled: Boolean(q("#notifBeforeEndEnabled")?.checked),
      subject_names: subjectNames,
      subject_service_map: subjectServiceMap,
      subject_critical_map: subjectCriticalMap,
      weekdays,
      title_template: q("#notifTitle")?.value || "",
      message_template: q("#notifMessage")?.value || "",
      sound: q("#notifSound")?.value?.trim() || "",
      image_url: q("#notifImage")?.value?.trim() || "",
      extra_data: extraData,
    };
  }

  function buildNotificationPersonMaps() {
    const subjectServiceMap = {};
    const subjectCriticalMap = {};
    const subjectNames = [];
    notificationSubjects.forEach((subject) => {
      const cfg = personConfigFor(subject);
      if (!cfg.service) return;
      subjectServiceMap[subject] = cfg.service;
      subjectCriticalMap[subject] = Boolean(cfg.critical);
      if (cfg.enabled) subjectNames.push(subject);
    });
    return { subjectServiceMap, subjectCriticalMap, subjectNames };
  }

  function renderPersonSettingsList() {
    const root = q("#notifPersonSettings");
    if (!root) return;
    root.innerHTML = "";
    notificationSubjects.forEach((subject) => {
      const cfg = personConfigFor(subject);
      const alias = getAliasPreferenceByKey(`raw:${(subject || "").trim().toLowerCase()}`) || "";
      const color = getColorPreferenceByKey(`raw:${(subject || "").trim().toLowerCase()}`) || "#4b4b4b";
      const row = document.createElement("div");
      row.className = "person-settings-row";
      row.innerHTML = `
        <span class="swatch" style="background:${escapeHtml(color)}"></span>
        <div>
          <div><strong>${escapeHtml(alias || subject)}</strong>${alias ? ` <small>(${escapeHtml(subject)})</small>` : ""}</div>
          <div class="meta">${cfg.enabled ? "Enabled" : "Disabled"} · ${escapeHtml(cfg.service || "No notify service")}</div>
        </div>
        <button class="btn btn-secondary" type="button">Edit</button>
      `;
      q("button", row)?.addEventListener("click", () => openPersonModal(subject));
      root.appendChild(row);
    });
  }

  function openPersonModal(subject) {
    activePersonKey = subject;
    debugLog("Opening person modal", { subject });
    const modal = q("#notifPersonModal");
    if (!modal) return;
    const cfg = personConfigFor(subject);
    const key = `raw:${(subject || "").trim().toLowerCase()}`;
    q("#notifPersonModalTitle").textContent = `Edit ${subject}`;
    q("#notifPersonAlias").value = getAliasPreferenceByKey(key) || "";
    q("#notifPersonColor").value = getColorPreferenceByKey(key) || "#4b4b4b";
    const serviceSelect = q("#notifPersonService");
    serviceSelect.innerHTML = notificationServices.length
      ? notificationServices.map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`).join("")
      : '<option value="">No notify services available</option>';
    serviceSelect.value = cfg.service || "";
    q("#notifPersonEnabled").checked = Boolean(cfg.enabled);
    q("#notifPersonCritical").checked = Boolean(cfg.critical);
    modal.hidden = false;
  }

  function closePersonModal() {
    const modal = q("#notifPersonModal");
    if (modal) modal.hidden = true;
    activePersonKey = "";
  }

  function canUseViewerAppearanceFns() {
    return (
      typeof getAliasPreferenceByKey === "function" &&
      typeof getColorPreferenceByKey === "function" &&
      typeof handleAliasPreferenceChange === "function" &&
      typeof handleColorPreferenceChange === "function"
    );
  }

  async function saveAliasAndColorPreference(name, alias, color) {
    const key = `raw:${(name || "").trim().toLowerCase()}`;
    if (!key || key === "raw:") return;

    console.log("[PeopleSettings] Saving alias/color", {
      name,
      key,
      alias,
      color,
      usingViewerFns: canUseViewerAppearanceFns(),
    });

    if (canUseViewerAppearanceFns()) {
      handleAliasPreferenceChange(key, alias || "");
      handleColorPreferenceChange(key, color || "#4b4b4b");
      return;
    }

    const resp = await fetch(apiUrl("/api/preferences"), { cache: "no-store" });
    console.log("[PeopleSettings] Loaded existing appearance preferences", { status: resp.status, ok: resp.ok });
    if (!resp.ok) {
      throw new Error(`Failed to load appearance preferences: ${resp.status}`);
    }
    const payload = await resp.json();
    const colors = payload && payload.colors && typeof payload.colors === "object" ? { ...payload.colors } : {};
    const aliases = payload && payload.aliases && typeof payload.aliases === "object" ? { ...payload.aliases } : {};

    const safeAlias = (alias || "").trim();
    if (safeAlias) {
      aliases[key] = safeAlias;
    } else {
      delete aliases[key];
    }

    const safeColor = (color || "").trim();
    if (/^#[0-9a-fA-F]{6}$/.test(safeColor)) {
      colors[key] = safeColor.toLowerCase();
    } else {
      delete colors[key];
    }

    const saveResp = await fetch(apiUrl("/api/preferences"), {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ colors, aliases }),
    });
    console.log("[PeopleSettings] Saved alias/color preferences", { status: saveResp.status, ok: saveResp.ok });
    if (!saveResp.ok) {
      throw new Error(`Failed to save appearance preferences: ${saveResp.status}`);
    }
  }

  async function savePersonModal() {
    if (!activePersonKey) {
      console.error("[PeopleSettings] Save attempted without an active person");
      return;
    }
    try {
      console.log("[PeopleSettings] Save button clicked for person", { activePersonKey });
      await saveAliasAndColorPreference(
        activePersonKey,
        q("#notifPersonAlias")?.value || "",
        q("#notifPersonColor")?.value || "#4b4b4b"
      );
      const cfg = personConfigFor(activePersonKey);
      cfg.service = q("#notifPersonService")?.value?.trim() || "";
      cfg.enabled = Boolean(q("#notifPersonEnabled")?.checked) && Boolean(cfg.service);
      cfg.critical = Boolean(q("#notifPersonCritical")?.checked);
      console.log("[PeopleSettings] Persisting person config", {
        activePersonKey,
        service: cfg.service,
        enabled: cfg.enabled,
        critical: cfg.critical,
      });
      notificationPersonConfig.set(activePersonKey, cfg);
      await persistNotificationPersonSettings();
      renderPersonSettingsList();
      closePersonModal();
      setNotificationStatus("Person settings saved.");
      console.log("[PeopleSettings] Person settings saved successfully", { activePersonKey });
    } catch (err) {
      console.error("[PeopleSettings] Failed to save person settings", err);
      setNotificationStatus(err.message || "Failed to save person settings", true);
    }
  }

  async function persistNotificationPersonSettings() {
    const existingResp = await fetch(apiUrl("/api/notification_settings"), { cache: "no-store" });
    console.log("[PeopleSettings] Loaded existing notification settings", { status: existingResp.status, ok: existingResp.ok });
    if (!existingResp.ok) {
      throw new Error(`Failed to load current settings: ${existingResp.status}`);
    }
    const existing = await existingResp.json();
    const { subjectServiceMap, subjectCriticalMap, subjectNames } = buildNotificationPersonMaps();
    const payload = {
      enabled: Boolean(existing.enabled),
      notify_before_end_enabled: Boolean(existing.notify_before_end_enabled),
      subject_names: subjectNames,
      subject_service_map: subjectServiceMap,
      subject_critical_map: subjectCriticalMap,
      weekdays: Array.isArray(existing.weekdays) ? existing.weekdays : [],
      title_template: existing.title_template || "",
      message_template: existing.message_template || "",
      sound: existing.sound || "",
      image_url: existing.image_url || "",
      extra_data: existing.extra_data && typeof existing.extra_data === "object" && !Array.isArray(existing.extra_data) ? existing.extra_data : {},
    };

    console.log("[PeopleSettings] Saving merged notification settings payload", payload);

    const resp = await fetch(apiUrl("/api/notification_settings"), {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    console.log("[PeopleSettings] Saved notification settings response", { status: resp.status, ok: resp.ok });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || `Save failed: ${resp.status}`);
    }
  }

  async function persistNotificationSettings() {
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
  }

  function applyNotificationSettingsToForm(settings, subjects, services) {
    const pairings = settings.subject_service_map || {};
    const configuredSubjects = Object.keys(pairings);
    notificationSubjects = Array.from(new Set([...(Array.isArray(subjects) ? subjects : []), ...configuredSubjects])).sort((a, b) => a.localeCompare(b));
    notificationServices = Array.isArray(services) ? services : [];
    notificationPersonConfig.clear();

    q("#notifEnabled").checked = Boolean(settings.enabled);
    q("#notifBeforeEndEnabled").checked = Boolean(settings.notify_before_end_enabled);
    q("#notifTitle").value = settings.title_template || "";
    q("#notifMessage").value = (settings.message_template || "").replace(/Whole Shift:\s*/g, "");
    q("#notifSound").value = settings.sound || "";
    q("#notifImage").value = settings.image_url || "";
    q("#notifExtraData").value = JSON.stringify(settings.extra_data || {}, null, 2);

    const activeDays = new Set(Array.isArray(settings.weekdays) ? settings.weekdays : []);
    qa("#notifWeekdays input[type='checkbox']").forEach((checkbox) => {
      checkbox.checked = activeDays.has((checkbox.dataset.day || "").toLowerCase());
    });

    const criticalMap = settings.subject_critical_map || {};
    const selected = new Set(Array.isArray(settings.subject_names) ? settings.subject_names : []);
    notificationSubjects.forEach((subject) => {
      notificationPersonConfig.set(subject, {
        service: pairings[subject] || "",
        critical: Boolean(criticalMap[subject]),
        enabled: selected.has(subject),
      });
    });
    renderPersonSettingsList();
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
    hideLegacyAppearanceControls();
    if (!q("#haNotificationPanel")) {
      console.warn("[PeopleSettings] Notification panel not found after injection");
      return;
    }
    debugLog("Notification panel initialised");

    q("#notifPersonCancel")?.addEventListener("click", closePersonModal);
    document.addEventListener("click", (event) => {
      const target = event.target instanceof Element ? event.target : null;
      const saveButton = target ? target.closest("#notifPersonSave") : null;
      if (!saveButton) return;
      event.preventDefault();
      event.stopPropagation();
      console.log("[PeopleSettings] notifPersonSave delegated click handler fired");
      savePersonModal();
    }, true);
    q("#notifPersonModal")?.addEventListener("click", (event) => {
      if (event.target && event.target.id === "notifPersonModal") closePersonModal();
    });

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
    try {
      await refreshNotificationPreview();
      await refreshNotificationDebugLog();
    } catch (err) {
      setNotificationStatus(err.message || "Failed to refresh notification preview", true);
    }

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
        await persistNotificationSettings();
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
