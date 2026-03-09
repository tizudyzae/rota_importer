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
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();