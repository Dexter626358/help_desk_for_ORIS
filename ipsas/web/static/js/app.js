(function () {
  "use strict";

  var STORAGE_COLLAPSED = "ipsas.sidebarCollapsed";
  var STORAGE_SECTIONS = "ipsas.sidebarSections";

  function qs(sel, root) {
    return (root || document).querySelector(sel);
  }

  function qsa(sel, root) {
    return Array.prototype.slice.call((root || document).querySelectorAll(sel));
  }

  function initSidebar() {
    var shell = qs("[data-app-shell]");
    if (!shell) return;

    var toggle = qs("[data-sidebar-toggle]");
    var mobileBtn = qs("[data-mobile-nav]");
    var backdrop = qs("[data-nav-backdrop]");

    try {
      if (localStorage.getItem(STORAGE_COLLAPSED) === "1") {
        shell.classList.add("is-collapsed");
      }
    } catch (e) {}

    if (toggle) {
      function syncToggleUi() {
        var collapsed = shell.classList.contains("is-collapsed");
        toggle.setAttribute("aria-label", collapsed ? "Развернуть меню" : "Свернуть меню");
        toggle.setAttribute("data-tooltip", collapsed ? "Развернуть меню" : "Свернуть меню");
        toggle.setAttribute("title", collapsed ? "Развернуть меню" : "Свернуть меню");
      }
      syncToggleUi();
      toggle.addEventListener("click", function () {
        shell.classList.toggle("is-collapsed");
        try {
          localStorage.setItem(
            STORAGE_COLLAPSED,
            shell.classList.contains("is-collapsed") ? "1" : "0"
          );
        } catch (e) {}
        syncToggleUi();
      });
    }

    function closeMobile() {
      shell.classList.remove("is-mobile-open");
      if (backdrop) backdrop.classList.remove("is-visible");
    }

    function openMobile() {
      shell.classList.add("is-mobile-open");
      if (backdrop) backdrop.classList.add("is-visible");
    }

    if (mobileBtn) {
      mobileBtn.addEventListener("click", function () {
        if (shell.classList.contains("is-mobile-open")) closeMobile();
        else openMobile();
      });
    }
    if (backdrop) backdrop.addEventListener("click", closeMobile);

    var saved = {};
    try {
      saved = JSON.parse(localStorage.getItem(STORAGE_SECTIONS) || "{}") || {};
    } catch (e) {
      saved = {};
    }

    qsa("[data-nav-section]").forEach(function (section) {
      var key = section.getAttribute("data-nav-section");
      var btn = qs("[data-nav-section-toggle]", section);
      if (!btn) return;

      if (Object.prototype.hasOwnProperty.call(saved, key)) {
        section.classList.toggle("is-open", !!saved[key]);
      } else if (section.querySelector(".nav-link.is-active, .nav-link[aria-current='page']")) {
        section.classList.add("is-open");
      }

      btn.setAttribute("aria-expanded", section.classList.contains("is-open") ? "true" : "false");

      btn.addEventListener("click", function () {
        section.classList.toggle("is-open");
        var open = section.classList.contains("is-open");
        btn.setAttribute("aria-expanded", open ? "true" : "false");
        try {
          saved[key] = open;
          localStorage.setItem(STORAGE_SECTIONS, JSON.stringify(saved));
        } catch (e) {}
      });
    });
  }

  function formatBytes(bytes) {
    if (!bytes && bytes !== 0) return "";
    if (bytes < 1024) return bytes + " Б";
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " КБ";
    return (bytes / (1024 * 1024)).toFixed(1) + " МБ";
  }

  function acceptMatches(fileName, accept) {
    if (!accept) return true;
    var lower = (fileName || "").toLowerCase();
    return accept.split(",").some(function (part) {
      var token = part.trim().toLowerCase();
      if (!token) return true;
      if (token.startsWith(".")) return lower.endsWith(token);
      return lower.indexOf(token) !== -1;
    });
  }

  function initFileUploads() {
    qsa("[data-file-upload]").forEach(function (wrap) {
      var input = qs("[data-file-input]", wrap);
      var dropzone = qs(".dropzone", wrap);
      var card = qs("[data-file-card]", wrap);
      var nameEl = qs("[data-file-name]", wrap);
      var metaEl = qs("[data-file-meta]", wrap);
      var statusEl = qs("[data-file-status]", wrap);
      var errorEl = qs("[data-file-error]", wrap);
      var clearBtn = qs("[data-file-clear]", wrap);
      var form = wrap.closest("form");
      var submitBtn = form ? qs("[data-submit-btn]", form) : null;
      var hint = form ? qs("[data-submit-hint]", form) : null;
      var accept = wrap.getAttribute("data-accept") || "";
      var maxBytes = parseInt(wrap.getAttribute("data-max-bytes") || "0", 10);

      function setSubmitEnabled(ok) {
        if (!submitBtn) return;
        submitBtn.disabled = !ok;
        submitBtn.classList.toggle("is-disabled", !ok);
        if (hint) hint.hidden = ok;
      }

      function showError(msg) {
        if (errorEl) {
          errorEl.hidden = !msg;
          errorEl.textContent = msg || "";
        }
        if (statusEl) statusEl.textContent = msg ? "Ошибка" : "Готов к обработке";
      }

      function renderFile(file) {
        if (!file) {
          if (card) card.hidden = true;
          showError("");
          setSubmitEnabled(false);
          return;
        }
        if (card) card.hidden = false;
        if (nameEl) nameEl.textContent = file.name;
        if (metaEl) {
          var ext = (file.name.split(".").pop() || "").toUpperCase();
          metaEl.textContent = ext + " · " + formatBytes(file.size);
        }

        if (!acceptMatches(file.name, accept)) {
          showError("Выбран файл «" + file.name + "». Для этого инструмента требуется: " + accept);
          setSubmitEnabled(false);
          return;
        }
        if (maxBytes && file.size > maxBytes) {
          showError(
            "Размер файла составляет " +
              formatBytes(file.size) +
              ". Максимальный допустимый размер — " +
              formatBytes(maxBytes) +
              "."
          );
          setSubmitEnabled(false);
          return;
        }
        showError("");
        setSubmitEnabled(true);
      }

      function pickFiles(files) {
        var file = files && files[0] ? files[0] : null;
        if (!file) {
          input.value = "";
          renderFile(null);
          return;
        }
        try {
          var dt = new DataTransfer();
          dt.items.add(file);
          input.files = dt.files;
        } catch (e) {
          /* Safari older: rely on native input */
        }
        renderFile(file);
      }

      if (dropzone) {
        dropzone.addEventListener("click", function () {
          input.click();
        });
        dropzone.addEventListener("keydown", function (e) {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            input.click();
          }
        });
        ["dragenter", "dragover"].forEach(function (evt) {
          dropzone.addEventListener(evt, function (e) {
            e.preventDefault();
            dropzone.classList.add("is-dragover");
          });
        });
        ["dragleave", "drop"].forEach(function (evt) {
          dropzone.addEventListener(evt, function (e) {
            e.preventDefault();
            dropzone.classList.remove("is-dragover");
          });
        });
        dropzone.addEventListener("drop", function (e) {
          pickFiles(e.dataTransfer && e.dataTransfer.files);
        });
      }

      if (input) {
        input.addEventListener("change", function () {
          pickFiles(input.files);
        });
        if (input.files && input.files[0]) renderFile(input.files[0]);
        else setSubmitEnabled(false);
      }

      if (clearBtn) {
        clearBtn.addEventListener("click", function () {
          input.value = "";
          renderFile(null);
        });
      }
    });
  }

  function initSubmitLoading() {
    qsa("form[data-loading-form]").forEach(function (form) {
      form.addEventListener("submit", function () {
        var btn = qs("[data-submit-btn]", form);
        if (!btn || btn.disabled) return;
        btn.classList.add("is-loading");
        btn.disabled = true;
        var loadingText = btn.getAttribute("data-loading-text");
        if (loadingText) btn.setAttribute("aria-label", loadingText);
      });
    });
  }

  function initUrlField() {
    qsa("[data-url-field]").forEach(function (input) {
      input.addEventListener("blur", function () {
        input.value = (input.value || "").trim();
      });
      var form = input.closest("form");
      if (!form) return;
      form.addEventListener("submit", function (e) {
        input.value = (input.value || "").trim();
        try {
          // Allow relative? No — require absolute URL with http(s)
          var u = new URL(input.value);
          if (u.protocol !== "http:" && u.protocol !== "https:") throw new Error("bad protocol");
        } catch (err) {
          e.preventDefault();
          input.focus();
          alert("Укажите корректную ссылку вида https://journals.example/…/issue/view/…");
        }
      });
    });
  }

  function initMatchFilters() {
    var bar = qs("[data-match-filters]");
    var table = qs("[data-match-table]");
    if (!bar || !table) return;

    var search = qs("[data-match-search]", bar);
    var problemsOnly = qs("[data-match-problems-only]", bar);
    var countEl = qs("[data-match-count]", bar);
    var rows = qsa("[data-match-row]", table);

    function apply() {
      var q = ((search && search.value) || "").trim().toLowerCase();
      var onlyProblems = !!(problemsOnly && problemsOnly.checked);
      var visible = 0;
      rows.forEach(function (row) {
        var hay = (row.getAttribute("data-search") || "").toLowerCase();
        var isProblem = row.getAttribute("data-problem") === "1";
        var ok = (!q || hay.indexOf(q) !== -1) && (!onlyProblems || isProblem);
        row.hidden = !ok;
        if (ok) visible += 1;
      });
      if (countEl) {
        countEl.textContent = "Показано: " + visible + " из " + rows.length;
      }
    }

    if (search) search.addEventListener("input", apply);
    if (problemsOnly) problemsOnly.addEventListener("change", apply);
    apply();
  }

  document.addEventListener("DOMContentLoaded", function () {
    initSidebar();
    initFileUploads();
    initSubmitLoading();
    initUrlField();
    initMatchFilters();
  });
})();
