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
      var fileRequired =
        wrap.getAttribute("data-file-required") === "1" ||
        (input && input.hasAttribute("required"));

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
          // без файла: блокируем submit только если файл обязателен
          setSubmitEnabled(!fileRequired);
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
        if (form.getAttribute("data-submit-blocked") === "1") {
          form.removeAttribute("data-submit-blocked");
          return;
        }
        var btn = qs("[data-submit-btn]", form);
        if (!btn || btn.disabled) return;
        btn.classList.add("is-loading");
        btn.disabled = true;
        var loadingText = btn.getAttribute("data-loading-text");
        if (loadingText) btn.setAttribute("aria-label", loadingText);
      });
    });
  }

  function _restoreSubmitButton(form) {
    var btn = qs("[data-submit-btn]", form);
    if (!btn) return;
    btn.classList.remove("is-loading");
    btn.disabled = false;
  }

  function initUrlField() {
    qsa("[data-url-field]").forEach(function (input) {
      input.addEventListener("blur", function () {
        input.value = (input.value || "").trim();
      });
      var form = input.closest("form");
      if (!form) return;
      form.addEventListener("submit", function (e) {
        // XOR: при выбранном файле ссылка не нужна
        if (form.hasAttribute("data-source-xor")) {
          var fileInput = qs("[data-file-input]", form);
          if (fileInput && fileInput.files && fileInput.files[0]) {
            input.value = "";
            input.disabled = false;
            return;
          }
        }
        input.value = (input.value || "").trim();
        if (!input.value) {
          if (input.hasAttribute("required")) {
            e.preventDefault();
            form.setAttribute("data-submit-blocked", "1");
            _restoreSubmitButton(form);
            input.focus();
            alert("Укажите корректную ссылку вида https://journals.example/…");
          }
          return;
        }
        try {
          var u = new URL(input.value);
          if (u.protocol !== "http:" && u.protocol !== "https:") throw new Error("bad protocol");
        } catch (err) {
          e.preventDefault();
          form.setAttribute("data-submit-blocked", "1");
          _restoreSubmitButton(form);
          input.focus();
          alert("Укажите корректную ссылку вида https://journals.example/…/issue/view/…");
        }
      });
    });
  }

  function initSourceXor() {
    qsa("form[data-source-xor]").forEach(function (form) {
      var urlInput = qs("[data-url-field]", form);
      var urlGroup = qs("[data-xor-url]", form) || (urlInput && urlInput.closest(".form-group"));
      var fileWrap = qs("[data-file-upload]", form);
      var fileInput = fileWrap ? qs("[data-file-input]", fileWrap) : null;
      var dropzone = fileWrap ? qs(".dropzone", fileWrap) : null;
      if (!urlInput || !fileInput) return;

      function hasFile() {
        return !!(fileInput.files && fileInput.files[0]);
      }

      function setDisabled(el, on) {
        if (!el) return;
        el.classList.toggle("is-disabled", on);
        el.style.opacity = on ? "0.55" : "";
        el.style.pointerEvents = on ? "none" : "";
      }

      function sync() {
        var fileOn = hasFile();
        var urlOn = !!(urlInput.value || "").trim();
        if (fileOn) {
          urlInput.value = "";
          urlInput.disabled = true;
          setDisabled(urlGroup, true);
          setDisabled(fileWrap, false);
          fileInput.disabled = false;
        } else if (urlOn) {
          urlInput.disabled = false;
          setDisabled(urlGroup, false);
          // файл не трогаем содержимым — только визуально приглушаем зону
          setDisabled(dropzone, true);
        } else {
          urlInput.disabled = false;
          setDisabled(urlGroup, false);
          setDisabled(dropzone, false);
          setDisabled(fileWrap, false);
          fileInput.disabled = false;
        }
      }

      urlInput.addEventListener("input", sync);
      urlInput.addEventListener("change", sync);
      fileInput.addEventListener("change", sync);
      if (fileWrap) {
        fileWrap.addEventListener("drop", function () {
          setTimeout(sync, 0);
        });
        var clearBtn = qs("[data-file-clear]", fileWrap);
        if (clearBtn) {
          clearBtn.addEventListener("click", function () {
            setTimeout(sync, 0);
          });
        }
      }
      form.addEventListener("submit", function () {
        // disabled fields are not submitted; ensure URL empty when file chosen
        if (hasFile()) {
          urlInput.disabled = false;
          urlInput.value = "";
        }
      });
      sync();
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
    initUrlField();
    initSourceXor();
    // loading — после валидации URL/XOR, чтобы не зависал спиннер при preventDefault
    initSubmitLoading();
    initMatchFilters();
    initLetterCopy();
    initManualResolves();
  });

  function initManualResolves() {
    var payloadEl = qs("[data-letter-payload]");
    if (!payloadEl) {
      return;
    }
    var payload;
    try {
      payload = JSON.parse(payloadEl.textContent || "{}");
    } catch (e) {
      return;
    }
    rebuildEditorialLetter(payload);
    qsa("[data-manual-id] input[type='radio']").forEach(function (input) {
      input.addEventListener("change", function () {
        rebuildEditorialLetter(payload);
      });
    });
  }

  function rebuildEditorialLetter(payload) {
    var preview = qs("[data-letter-html]");
    var plainEl = qs("[data-letter-plain]");
    var warn = qs("[data-letter-pending]");
    var pending = 0;
    var extra = [];
    (payload.manual_items || []).forEach(function (item) {
      var chosen = qs("[data-manual-id='" + item.id + "'] input:checked");
      if (!chosen) {
        pending += 1;
        return;
      }
      if (chosen.value === "fail" && item.text) {
        extra.push(item);
      }
    });
    var items = (payload.auto_items || []).concat(extra);
    items.sort(function (a, b) {
      return (parseInt(a.order, 10) || 999) - (parseInt(b.order, 10) || 999);
    });
    var html = renderLetterHtml(payload.title || "журнал", payload.date_line || "", items);
    var plain = renderLetterPlain(payload.title || "журнал", payload.date_line || "", items);
    if (preview) preview.innerHTML = html;
    if (plainEl) plainEl.value = plain;
    if (warn) {
      if (pending > 0) {
        warn.hidden = false;
        warn.textContent =
          "Письмо сформировано предварительно. Не завершены " +
          pending +
          " " +
          (pending === 1 ? "ручная проверка" : pending < 5 ? "ручные проверки" : "ручных проверок") +
          ".";
      } else {
        warn.hidden = true;
        warn.textContent = "";
      }
    }
  }

  function escapeHtml(text) {
    return String(text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function renderLetterItemHtml(item) {
    var parts = ["<div>" + escapeHtml(item.text || "") + "</div>"];
    if (item.fix_path) {
      parts.push("<div><strong>Где исправить:</strong> " + escapeHtml(item.fix_path) + "</div>");
    }
    if (item.fix_role) {
      parts.push(
        '<div style="color:#555;font-size:0.95em;">Роль: ' + escapeHtml(item.fix_role) + "</div>"
      );
    }
    var steps = item.fix_steps || [];
    if (steps.length) {
      parts.push("<div><strong>Как исправить:</strong></div>");
      parts.push("<ul>");
      steps.forEach(function (step) {
        parts.push("  <li>" + escapeHtml(step) + "</li>");
      });
      parts.push("</ul>");
    }
    var docUrl = item.fix_doc_url || item.doc_url || "";
    if (docUrl) {
      parts.push(
        '<div><a href="' +
          escapeHtml(docUrl) +
          '" target="_blank" rel="noopener noreferrer">Открыть инструкцию</a></div>'
      );
    }
    return "<li>\n  " + parts.join("\n  ") + "\n</li>";
  }

  function appendLetterItemPlain(lines, index, item) {
    lines.push(index + ". " + (item.text || ""));
    if (item.fix_path) {
      lines.push("   Где исправить: " + item.fix_path);
    }
    if (item.fix_role) {
      lines.push("   Роль: " + item.fix_role);
    }
    var steps = item.fix_steps || [];
    if (steps.length) {
      lines.push("   Как исправить:");
      steps.forEach(function (step) {
        lines.push("   — " + step);
      });
    }
    var docUrl = item.fix_doc_url || item.doc_url || "";
    if (docUrl) {
      lines.push("   Инструкция: " + docUrl);
    }
  }

  function renderLetterHtml(title, dateLine, items) {
    var parts = [
      "<p>Уважаемые коллеги!</p>",
      "<p>  Мы проверили настройки сайта журнала  <strong>«" +
        escapeHtml(title) +
        "»</strong>  на Национальной платформе периодических научных изданий.</p>",
    ];
    if (!items.length) {
      parts.push(
        "<p>По результатам проверки обязательных замечаний нет — дополнительных изменений не требуется.</p>"
      );
      parts.push("<p><strong>Дата проверки:</strong> " + escapeHtml(dateLine) + ".</p>");
      parts.push(
        "<p>Если останутся вопросы по отдельным настройкам, пожалуйста, напишите нам.</p>"
      );
    } else {
      parts.push("<p>По результатам проверки просим внести следующие изменения:</p>");
      parts.push("<ol>");
      items.forEach(function (item) {
        parts.push(renderLetterItemHtml(item));
      });
      parts.push("</ol>");
      parts.push("<p><strong>Дата проверки:</strong> " + escapeHtml(dateLine) + ".</p>");
      parts.push(
        "<p>Если при внесении изменений потребуется помощь или пример заполнения, пожалуйста, напишите нам.</p>"
      );
    }
    parts.push(
      "<p>  С уважением,<br>  служба поддержки<br>  Национальной платформы периодических научных изданий</p>"
    );
    return parts.join("\n");
  }

  function renderLetterPlain(title, dateLine, items) {
    var lines = [
      "Уважаемые коллеги!",
      "",
      "Мы проверили настройки сайта журнала «" +
        title +
        "» на Национальной платформе периодических научных изданий.",
      "",
    ];
    if (!items.length) {
      lines.push(
        "По результатам проверки обязательных замечаний нет — дополнительных изменений не требуется."
      );
      lines.push("");
      lines.push("Дата проверки: " + dateLine + ".");
      lines.push("");
      lines.push("Если останутся вопросы по отдельным настройкам, пожалуйста, напишите нам.");
      lines.push("");
    } else {
      lines.push("По результатам проверки просим внести следующие изменения:");
      lines.push("");
      items.forEach(function (item, i) {
        appendLetterItemPlain(lines, i + 1, item);
        lines.push("");
      });
      lines.push("Дата проверки: " + dateLine + ".");
      lines.push("");
      lines.push(
        "Если при внесении изменений потребуется помощь или пример заполнения, пожалуйста, напишите нам."
      );
      lines.push("");
    }
    lines.push("С уважением,");
    lines.push("служба поддержки");
    lines.push("Национальной платформы периодических научных изданий");
    return lines.join("\n") + "\n";
  }

  function initLetterCopy() {
    var preview = qs("[data-letter-html]");
    var plainEl = qs("[data-letter-plain]");
    if (!preview && !plainEl) return;

    qsa("[data-copy-letter]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var mode = btn.getAttribute("data-copy-letter");
        var html = preview ? preview.innerHTML.trim() : "";
        var plain = plainEl ? plainEl.value : preview ? preview.innerText.trim() : "";
        var label = btn.textContent;
        function done(ok) {
          btn.textContent = ok ? "Скопировано" : "Не удалось скопировать";
          setTimeout(function () {
            btn.textContent = label;
          }, 1600);
        }
        if (mode === "html") {
          copyLetterHtml(html, plain, done);
        } else {
          copyLetterPlain(plain, done);
        }
      });
    });
  }

  function copyLetterPlain(text, done) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(
        function () {
          done(true);
        },
        function () {
          done(copyViaTextarea(text));
        }
      );
      return;
    }
    done(copyViaTextarea(text));
  }

  function copyLetterHtml(html, plain, done) {
    if (window.ClipboardItem && navigator.clipboard && navigator.clipboard.write) {
      try {
        var item = new ClipboardItem({
          "text/html": new Blob([html], { type: "text/html" }),
          "text/plain": new Blob([plain], { type: "text/plain" }),
        });
        navigator.clipboard.write([item]).then(
          function () {
            done(true);
          },
          function () {
            copyHtmlFallback(html, plain, done);
          }
        );
        return;
      } catch (e) {
        copyHtmlFallback(html, plain, done);
        return;
      }
    }
    copyHtmlFallback(html, plain, done);
  }

  function copyHtmlFallback(html, plain, done) {
    var el = document.createElement("div");
    el.contentEditable = "true";
    el.innerHTML = html;
    el.style.position = "fixed";
    el.style.left = "-9999px";
    document.body.appendChild(el);
    var range = document.createRange();
    range.selectNodeContents(el);
    var sel = window.getSelection();
    sel.removeAllRanges();
    sel.addRange(range);
    var ok = false;
    try {
      ok = document.execCommand("copy");
    } catch (e) {
      ok = false;
    }
    sel.removeAllRanges();
    document.body.removeChild(el);
    if (ok) {
      done(true);
      return;
    }
    copyLetterPlain(plain, done);
  }

  function copyViaTextarea(text) {
    var ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    var ok = false;
    try {
      ok = document.execCommand("copy");
    } catch (e) {
      ok = false;
    }
    document.body.removeChild(ta);
    return ok;
  }
})();
