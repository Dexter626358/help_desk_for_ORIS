(function () {
  function onReady(fn) {
    if (document.readyState !== "loading") fn();
    else document.addEventListener("DOMContentLoaded", fn);
  }

  onReady(function () {
    // Drag-and-drop загрузки
    var dropzone = document.getElementById("dropzone");
    var fileInput = document.getElementById("xml_file");
    var chosen = document.getElementById("chosen-name");
    if (dropzone && fileInput) {
      ["dragenter", "dragover"].forEach(function (ev) {
        dropzone.addEventListener(ev, function (e) {
          e.preventDefault();
          e.stopPropagation();
          dropzone.classList.add("dragover");
        });
      });
      ["dragleave", "drop"].forEach(function (ev) {
        dropzone.addEventListener(ev, function (e) {
          e.preventDefault();
          e.stopPropagation();
          dropzone.classList.remove("dragover");
        });
      });
      dropzone.addEventListener("drop", function (e) {
        var files = e.dataTransfer && e.dataTransfer.files;
        if (files && files.length) {
          fileInput.files = files;
          if (chosen) chosen.textContent = files[0].name;
        }
      });
      fileInput.addEventListener("change", function () {
        if (chosen && fileInput.files && fileInput.files[0]) {
          chosen.textContent = fileInput.files[0].name;
        }
      });
    }

    // Подтверждения
    document.querySelectorAll("form[data-confirm], button[data-confirm]").forEach(function (el) {
      el.addEventListener("click", function (e) {
        var msg = el.getAttribute("data-confirm");
        if (msg && !window.confirm(msg)) {
          e.preventDefault();
          e.stopPropagation();
        }
      });
    });

    // Активная вкладка → hidden field
    var activeTabInput = document.getElementById("active_tab");
    document.querySelectorAll("#articleTabs [data-tab-name]").forEach(function (btn) {
      btn.addEventListener("shown.bs.tab", function () {
        if (activeTabInput) activeTabInput.value = btn.getAttribute("data-tab-name") || "main";
      });
    });

    // ENG → RUS для автора
    document.querySelectorAll(".btn-copy-eng-rus").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var block = btn.closest(".author-block");
        if (!block) return;
        var fields = ["surname", "initials", "first_name", "middle_name", "org_name", "address", "email", "orcid"];
        fields.forEach(function (field) {
          var eng = block.querySelector('[data-lang="ENG"][data-field="' + field + '"]');
          var rus = block.querySelector('[data-lang="RUS"][data-field="' + field + '"]');
          if (eng && rus) rus.value = eng.value;
        });
      });
    });

    // Источники: добавить / удалить
    var refsList = document.getElementById("refs-list");
    var addRefBtn = document.getElementById("btn-add-ref");
    function nextRefIndex() {
      var max = -1;
      refsList.querySelectorAll(".ref-item").forEach(function (item) {
        var idx = parseInt(item.getAttribute("data-ref-index") || "-1", 10);
        if (idx > max) max = idx;
      });
      return max + 1;
    }
    function bindRemove(btn) {
      btn.addEventListener("click", function () {
        var item = btn.closest(".ref-item");
        if (item) item.remove();
      });
    }
    if (refsList) {
      refsList.querySelectorAll(".btn-remove-ref").forEach(bindRemove);
    }
    if (addRefBtn && refsList) {
      addRefBtn.addEventListener("click", function () {
        var empty = document.getElementById("refs-empty");
        if (empty) empty.remove();
        var idx = nextRefIndex();
        var wrap = document.createElement("div");
        wrap.className = "ref-item border rounded p-2 mb-2";
        wrap.setAttribute("data-ref-index", String(idx));
        wrap.innerHTML =
          '<div class="d-flex justify-content-between align-items-center mb-1">' +
          '<span class="small text-muted">Новый источник · UNK</span>' +
          '<button type="button" class="btn btn-sm btn-outline-danger btn-remove-ref">Удалить</button>' +
          "</div>" +
          '<input type="hidden" name="ref-' + idx + '-lang" value="UNK">' +
          '<textarea class="form-control" name="ref-' + idx + '-text" rows="3"></textarea>';
        refsList.appendChild(wrap);
        bindRemove(wrap.querySelector(".btn-remove-ref"));
      });
    }
  });
})();
