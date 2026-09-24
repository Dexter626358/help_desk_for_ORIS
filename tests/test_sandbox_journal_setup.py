"""Тесты sandbox journal setup (без сети)."""

from __future__ import annotations

import pytest

from ipsas.modules.sandbox_journal_setup.forms import (
    HtmlForm,
    find_post_form,
    plugin_action_links,
)
from ipsas.modules.sandbox_journal_setup.urls import journal_url, parse_journal_url


def test_parse_journal_url_index() -> None:
    base, journal = parse_journal_url("https://f23g45.rcsi.science/257/index")
    assert base == "https://f23g45.rcsi.science"
    assert journal == "257"


def test_parse_journal_url_manager() -> None:
    base, journal = parse_journal_url(
        "https://f23g45.rcsi.science/257/manager/languages"
    )
    assert base == "https://f23g45.rcsi.science"
    assert journal == "257"


def test_parse_journal_url_trailing_slash() -> None:
    base, journal = parse_journal_url("https://f23g45.rcsi.science/257/")
    assert journal == "257"
    assert base.endswith("f23g45.rcsi.science")


def test_parse_journal_url_rejects_empty() -> None:
    with pytest.raises(ValueError):
        parse_journal_url("")


def test_journal_url_join() -> None:
    assert (
        journal_url("https://f23g45.rcsi.science", "257", "manager/languages")
        == "https://f23g45.rcsi.science/257/manager/languages"
    )


def test_languages_form_ensure_locales() -> None:
    html = """
    <form method="post" action="/257/manager/saveLanguageSettings">
      <select name="primaryLocale"><option value="ru_RU" selected>RU</option></select>
      <input type="checkbox" name="supportedLocales[]" value="en_US" checked>
      <input type="checkbox" name="supportedSubmissionLocales[]" value="en_US">
      <input type="checkbox" name="supportedFormLocales[]" value="en_US">
      <input type="checkbox" name="supportedLocales[]" value="ru_RU" checked>
      <input type="checkbox" name="supportedSubmissionLocales[]" value="ru_RU" checked>
      <input type="checkbox" name="supportedFormLocales[]" value="ru_RU" checked>
      <input type="checkbox" name="supportedLocales[]" value="zh_CN" checked>
    </form>
    """
    form = find_post_form(html, "https://f23g45.rcsi.science/257/manager/languages")
    assert form is not None
    form.ensure_multi_values(
        "supportedSubmissionLocales[]", {"ru_RU", "en_US"}, keep_others=True
    )
    form.ensure_multi_values(
        "supportedFormLocales[]", {"ru_RU", "en_US"}, keep_others=True
    )
    assert set(form.values_for("supportedLocales[]")) >= {"ru_RU", "en_US", "zh_CN"}
    assert set(form.values_for("supportedSubmissionLocales[]")) >= {"ru_RU", "en_US"}
    assert set(form.values_for("supportedFormLocales[]")) >= {"ru_RU", "en_US"}


def test_plugin_action_links() -> None:
    html = """
    <ul id="plugins">
      <li><h4>DOI</h4>
        <a href="https://x/257/manager/plugin/pubIds/DOIPubIdPlugin/enable">Включить</a>
      </li>
      <li><h4>EDN</h4>
        <a href="https://x/257/manager/plugin/pubIds/EDNPubIdPlugin/disable">Выключить</a>
      </li>
    </ul>
    """
    links = plugin_action_links(html)
    assert "doipubidplugin" in links
    assert "enable" in links["doipubidplugin"]
    assert "disable" in links["ednpubidplugin"]


def test_html_form_set_checkbox() -> None:
    form = HtmlForm(action="/x", method="post", form_id="")
    form.fields = [("keep", "1")]
    form.set_checkbox("enabled", True, "1")
    assert form.values_for("enabled") == ["1"]
    form.set_checkbox("enabled", False)
    assert form.values_for("enabled") == []


def test_plugin_label_ru() -> None:
    from ipsas.modules.sandbox_journal_setup.labels import plugin_label

    assert plugin_label("DOIPubIdPlugin") == "DOI"
    assert plugin_label("browseplugin") == "Браузер"
    assert plugin_label("acronPlugin") == "ACRON"
    assert plugin_label("fundrefplugin") == "FundRef"


def test_checklist_payload_helpers() -> None:
    from ipsas.modules.sandbox_journal_setup.steps import (
        _checklist_indexes,
        _payload_without_checklist,
    )

    fields = [
        ("formLocale", "ru_RU"),
        ("submissionChecklist[ru_RU][0][order]", "1"),
        ("submissionChecklist[ru_RU][0][content]", "text"),
        ("submissionChecklist[ru_RU][1][order]", "2"),
        ("metaSubject", "1"),
    ]
    assert _checklist_indexes(fields) == [0, 1]
    cleared = _payload_without_checklist(fields)
    assert cleared == [("formLocale", "ru_RU"), ("metaSubject", "1")]
    assert _checklist_indexes(cleared) == []


def test_homepage_template_blank() -> None:
    from ipsas.modules.sandbox_journal_setup.homepage_template import (
        HOMEPAGE_DESCRIPTION_EN,
        HOMEPAGE_DESCRIPTION_RU,
        is_blank_description,
    )

    assert is_blank_description("")
    assert is_blank_description("<p></p>")
    assert is_blank_description("<p>&nbsp;</p>")
    assert not is_blank_description(HOMEPAGE_DESCRIPTION_RU)
    assert not is_blank_description(HOMEPAGE_DESCRIPTION_EN)
    assert "ISSN (print)" in HOMEPAGE_DESCRIPTION_RU
    assert "[указать]" in HOMEPAGE_DESCRIPTION_RU
    assert "2306" not in HOMEPAGE_DESCRIPTION_RU
    assert "Golovko" not in HOMEPAGE_DESCRIPTION_EN
    assert "Founder:" in HOMEPAGE_DESCRIPTION_EN
    assert "[specify]" in HOMEPAGE_DESCRIPTION_EN
