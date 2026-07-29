"""Тесты проверки заполненности сайта журнала."""

from __future__ import annotations

from ipsas.modules.journal_site.checker import JournalSiteChecker
from ipsas.modules.journal_site.criteria import CRITERIA
from ipsas.modules.journal_site.evaluate import detect_access_model
from ipsas.modules.journal_site.parser import (
    extract_about_menu,
    extract_homepage_fields,
    normalize_journal_base_url,
    parse_html,
)


HOME_HTML_EN = """
<html lang="en"><body>
  <div id="homepageImage"><img src="/public/site/images/cover.jpg" alt="Homepage Image"/></div>
  <div id="content">
    <h1>Test Journal Title</h1>
    <p><strong>ISSN (online):</strong> 2312-1327</p>
    <p><strong>Languages:</strong> English, Russian</p>
    <p>The important results of fundamental and applied researches are published in the journal.
    The concept of the journal is defined by modern tendencies of scientific search in chemistry.</p>
    <div class="current_issue"><h2>No 1 (2026)</h2>
      <a href="/2312-1327/issue/view/27010">URL</a>
    </div>
  </div>
</body></html>
"""

HOME_HTML_RU = """
<html lang="ru"><body>
  <div id="homepageImage"><img src="/public/site/images/cover.jpg" alt="Homepage Image"/></div>
  <div id="content">
    <h1>Тестовый журнал</h1>
    <p><strong>ISSN (online):</strong> 2312-1327</p>
    <p><strong>Языки публикации:</strong> русский, английский</p>
    <p>В журнале публикуются важные результаты фундаментальных и прикладных исследований.
    Концепция журнала определяется современными тенденциями научного поиска в химии.</p>
    <div class="current_issue"><h2>№ 1 (2026)</h2>
      <a href="/2312-1327/issue/view/27010">URL</a>
    </div>
  </div>
</body></html>
"""

ABOUT_HTML = """
<html lang="en"><body><div id="content">
  <h1>About the Journal</h1>
  <h2>People</h2>
  <ul>
    <li><a href="https://journals.example/2312-1327/about/contact">Contact</a></li>
    <li><a href="https://journals.example/2312-1327/about/editorialTeam">Editorial Team</a></li>
  </ul>
  <h2>Policies</h2>
  <ul>
    <li><a href="https://journals.example/2312-1327/about/editorialPolicies#focusAndScope">Aims and Scope</a></li>
    <li><a href="https://journals.example/2312-1327/about/editorialPolicies#peerReviewProcess">Peer Review Process</a></li>
    <li><a href="https://journals.example/2312-1327/about/editorialPolicies#publicationFrequency">Publication Frequency</a></li>
    <li><a href="https://journals.example/2312-1327/about/editorialPolicies#openAccessPolicy">Open Access Policy</a></li>
    <li><a href="https://journals.example/2312-1327/about/editorialPolicies#custom-0">Publication Ethics</a></li>
  </ul>
  <h2>Submissions</h2>
  <ul>
    <li><a href="https://journals.example/2312-1327/about/submissions#onlineSubmissions">Online Submissions</a></li>
    <li><a href="https://journals.example/2312-1327/about/submissions#authorGuidelines">Author Guidelines</a></li>
    <li><a href="https://journals.example/2312-1327/about/submissions#copyrightNotice">Copyright Notice</a></li>
  </ul>
  <h2>Other</h2>
  <ul>
    <li><a href="https://journals.example/2312-1327/about/journalSponsorship">Journal Sponsorship</a></li>
    <li><a href="https://journals.example/2312-1327/about/history">History</a></li>
  </ul>
</div></body></html>
"""

POLICIES_HTML = """
<html lang="en"><body><div id="content">
  <div id="focusAndScope"><h2>Aims and Scope</h2>
    <p>The journal covers topical issues of chemistry and materials science for researchers and practitioners worldwide.
    Article types include original articles, reviews and short communications.</p>
  </div>
  <div id="peerReviewProcess"><h2>Peer Review Process</h2>
    <p>All scientific articles undergo double-blind peer review by independent reviewers.
    After receiving reviewer comments authors revise the manuscript.
    The editor-in-chief makes the final decision on publication.</p>
  </div>
  <div id="publicationFrequency"><h2>Publication Frequency</h2><p>4 issues per year.</p></div>
  <div id="openAccessPolicy"><h2>Open Access Policy</h2>
    <p>This journal provides open access to its content under the CC BY 4.0 license.
    Readers may reuse materials with attribution according to Creative Commons terms.</p>
  </div>
  <div id="custom-1"><h2>Sections and Directions</h2>
    <p>The journal is organized into thematic sections covering materials science and applied chemistry.</p>
  </div>
  <div id="custom-2"><h2>Archiving</h2>
    <p>Long-term archiving is provided via LOCKSS and CLOCKSS networks.</p>
  </div>
  <div id="custom-3"><h2>Indexing</h2>
    <p>The journal is indexed in international and Russian citation databases.</p>
  </div>
  <div id="custom-4"><h2>Open Access to Metadata</h2>
    <p>Publication metadata are openly available for harvesting and reuse.</p>
  </div>
  <div id="custom-5"><h2>Personal Data Policy</h2>
    <p>Personal data of authors, reviewers and readers are processed in accordance with applicable law.</p>
  </div>
  <div id="custom-6"><h2>Generative AI Policy</h2>
    <p>Authors must disclose any use of generative artificial intelligence tools in manuscript preparation.</p>
  </div>
  <div id="custom-7"><h2>Retraction Policy</h2>
    <p>The journal publishes retractions when serious issues are confirmed after publication.</p>
  </div>
  <div id="custom-8"><h2>Publication Fee</h2>
    <p>Publication fee is not charged for accepted articles.</p>
  </div>
  <div id="custom-0"><h2>Publication Ethics</h2>
    <p>The journal follows COPE publication ethics. Authorship criteria and author responsibility are defined.
    Authors must disclose conflict of interest and funding. Plagiarism is checked with specialized software.
    Duplicate publication is prohibited. The journal publishes corrections and handles retraction of articles.
    Complaints and appeals against editorial decisions are accepted. Long-term archiving is provided via LOCKSS.
    Publication fee is not charged.</p>
  </div>
</div></body></html>
"""

POLICIES_HTML_RU = POLICIES_HTML.replace('lang="en"', 'lang="ru"')

TEAM_HTML = """
<html lang="en"><body><div id="content">
  <h1>Editorial Team</h1>
  <p>Editor-in-Chief: Ivan Ivanov, PhD, Moscow State University</p>
  <div id="editorialBoard">
    <h2>Editorial Board</h2>
    <ul>
      <li>Petr Petrov, Institute of Chemistry RAS</li>
      <li>Anna Sidorova, Novosibirsk State University</li>
      <li>John Smith, University of Oxford</li>
      <li>Maria Garcia, Barcelona Institute of Science</li>
      <li>Chen Wei, Peking University</li>
      <li>Olga Kuznetsova, Tomsk Polytechnic University</li>
    </ul>
  </div>
</div></body></html>
"""

TEAM_HTML_RU = TEAM_HTML.replace('lang="en"', 'lang="ru"').replace(
    "Editor-in-Chief", "Главный редактор"
)

SUBMISSIONS_HTML = """
<html lang="en"><body><div id="content">
  <h1>Submissions</h1>
  <p>Authors may submit manuscripts to the journal through the online editorial system.</p>
  <div id="onlineSubmissions"><h2>Online Submissions</h2>
    <p>Registration and login are required to submit items online to this journal through the platform.</p>
  </div>
  <div id="authorGuidelines"><h2>Author Guidelines</h2>
    <p>Authors should prepare manuscripts according to the journal template. The article structure must include
    title, abstract, keywords, main text, and references. Author information must contain names, affiliations
    and contacts. Declarations on conflict of interest and funding are required. References must follow the
    journal citation style. Submissions are accepted via the online system after registration.</p>
  </div>
  <div id="copyrightNotice"><h2>Copyright Notice</h2>
    <p>Authors who publish in this journal agree to the copyright terms and Creative Commons reuse conditions.
    Copyright belongs to the authors while publishing rights are licensed to the journal.</p>
  </div>
  <div id="privacyStatement"><h2>Privacy Statement</h2>
    <p>The journal collects and processes personal data of authors and reviewers in accordance with applicable law.</p>
  </div>
</div></body></html>
"""

SUBMISSIONS_HTML_RU = (
    SUBMISSIONS_HTML.replace('lang="en"', 'lang="ru"')
    .replace("<h1>Submissions</h1>", "<h1>Прием статей</h1>")
    .replace("Online Submissions", "Отправка статей")
    .replace("Author Guidelines", "Правила для авторов")
    .replace("Copyright Notice", "Авторские права")
    .replace("Privacy Statement", "Конфиденциальность")
)

CONTACT_HTML = """
<html lang="en"><body><div id="content">
  <h1>Journal Contact</h1>
  <div id="mailingAddress">
    <h2>Mailing Address</h2>
    <p>Editorial Office of Test Journal, Russia, 664047, Irkutsk, Kommunarov 3</p>
    <iframe src="https://www.google.com/maps/embed?pb=test" width="600" height="450"></iframe>
  </div>
  <div id="principalContact">
    <h2>Principal Contact</h2>
    <p>Ljudmila Ivanova Editor-in-Chief Phone:+7 902 177 25 67 Email: credeexperto@if-mstuca.ru</p>
  </div>
</div></body></html>
"""

SPONSORSHIP_HTML = """
<html lang="en"><body><div id="content">
  <h1>Journal Sponsorship</h1>
  <p>Publisher: Test University Press. Founder: Test University.
  The responsible organization for the journal is Test University Press.</p>
</div></body></html>
"""

HISTORY_HTML = """
<html lang="en"><body><div id="content">
  <h1>History</h1>
  <p>The journal was founded in 2001 and publishes chemistry research.</p>
</div></body></html>
"""


def test_normalize_journal_base_url():
    assert (
        normalize_journal_base_url("https://journals.rcsi.science/2312-1327/index")
        == "https://journals.rcsi.science/2312-1327"
    )


def test_extract_homepage_fields():
    fields = extract_homepage_fields(parse_html(HOME_HTML_EN))
    assert fields["journal_title"] == "Test Journal Title"
    assert "2312-1327" in fields["issn"]


def test_extract_about_menu_all_items():
    items = extract_about_menu(
        parse_html(ABOUT_HTML),
        base_url="https://journals.example/2312-1327",
    )
    ids = [i.id for i in items]
    assert "about_focusAndScope" in ids
    assert "about_editorialTeam" in ids


def test_extract_about_menu_russian_section_titles():
    about_ru = """
    <html lang="ru"><body><div id="content">
      <h2>Редакция</h2>
      <ul><li><a href="https://journals.example/j/about/editorialTeam">Редакция</a></li></ul>
      <h2>Политика редакции</h2>
      <ul>
        <li><a href="https://journals.example/j/about/editorialPolicies#focusAndScope">Тематика журнала</a></li>
      </ul>
      <h2>Прием статей</h2>
      <ul><li><a href="https://journals.example/j/about/submissions#authorGuidelines">Правила для авторов</a></li></ul>
      <h2>Прочее</h2>
      <ul><li><a href="https://journals.example/j/about/subscriptions">Подписка</a></li></ul>
    </div></body></html>
    """
    items = extract_about_menu(parse_html(about_ru), base_url="https://journals.example/j")
    by_cat: dict[str, list[str]] = {}
    for i in items:
        by_cat.setdefault(i.category, []).append(i.id)
    assert "about_focusAndScope" in by_cat.get("policies", [])


def test_editorial_members_ignore_technical_editors():
    """Секция Editors / Редакторы — редколлегия; техредакторы не входят."""
    from ipsas.modules.journal_site.parser import extract_editorial_members

    html = """
    <html><body><div id="content">
      <div id="editorialTeam">
        <div id="editors"><h2>Редакторы</h2>
          <ul><li>Иванов Иван, Университет</li></ul>
        </div>
        <div id="layoutEditors"><h2>Худ. редакторы</h2>
          <ul><li>Петров Пётр, Университет</li></ul>
        </div>
      </div>
    </div></body></html>
    """
    members_ru = extract_editorial_members(parse_html(html))
    assert len(members_ru) == 1
    assert "Иванов" in members_ru[0]["raw"]

    html_ojs_editors = """
    <html><body><div id="content">
      <div id="editors"><h2>Editors</h2>
        <ol>
          <li>Oleg K. Gogaev, Gorsky State Agrarian University, Russia</li>
          <li>Alena A. Erusalimskaya, Russia</li>
        </ol>
      </div>
      <div id="layoutEditors"><h2>Layout Editors</h2>
        <ol><li>Valentina Zolotareva, University</li></ol>
      </div>
    </div></body></html>
    """
    members_ojs = extract_editorial_members(parse_html(html_ojs_editors))
    assert len(members_ojs) == 2
    assert all(m["has_affiliation"] for m in members_ojs[:1])

    html_board = """
    <html><body><div id="content">
      <div id="editorialBoard"><h2>Редакционная коллегия</h2>
        <ul>
          <li>Сидоров С.С., МГУ</li>
          <li>Smith J., Oxford University</li>
          <li>Chen W., Peking University</li>
          <li>Garcia M., Barcelona Institute</li>
          <li>Kuznetsova O., Tomsk University</li>
        </ul>
      </div>
    </div></body></html>
    """
    members = extract_editorial_members(parse_html(html_board))
    assert len(members) >= 5


def test_detect_access_model_ignores_plant_hybrid_word():
    assert detect_access_model("изучены гибриды картофеля на склонах") == "unknown"
    assert detect_access_model("журнал предоставляет открытый доступ") == "open_access"
    assert detect_access_model("Open") == "open_access"


def test_criteria_catalog_has_requirement_levels():
    reqs = {c.requirement for c in CRITERIA}
    assert reqs == {"required", "conditional", "optional"}
    assert any(c.critical for c in CRITERIA)


def test_resolve_policy_section_by_heading():
    from ipsas.modules.journal_site.parser import parse_html, resolve_policy_section_body

    html = """
    <html><body><div id="content">
      <div id="custom-3"><h2>Индексирование</h2><p>Журнал включён в РИНЦ.</p></div>
    </div></body></html>
    """
    body, fid = resolve_policy_section_body(
        parse_html(html),
        title_aliases=("индексирование", "indexing"),
    )
    assert fid == "custom-3"
    assert "РИНЦ" in body


def test_checker_bilingual_with_fixture_fetch():
    pages = {
        ("index", "en"): HOME_HTML_EN.encode("utf-8"),
        ("index", "ru"): HOME_HTML_RU.encode("utf-8"),
        ("about", "en"): ABOUT_HTML.encode("utf-8"),
        ("about", "ru"): ABOUT_HTML.replace('lang="en"', 'lang="ru"').encode("utf-8"),
        ("editorialTeam", "en"): TEAM_HTML.encode("utf-8"),
        ("editorialTeam", "ru"): TEAM_HTML_RU.encode("utf-8"),
        ("editorialPolicies", "en"): POLICIES_HTML.encode("utf-8"),
        ("editorialPolicies", "ru"): POLICIES_HTML_RU.encode("utf-8"),
        ("submissions", "en"): SUBMISSIONS_HTML.encode("utf-8"),
        ("submissions", "ru"): SUBMISSIONS_HTML_RU.encode("utf-8"),
        ("contact", "en"): CONTACT_HTML.encode("utf-8"),
        ("contact", "ru"): CONTACT_HTML.encode("utf-8"),
        ("journalSponsorship", "en"): SPONSORSHIP_HTML.encode("utf-8"),
        ("journalSponsorship", "ru"): SPONSORSHIP_HTML.encode("utf-8"),
        ("history", "en"): HISTORY_HTML.encode("utf-8"),
        ("history", "ru"): HISTORY_HTML.encode("utf-8"),
        ("subscriptions", "en"): "<html><body><div id='content'>Open access journal</div></body></html>".encode("utf-8"),
        ("subscriptions", "ru"): "<html><body><div id='content'>Открытый доступ</div></body></html>".encode("utf-8"),
    }

    def fetch(url: str, lang: str) -> bytes:
        path = url.rstrip("/").split("?")[0]
        for key, body in pages.items():
            page_key, page_lang = key
            if page_lang != lang:
                continue
            if path.endswith("/" + page_key) or path.endswith(page_key):
                return body
        raise AssertionError(f"unexpected url: {url} lang={lang}")

    report = JournalSiteChecker(fetch=fetch).check(
        "https://journals.example/2312-1327/index"
    )
    data = report.to_dict()
    assert data["completeness_ru"] > 50
    assert data["completeness_en"] > 50
    assert data["locales"]["en"]["journal_title"] == "Test Journal Title"
    en_fields = {f["id"]: f for f in data["locales"]["en"]["fields"]}
    assert en_fields["journal.title"]["status"] == "complete"
    assert en_fields["policy.aims_scope"]["status"] in {"complete", "partial"}
    assert en_fields["policy.peer_review"]["status"] in {"complete", "partial"}
    assert en_fields["contact.email"]["status"] == "complete"
    assert en_fields["ownership.publisher"]["status"] in {"complete", "partial"}
    assert en_fields["journal.cover"]["requirement"] == "required"
    assert en_fields["journal.cover"]["status"] == "complete"
    assert en_fields["journal.current_issue"]["status"] == "complete"
    assert en_fields["journal.language"]["requirement"] == "optional"
    # подписка не требуется для OA
    assert en_fields["subscription.method"]["status"] == "not_applicable"
    assert data["locales"]["en"]["required_total"] > 0
    assert isinstance(data["must_fix"], list)
    assert isinstance(data["recommendations"], list)
