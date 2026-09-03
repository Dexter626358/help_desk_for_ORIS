"""Парсинг JATS XML статьи для issue metadata."""

from __future__ import annotations

import re
from datetime import date
from typing import Callable, Dict, List, Optional

from lxml import etree

from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

def parse_jats_xml(
    xml_bytes: bytes,
    *,
    detect_lang_fn: Callable[[Optional[str]], Optional[str]] | None = None,
) -> Dict[str, object]:
    """Разобрать JATS XML статьи в словарь полей."""
    from ipsas.modules.issue_metadata.lang import detect_lang as _default_detect

    _detect = detect_lang_fn or _default_detect

    # Сначала строгий парсинг (чтобы не получить silent corruption), затем recover с явным предупреждением.
    parser = etree.XMLParser(
        recover=False,
        huge_tree=False,
        resolve_entities=False,
        no_network=True,
        load_dtd=False,
    )
    recovered = False
    try:
        root = etree.fromstring(xml_bytes, parser=parser)
    except etree.XMLSyntaxError as e:
        logger.warning("JATS XML синтаксически некорректен, включаем recover=True: %s", e)
        recovered = True
        parser_recover = etree.XMLParser(
            recover=True,
            huge_tree=False,
            resolve_entities=False,
            no_network=True,
            load_dtd=False,
        )
        try:
            root = etree.fromstring(xml_bytes, parser=parser_recover)
        except etree.XMLSyntaxError as e2:
            raise ValueError(f"Ошибка парсинга JATS XML: {e2}") from e2

    def detect_lang(text: Optional[str]) -> Optional[str]:
        return _detect(text)

    # XML namespace для атрибута xml:lang (JATS и др. используют namespace)
    NS_XML = "http://www.w3.org/XML/1998/namespace"

    def get_lang_attr(node: etree._Element) -> str:
        xml_lang = node.get("{http://www.w3.org/XML/1998/namespace}lang")
        lang = xml_lang or node.get("lang") or ""
        return lang.strip().lower()

    def normalize_lang(lang: str) -> str:
        lang = (lang or "").strip().lower()
        if lang.startswith("ru") or lang in {"rus", "russian"}:
            return "ru"
        if lang.startswith("en") or lang in {"eng", "english"}:
            return "en"
        return lang

    def extract_text(node: etree._Element) -> Optional[str]:
        para_texts = [t.strip() for t in node.xpath(".//*[local-name()='p']//text()") if t and t.strip()]
        if para_texts:
            text = " ".join(para_texts)
        else:
            parts: List[str] = []
            if node.text and node.text.strip():
                parts.append(node.text.strip())
            for child in node:
                local_name = etree.QName(child).localname
                if local_name in {"title", "label"}:
                    if child.tail and child.tail.strip():
                        parts.append(child.tail.strip())
                    continue
                parts.extend(t.strip() for t in child.itertext() if t and t.strip())
                if child.tail and child.tail.strip():
                    parts.append(child.tail.strip())
            text = " ".join(parts)
        cleaned = re.sub(r"\s+", " ", text).strip()
        return cleaned or None

    def collect_abstract(lang: str) -> Optional[str]:
        # 0) Сначала ищем по XPath с учётом namespace: abstract/trans-abstract с xml:lang или lang.
        # В JATS элементы в namespace, атрибут xml:lang тоже в namespace — без local-name() и
        # привязки префикса xml XPath ничего не находит.
        nodes_by_lang = root.xpath(
            f".//*[(local-name()='abstract' or local-name()='trans-abstract') and "
            f"(starts-with(@xml:lang, '{lang}') or starts-with(@lang, '{lang}'))]",
            namespaces={"xml": NS_XML},
        )
        if nodes_by_lang:
            for node in nodes_by_lang:
                text = extract_text(node)
                if text:
                    detected = detect_lang(text)
                    if lang == "ru" and detected != "ru":
                        continue
                    if lang == "en" and detected != "en":
                        continue
                    return text

        # Все abstract/trans-abstract без привязки к namespace (local-name() уже от namespace не зависит)
        all_abstracts: List[etree._Element] = root.xpath(
            ".//*[local-name()='abstract' or local-name()='trans-abstract']"
        )
        if not all_abstracts:
            return None

        # 1) strict by lang attr (по уже полученному атрибуту узла)
        for node in all_abstracts:
            node_lang = normalize_lang(get_lang_attr(node))
            if node_lang == lang:
                text = extract_text(node)
                if text:
                    detected = detect_lang(text)
                    if lang == "ru" and detected != "ru":
                        continue
                    if lang == "en" and detected != "en":
                        continue
                    return text

        # 2) fallback by content language detection
        for node in all_abstracts:
            text = extract_text(node)
            if text and detect_lang(text) == lang:
                return text

        return None

    def collect_keywords(lang: str) -> List[str]:
        kwd_groups: List[etree._Element] = root.xpath(".//*[local-name()='kwd-group']")
        result: List[str] = []
        allowed_types = {"", "author-generated", "author-keywords", "author"}

        def add_from_group(group: etree._Element) -> None:
            for node in group.xpath("./*[local-name()='kwd']"):
                text = extract_text(node) or " ".join(
                    t.strip() for t in node.xpath(".//text()") if t and t.strip()
                )
                if text and text not in result:
                    result.append(text)

        for group in kwd_groups:
            if normalize_lang(get_lang_attr(group)) != lang:
                continue
            group_type = (group.get("kwd-group-type") or "").strip().lower()
            if group_type not in allowed_types:
                continue
            add_from_group(group)
        if result:
            return result

        for group in kwd_groups:
            group_type = (group.get("kwd-group-type") or "").strip().lower()
            if group_type not in allowed_types:
                continue
            keywords: List[str] = []
            for node in group.xpath("./*[local-name()='kwd']"):
                text = extract_text(node) or " ".join(
                    t.strip() for t in node.xpath(".//text()") if t and t.strip()
                )
                if text:
                    keywords.append(text)
            if not keywords:
                continue
            sample = " ".join(keywords[:3])
            if detect_lang(sample) == lang:
                for kw in keywords:
                    if kw not in result:
                        result.append(kw)
        return result

    def extract_article_titles() -> Dict[str, Optional[str]]:
        title_ru: Optional[str] = None
        title_en: Optional[str] = None
        for group in root.xpath(
            ".//*[local-name()='article-meta']/*[local-name()='title-group']"
        ):
            for node in group.xpath("./*[local-name()='article-title']"):
                text = extract_text(node)
                if not text:
                    continue
                lang = normalize_lang(get_lang_attr(node))
                if lang == "ru":
                    title_ru = title_ru or text
                elif lang == "en":
                    title_en = title_en or text
                else:
                    detected = detect_lang(text)
                    if detected == "ru":
                        title_ru = title_ru or text
                    elif detected == "en":
                        title_en = title_en or text
            for node in group.xpath(
                ".//*[local-name()='trans-title-group']/*[local-name()='trans-title']"
            ):
                text = extract_text(node)
                if not text:
                    continue
                parent = node.getparent()
                lang = normalize_lang(get_lang_attr(parent) if parent is not None else "")
                if not lang:
                    lang = normalize_lang(get_lang_attr(node))
                if lang == "ru":
                    title_ru = title_ru or text
                elif lang == "en":
                    title_en = title_en or text
                else:
                    detected = detect_lang(text)
                    if detected == "ru":
                        title_ru = title_ru or text
                    elif detected == "en":
                        title_en = title_en or text
        return {"title_ru": title_ru, "title_en": title_en}

    def extract_affiliations() -> Dict[str, object]:
        aff_nodes = root.xpath(
            ".//*[local-name()='article-meta']//*[local-name()='aff']"
        )
        raw_entries: List[Dict[str, object]] = []
        for node in aff_nodes:
            aff_id = (node.get("id") or "").strip() or None
            inst_nodes = node.xpath(".//*[local-name()='institution']")
            if inst_nodes:
                for inst in inst_nodes:
                    name = extract_text(inst)
                    if name:
                        name = re.sub(r"^\d+\s*[.)]?\s*", "", name).strip() or name
                    lang = normalize_lang(get_lang_attr(inst)) or detect_lang(name)
                    raw_entries.append(
                        {
                            "id": aff_id,
                            "name": name,
                            "lang": lang,
                            "empty": not bool(name),
                        }
                    )
            else:
                name = extract_text(node)
                if name:
                    name = re.sub(r"^\d+\s*[.)]?\s*", "", name).strip() or name
                lang = normalize_lang(get_lang_attr(node)) or detect_lang(name)
                raw_entries.append(
                    {
                        "id": aff_id,
                        "name": name,
                        "lang": lang,
                        "empty": not bool(name),
                    }
                )

        has_real_ids = any(bool(e.get("id")) for e in raw_entries)
        affiliations: List[Dict[str, object]] = []
        if has_real_ids:
            affiliations = raw_entries
        elif raw_entries:
            # Нет id у <aff>, но авторы ссылаются на aff1/aff2 — нумеруем
            # организации по порядку RU/EN institution.
            ru_list = [e for e in raw_entries if e.get("lang") == "ru" and e.get("name")]
            en_list = [e for e in raw_entries if e.get("lang") == "en" and e.get("name")]
            other = [
                e
                for e in raw_entries
                if e.get("lang") not in {"ru", "en"} and e.get("name")
            ]
            n_slots = max(len(ru_list), len(en_list), len(other), 0)
            for i in range(n_slots):
                sid = f"aff{i + 1}"
                ru_name = str(ru_list[i]["name"]) if i < len(ru_list) else None
                en_name = str(en_list[i]["name"]) if i < len(en_list) else None
                other_name = str(other[i]["name"]) if i < len(other) else None
                if ru_name:
                    affiliations.append(
                        {
                            "id": sid,
                            "name": ru_name,
                            "name_ru": ru_name,
                            "name_en": en_name,
                            "lang": "ru",
                            "empty": False,
                        }
                    )
                if en_name:
                    affiliations.append(
                        {
                            "id": sid,
                            "name": en_name,
                            "name_ru": ru_name,
                            "name_en": en_name,
                            "lang": "en",
                            "empty": False,
                        }
                    )
                if other_name and not ru_name and not en_name:
                    affiliations.append(
                        {
                            "id": sid,
                            "name": other_name,
                            "lang": other[i].get("lang"),
                            "empty": False,
                        }
                    )
        else:
            affiliations = []

        aff_ids: set[str] = {
            str(a.get("id")).strip()
            for a in affiliations
            if a.get("id") and str(a.get("id")).strip()
        }

        contrib_refs: List[Dict[str, object]] = []
        broken: List[Dict[str, object]] = []
        for contrib in root.xpath(
            ".//*[local-name()='article-meta']//*[local-name()='contrib']"
        ):
            ctype = (contrib.get("contrib-type") or "").strip().lower()
            if ctype and ctype not in {"author", "aut"}:
                continue
            # Предпочитаем RU-имя для сообщений редакции
            author_name = None
            for name_node in contrib.xpath(
                ".//*[local-name()='name-alternatives']"
                "/*[local-name()='name' or local-name()='string-name']"
                " | ./*[local-name()='name' or local-name()='string-name']"
            ):
                formatted = None
                local = etree.QName(name_node).localname
                if local == "string-name":
                    formatted = extract_text(name_node)
                else:
                    surname = ""
                    given = ""
                    for sn in name_node.xpath(".//*[local-name()='surname']"):
                        surname = (sn.text or "").strip()
                        if surname:
                            break
                    for gn in name_node.xpath(".//*[local-name()='given-names']"):
                        given = (gn.text or "").strip()
                        if given:
                            break
                    if surname or given:
                        formatted = f"{surname} {given}".strip()
                if not formatted:
                    continue
                lang = normalize_lang(get_lang_attr(name_node)) or detect_lang(formatted)
                if lang == "ru" or author_name is None:
                    author_name = formatted
                if lang == "ru":
                    break

            rids: List[str] = []
            for xref in contrib.xpath(".//*[local-name()='xref']"):
                ref_type = (xref.get("ref-type") or "").strip().lower()
                if ref_type and ref_type not in {"aff", "affiliation"}:
                    continue
                rid = (xref.get("rid") or "").strip()
                if not rid:
                    continue
                for part in re.split(r"\s+", rid):
                    if not part:
                        continue
                    rids.append(part)
                    if aff_ids and part not in aff_ids:
                        broken.append(
                            {
                                "author": author_name,
                                "rid": part,
                                "reason": "missing_aff",
                            }
                        )
            # Всегда по одному элементу на автора — для выравнивания с authors_ru/en
            contrib_refs.append({"author": author_name, "rid": rids})

        for aff in affiliations:
            if aff.get("empty") and aff.get("id"):
                broken.append(
                    {
                        "author": None,
                        "rid": aff.get("id"),
                        "reason": "empty_aff",
                    }
                )

        return {
            "jats_affiliations": affiliations,
            "contributor_affiliation_refs": contrib_refs,
            "broken_affiliation_refs": broken,
        }

    def collect_references_by_lang() -> Dict[str, object]:
        """
        JATS: один <ref> = одна запись списка.

        Язык citation берётся из xml:lang / lang у mixed-citation|element-citation
        (типично citation-alternatives с xml:lang=\"ru\" и xml:lang=\"en\").
        Эвристика по алфавиту — только если атрибут языка отсутствует.

        При наличии обеих языковых версий у ref считаем обе (parallel_citations),
        а не выбираем одну «доминирующую».
        """
        ru_refs: List[str] = []
        en_refs: List[str] = []
        unk_refs: List[str] = []
        primary_items: List[str] = []
        refs_with_both = 0
        citations_with_lang_attr = 0
        ref_nodes = root.xpath(".//*[local-name()='ref-list']//*[local-name()='ref']")

        for ref_node in ref_nodes:
            mixed_nodes: List[etree._Element] = ref_node.xpath(
                ".//*[local-name()='mixed-citation' or local-name()='element-citation']"
            )
            by_lang: Dict[str, str] = {}
            unlanged: List[str] = []
            for node in mixed_nodes:
                raw_lang = get_lang_attr(node)
                lang = normalize_lang(raw_lang)
                # Текст citation без <label> соседнего уровня; label обычно вне mixed-citation
                txt = " ".join(t.strip() for t in node.xpath(".//text()") if t and t.strip())
                txt = re.sub(r"\s+", " ", txt).strip()
                if not txt:
                    continue
                if lang in {"ru", "en"}:
                    by_lang.setdefault(lang, txt)
                    citations_with_lang_attr += 1
                else:
                    unlanged.append(txt)

            if "ru" in by_lang and "en" in by_lang:
                refs_with_both += 1

            # Основная запись для списка/проверок качества — одна на <ref>
            primary = by_lang.get("ru") or by_lang.get("en")
            if not primary and unlanged:
                primary = unlanged[0]
            if not primary:
                txt = " ".join(t.strip() for t in ref_node.xpath(".//text()") if t and t.strip())
                primary = re.sub(r"\s+", " ", txt).strip() or None
            if primary:
                primary_items.append(primary)

            # Язык — строго по xml:lang, если он задан
            if by_lang:
                if "ru" in by_lang:
                    ru_refs.append(by_lang["ru"])
                if "en" in by_lang:
                    en_refs.append(by_lang["en"])
                # Только неизвестный lang без ru/en — не сюда (уже в unlanged)
            elif primary:
                # Нет xml:lang — эвристика по доминирующему алфавиту
                lat = len(re.findall(r"[A-Za-z]", primary))
                cyr = len(re.findall(r"[А-Яа-яЁё]", primary))
                if cyr > lat:
                    ru_refs.append(primary)
                elif lat > cyr:
                    en_refs.append(primary)
                else:
                    unk_refs.append(primary)

        def stats(items: List[str]) -> Dict[str, Optional[object]]:
            return {
                "count": len(items),
                "first": items[0] if items else None,
                "last": items[-1] if items else None,
            }

        total_refs = len(ref_nodes)
        # parallel_citations — у большинства ref есть обе языковые версии по xml:lang
        mode = "single_list"
        if total_refs >= 1 and refs_with_both >= max(1, (total_refs + 1) // 2):
            mode = "parallel_citations"

        return {
            "mode": mode,
            "total_refs": total_refs,
            "items": primary_items,
            "lang_from_attr": citations_with_lang_attr > 0,
            "ru": stats(ru_refs),
            "en": stats(en_refs),
            "unk": stats(unk_refs),
        }

    def extract_journal_meta() -> Dict[str, Optional[str]]:
        # JATS: journal-title-group/journal-title (EN) + trans-title-group xml:lang="ru"/trans-title (RU)
        journal_title_en = None
        for node in root.xpath(".//*[local-name()='journal-title']"):
            lang = (node.get("{http://www.w3.org/XML/1998/namespace}lang") or node.get("lang") or "").strip().lower()
            txt = (node.text or "").strip()
            if not txt:
                continue
            if lang.startswith("en") or not lang:
                journal_title_en = journal_title_en or txt
        journal_title_ru = None
        for node in root.xpath(".//*[local-name()='trans-title-group']"):
            lang = (node.get("{http://www.w3.org/XML/1998/namespace}lang") or node.get("lang") or "").strip().lower()
            if not lang.startswith("ru"):
                continue
            trans = node.xpath(".//*[local-name()='trans-title']/text()")
            if trans and (trans[0] or "").strip():
                journal_title_ru = (trans[0] or "").strip()
                break
        # ISSN: встречаются и pub-type (JATS), и publication-format (некоторые выгрузки)
        issn_print = None
        issn_online = None
        for node in root.xpath(".//*[local-name()='issn']"):
            value = (node.text or "").strip()
            if not value:
                continue
            pub_type = (node.attrib.get("pub-type") or "").strip().lower()
            pub_format = (node.attrib.get("publication-format") or node.attrib.get("publication_format") or "").strip().lower()
            if pub_type in {"ppub", "print"} or pub_format in {"print"}:
                issn_print = issn_print or value
            elif pub_type in {"epub", "online"} or pub_format in {"electronic", "online"}:
                issn_online = issn_online or value
            else:
                issn_print = issn_print or value
        return {
            "journal_title": journal_title_en,
            "journal_title_ru": journal_title_ru,
            "issn": issn_print,
            "eissn": issn_online,
        }

    # Идентификаторы статьи: DOI и EDN (из codes/ или article-id)
    identifiers: Dict[str, Optional[str]] = {
        "doi": None,
        "edn": None,
        "internal_id": None,
        "invalid_edn": None,
    }
    from ipsas.modules.issue_metadata.validators import looks_like_edn

    codes_elems = root.xpath(".//*[local-name()='codes']")
    codes = codes_elems[0] if codes_elems else None
    if codes is not None:
        doi_el = codes.xpath(".//*[local-name()='doi']")
        if doi_el and doi_el[0].text and doi_el[0].text.strip():
            identifiers["doi"] = doi_el[0].text.strip()
        edn_el = codes.xpath(".//*[local-name()='edn']")
        if edn_el and edn_el[0].text and edn_el[0].text.strip():
            edn_candidate = edn_el[0].text.strip()
            if looks_like_edn(edn_candidate):
                identifiers["edn"] = edn_candidate
            elif edn_candidate.isdigit():
                identifiers["internal_id"] = edn_candidate
            else:
                identifiers["invalid_edn"] = edn_candidate
    def pub_id_type(node: etree._Element) -> Optional[str]:
        """Значение атрибута pub-id-type (с учётом namespace)."""
        v = node.get("pub-id-type")
        if v:
            return v.strip().lower()
        for key, val in (node.attrib or {}).items():
            if key.split("}")[-1] == "pub-id-type" and val:
                return val.strip().lower()
        return None

    for node in root.xpath(".//*[local-name()='article-id']"):
        text = (node.text or "").strip()
        if not text:
            continue
        pt = pub_id_type(node)
        if pt == "doi" and not identifiers["doi"]:
            identifiers["doi"] = text
        elif pt == "edn" and not identifiers["edn"]:
            if looks_like_edn(text):
                identifiers["edn"] = text
            elif text.isdigit() and not identifiers.get("internal_id"):
                identifiers["internal_id"] = text
            else:
                identifiers["invalid_edn"] = identifiers.get("invalid_edn") or text
        elif pt in {"publisher-id", "other", "articlenum", "artnum"} and not identifiers.get("internal_id"):
            if text.isdigit():
                identifiers["internal_id"] = text
            elif not looks_like_edn(text):
                identifiers["internal_id"] = text

    # Страницы из JATS (только article-meta, не библиография)
    pages_jats: Optional[str] = None
    fpage_vals = root.xpath(
        ".//*[local-name()='article-meta']//*[local-name()='fpage']/text()"
    )
    lpage_vals = root.xpath(
        ".//*[local-name()='article-meta']//*[local-name()='lpage']/text()"
    )
    elocation_vals = root.xpath(
        ".//*[local-name()='article-meta']//*[local-name()='elocation-id']/text()"
    )
    fpage = (fpage_vals[0] or "").strip() if fpage_vals else ""
    lpage = (lpage_vals[0] or "").strip() if lpage_vals else ""
    elocation = (elocation_vals[0] or "").strip() if elocation_vals else ""
    if fpage and lpage:
        pages_jats = f"{fpage}-{lpage}"
    elif fpage:
        pages_jats = fpage
    elif elocation:
        pages_jats = elocation

    # Тип статьи: сначала атрибут <article article-type="...">
    article_type: Optional[str] = None
    root_type = (root.get("article-type") or root.get("article_type") or "").strip()
    if root_type:
        article_type = root_type
    if not article_type:
        for node in root.xpath(".//*[local-name()='subj-group']"):
            if (node.get("subj-group-type") or "").strip().lower() == "article-type":
                subj = node.xpath(".//*[local-name()='subject']/text()")
                if subj and (subj[0] or "").strip():
                    article_type = (subj[0] or "").strip()
                    break

    def extract_publication_date() -> Optional[str]:
        nodes = root.xpath(".//*[local-name()='pub-date']")
        ordered: List[etree._Element] = []
        for pt in ("epub", "ppub"):
            for node in nodes:
                if (node.get("pub-type") or "").strip().lower() == pt:
                    ordered.append(node)
        for node in nodes:
            if node not in ordered:
                ordered.append(node)
        for node in ordered:
            def part(name: str) -> Optional[str]:
                vals = node.xpath(f".//*[local-name()='{name}']/text()")
                return vals[0].strip() if vals and (vals[0] or "").strip() else None

            year = part("year")
            if not year or not re.match(r"^\d{4}$", year):
                continue
            month = part("month") or "1"
            day = part("day") or "1"
            try:
                parsed = date(int(year), int(month), int(day))
                return parsed.isoformat()
            except ValueError:
                continue
        return None

    titles = extract_article_titles()
    aff_data = extract_affiliations()

    def extract_authors() -> Dict[str, object]:
        authors_ru: List[str] = []
        authors_en: List[str] = []
        authors: List[str] = []
        orcids: List[str] = []
        emails: List[str] = []
        has_corresp = False

        def format_name_node(node: etree._Element) -> Optional[str]:
            """Одно <name> / <string-name> → строка ФИО."""
            local = etree.QName(node).localname
            if local == "string-name":
                txt = extract_text(node) or " ".join(
                    t.strip() for t in node.xpath(".//text()") if t and t.strip()
                )
                return re.sub(r"\s+", " ", txt).strip() or None
            # <name>: только прямые/вложенные surname/given этого узла, не siblings
            surname = ""
            given = ""
            for sn in node.xpath(".//*[local-name()='surname']"):
                surname = (sn.text or "").strip()
                if surname:
                    break
            for gn in node.xpath(".//*[local-name()='given-names']"):
                given = (gn.text or "").strip()
                if given:
                    break
            if surname and given:
                return f"{surname} {given}".strip()
            if surname or given:
                return (surname or given).strip() or None
            txt = " ".join(t.strip() for t in node.xpath(".//text()") if t and t.strip())
            return re.sub(r"\s+", " ", txt).strip() or None

        def names_by_lang(contrib: etree._Element) -> Dict[str, str]:
            """
            Из name-alternatives / нескольких name взять RU и EN версии одного автора.
            Раньше брали только первое surname в contrib → часть авторов попадала
            только в RU, часть только в EN.
            """
            by_lang: Dict[str, str] = {}
            # Явные name/string-name (обычно внутри name-alternatives)
            name_nodes = contrib.xpath(
                ".//*[local-name()='name-alternatives']"
                "/*[local-name()='name' or local-name()='string-name']"
                " | ./*[local-name()='name' or local-name()='string-name']"
            )
            if not name_nodes:
                name_nodes = contrib.xpath(
                    ".//*[local-name()='name' or local-name()='string-name']"
                )
            for name_node in name_nodes:
                formatted = format_name_node(name_node)
                if not formatted:
                    continue
                lang = normalize_lang(get_lang_attr(name_node))
                if lang not in {"ru", "en"}:
                    lang = detect_lang(formatted) or "unk"
                if lang in {"ru", "en"}:
                    by_lang.setdefault(lang, formatted)
                else:
                    by_lang.setdefault("unk", formatted)
            # Fallback: нет отдельных name — старый разбор всего contrib
            if not by_lang:
                # Нельзя брать .//surname по всему contrib — смешает alternatives
                formatted = None
                string_names = contrib.xpath(".//*[local-name()='string-name']")
                if string_names:
                    formatted = format_name_node(string_names[0])
                if formatted:
                    lang = detect_lang(formatted) or "unk"
                    if lang in {"ru", "en"}:
                        by_lang[lang] = formatted
                    else:
                        by_lang["unk"] = formatted
            return by_lang

        contribs = root.xpath(
            ".//*[local-name()='article-meta']//*[local-name()='contrib']"
        )
        author_contrib_count = 0
        for contrib in contribs:
            ctype = (contrib.get("contrib-type") or "").strip().lower()
            if ctype and ctype not in {"author", "aut"}:
                continue
            if (contrib.get("corresp") or "").strip().lower() in {"yes", "true", "1"}:
                has_corresp = True

            by_lang = names_by_lang(contrib)
            if not by_lang:
                continue
            author_contrib_count += 1

            if "ru" in by_lang:
                authors_ru.append(by_lang["ru"])
            if "en" in by_lang:
                authors_en.append(by_lang["en"])
            primary = by_lang.get("ru") or by_lang.get("en") or next(iter(by_lang.values()))
            authors.append(primary)

            for cid in contrib.xpath(".//*[local-name()='contrib-id']"):
                cid_type = (cid.get("contrib-id-type") or "").strip().lower()
                val = (cid.text or "").strip()
                if not val:
                    continue
                if "orcid" in cid_type or "orcid.org" in val.lower():
                    m = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dXx])", val)
                    orcids.append(m.group(1) if m else val)
            for em in contrib.xpath(".//*[local-name()='email']"):
                val = (em.text or "").strip()
                if val and val not in emails:
                    emails.append(val)

        # Уникальные с сохранением порядка
        def uniq(vals: List[str]) -> List[str]:
            seen: set[str] = set()
            out: List[str] = []
            for v in vals:
                if v and v not in seen:
                    seen.add(v)
                    out.append(v)
            return out

        authors_ru = uniq(authors_ru)
        authors_en = uniq(authors_en)
        authors = uniq(authors) or authors_ru or authors_en
        # Количество авторов = число contrib author, не длина одного языкового списка
        count = author_contrib_count or len(authors) or len(authors_ru) or len(authors_en)
        return {
            "authors_ru": authors_ru,
            "authors_en": authors_en,
            "authors": authors,
            "orcids": uniq(orcids),
            "emails": uniq(emails),
            "has_corresponding_author": has_corresp or None,
            "authors_count": count,
        }

    authors_data = extract_authors()
    data = {
        "abstract_ru": collect_abstract("ru"),
        "abstract_en": collect_abstract("en"),
        "keywords_ru": collect_keywords("ru"),
        "keywords_en": collect_keywords("en"),
        "title_ru": titles.get("title_ru"),
        "title_en": titles.get("title_en"),
        "identifiers": identifiers,
        "pages_jats": pages_jats,
        "article_type": article_type,
        "journal_meta": extract_journal_meta(),
        "references_by_lang": collect_references_by_lang(),
        "publication_date": extract_publication_date(),
        "jats_affiliations": aff_data.get("jats_affiliations") or [],
        "contributor_affiliation_refs": aff_data.get("contributor_affiliation_refs") or [],
        "broken_affiliation_refs": aff_data.get("broken_affiliation_refs") or [],
        **authors_data,
    }
    if recovered:
        warnings = data.get("warnings")
        if not isinstance(warnings, list):
            warnings = []
        warnings.append({
            "text": "JATS XML содержит синтаксические ошибки: данные извлечены в режиме восстановления (recover=True) и могут быть неполными/искажёнными.",
            "severity": "warning",
            "field": "jats_xml",
        })
        data["warnings"] = warnings
    return data

