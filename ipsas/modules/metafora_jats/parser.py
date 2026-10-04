"""Извлечение Publication из JATS Article XML."""

from __future__ import annotations

from lxml import etree

from ipsas.common.xml_secure import create_secure_parser
from ipsas.modules.metafora_jats.models import Author, Publication, TitleEntry

NS_XML = "http://www.w3.org/XML/1998/namespace"
NS_XLINK = "http://www.w3.org/1999/xlink"


def local_name(el: etree._Element) -> str:
    if not isinstance(el.tag, str):
        return ""
    return etree.QName(el).localname


def _text(el: etree._Element | None) -> str:
    if el is None:
        return ""
    parts = [t.strip() for t in el.itertext() if t and t.strip()]
    return " ".join(parts).strip()


def _lang(el: etree._Element) -> str:
    """Язык элемента или ближайшего предка (часто у trans-title-group)."""
    cur: etree._Element | None = el
    while cur is not None:
        val = (
            cur.get(f"{{{NS_XML}}}lang") or cur.get("lang") or ""
        ).strip().lower()
        if val:
            return val
        cur = cur.getparent()
    return ""


def _find_meta(root: etree._Element) -> etree._Element | None:
    for el in root.iter():
        if local_name(el) == "article-meta":
            return el
    return None


def parse_secure_bytes(xml_bytes: bytes) -> etree._Element:
    """Строгий XXE-safe разбор; бросает ``etree.XMLSyntaxError``."""
    parser = create_secure_parser(recover=False, huge_tree=False)
    return etree.fromstring(xml_bytes, parser=parser)


def is_jats_article(root: etree._Element) -> bool:
    return local_name(root) == "article"


def parse_publication(root: etree._Element, *, filename: str = "") -> Publication:
    """Преобразовать корень ``<article>`` во внутреннюю модель."""
    pub = Publication(source_filename=filename)
    pub.publication_type = (root.get("article-type") or "").strip()
    pub.publication_type_xpath = "/article/@article-type"

    meta = _find_meta(root)

    # Titles
    title_group = None
    if meta is not None:
        for el in meta:
            if local_name(el) == "title-group":
                title_group = el
                break
    search_root = title_group if title_group is not None else (meta or root)
    for el in search_root.iter():
        ln = local_name(el)
        if ln in {"article-title", "trans-title"}:
            text = _text(el)
            if text:
                pub.titles.append(
                    TitleEntry(
                        text=text,
                        lang=_lang(el),
                        xpath=f".//{ln}",
                    )
                )

    # pub-date date-type=pub
    if meta is not None:
        for el in meta.iter():
            if local_name(el) != "pub-date":
                continue
            date_type = (el.get("date-type") or "").strip().lower()
            if date_type and date_type != "pub":
                continue
            # без date-type тоже принимаем как кандидат, если нет явного pub
            iso = (el.get("iso-8601-date") or "").strip()
            if iso:
                pub.publication_date_raw = iso
                pub.publication_date_xpath = (
                    '/article/front/article-meta/pub-date[@date-type="pub"]'
                    '/@iso-8601-date'
                )
                break
            year = month = day = ""
            for child in el:
                cn = local_name(child)
                if cn == "year":
                    year = (child.text or "").strip()
                elif cn == "month":
                    month = (child.text or "").strip()
                elif cn == "day":
                    day = (child.text or "").strip()
            if year and month and day:
                pub.publication_date_raw = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            elif year and month:
                pub.publication_date_raw = f"{year}-{month.zfill(2)}"
            elif year:
                pub.publication_date_raw = year
            if pub.publication_date_raw:
                pub.publication_date_xpath = (
                    '/article/front/article-meta/pub-date[@date-type="pub"]'
                )
                break
        # второй проход: pub-date без date-type, если ещё пусто
        if not pub.publication_date_raw:
            for el in meta.iter():
                if local_name(el) != "pub-date":
                    continue
                if (el.get("date-type") or "").strip():
                    continue
                iso = (el.get("iso-8601-date") or "").strip()
                year = month = day = ""
                for child in el:
                    cn = local_name(child)
                    if cn == "year":
                        year = (child.text or "").strip()
                    elif cn == "month":
                        month = (child.text or "").strip()
                    elif cn == "day":
                        day = (child.text or "").strip()
                if iso:
                    pub.publication_date_raw = iso
                elif year and month and day:
                    pub.publication_date_raw = (
                        f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    )
                elif year:
                    pub.publication_date_raw = year
                if pub.publication_date_raw:
                    pub.publication_date_xpath = (
                        "/article/front/article-meta/pub-date"
                    )
                    break

    # pages / elocation
    if meta is not None:
        for el in meta.iter():
            ln = local_name(el)
            if ln == "fpage" and not pub.fpage:
                pub.fpage = (el.text or "").strip()
            elif ln == "lpage" and not pub.lpage:
                pub.lpage = (el.text or "").strip()
            elif ln == "page-range" and not pub.page_range:
                pub.page_range = (el.text or "").strip()
            elif ln == "elocation-id" and not pub.elocation_id:
                pub.elocation_id = (el.text or "").strip()
        if pub.fpage or pub.lpage:
            pub.pagination_xpath = "/article/front/article-meta/fpage|lpage"
        elif pub.page_range:
            pub.pagination_xpath = "/article/front/article-meta/page-range"
        elif pub.elocation_id:
            pub.pagination_xpath = "/article/front/article-meta/elocation-id"

    # identifiers
    if meta is not None:
        for el in meta.iter():
            if local_name(el) != "article-id":
                continue
            pid = (el.get("pub-id-type") or "").strip().lower()
            val = (el.text or "").strip()
            if pid == "doi":
                pub.doi_declared = True
                pub.doi_xpath = (
                    '/article/front/article-meta/article-id[@pub-id-type="doi"]'
                )
                if val and not pub.doi:
                    pub.doi = val
            elif pid == "edn" and not pub.edn:
                pub.edn = val
                pub.edn_xpath = (
                    '/article/front/article-meta/article-id[@pub-id-type="edn"]'
                )

    # affiliations map: id → есть непустой текст
    aff_by_id: dict[str, bool] = {}
    scan_aff = meta if meta is not None else root
    for el in scan_aff.iter():
        ln = local_name(el)
        if ln not in {"aff", "aff-alternatives"}:
            continue
        aff_id = (el.get("id") or "").strip()
        if not aff_id:
            continue
        aff_by_id[aff_id] = bool(_text(el))

    # authors
    contrib_group_nodes: list[etree._Element] = []
    scan = meta if meta is not None else root
    for el in scan.iter():
        if local_name(el) == "contrib" and (
            el.get("contrib-type") or ""
        ).strip().lower() == "author":
            contrib_group_nodes.append(el)

    for contrib in contrib_group_nodes:
        author = Author(xpath=".//contrib[@contrib-type='author']")
        # name-alternatives or direct name
        names: list[etree._Element] = []
        for child in contrib.iter():
            if local_name(child) == "name":
                names.append(child)
        # prefer first non-empty surname/given from any name
        for name_el in names:
            surname = given = ""
            for child in name_el:
                cn = local_name(child)
                if cn == "surname":
                    surname = (child.text or "").strip()
                elif cn in {"given-names", "given-name"}:
                    given = (child.text or "").strip()
            if surname or given:
                if not author.surname:
                    author.surname = surname
                if not author.given_names:
                    author.given_names = given
        for child in contrib.iter():
            if local_name(child) == "contrib-id" and (
                child.get("contrib-id-type") or ""
            ).strip().lower() == "orcid":
                author.orcid = (child.text or "").strip()
                break

        # аффилиации: xref ref-type=aff или вложенный aff
        rids: list[str] = []
        for child in contrib.iter():
            if local_name(child) != "xref":
                continue
            if (child.get("ref-type") or "").strip().lower() != "aff":
                continue
            for rid in (child.get("rid") or "").split():
                rid = rid.strip()
                if rid and rid not in rids:
                    rids.append(rid)
        author.affiliation_ids = rids
        has_linked = any(aff_by_id.get(rid, False) for rid in rids)
        has_inline = False
        for child in contrib:
            if local_name(child) in {"aff", "aff-alternatives"} and _text(child):
                has_inline = True
                break
        author.has_affiliation = has_linked or has_inline
        pub.authors.append(author)

    # bibliography: back/ref-list/ref
    ref_count = 0
    for el in root.iter():
        if local_name(el) != "ref-list":
            continue
        for child in el.iter():
            if local_name(child) == "ref":
                ref_count += 1
        pub.references_xpath = "/article/back/ref-list"
    pub.reference_count = ref_count

    # ссылка на статью: self-uri (xlink:href или текст)
    scan_uri = meta if meta is not None else root
    for el in scan_uri.iter():
        if local_name(el) != "self-uri":
            continue
        href = (
            el.get(f"{{{NS_XLINK}}}href")
            or el.get("href")
            or _text(el)
            or ""
        ).strip()
        if href.lower().startswith(("http://", "https://")):
            pub.article_url = href
            break

    return pub
