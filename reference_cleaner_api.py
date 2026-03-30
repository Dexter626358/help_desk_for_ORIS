from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, UploadFile
from fastapi.responses import Response
from lxml import etree

from ipsas.modules.reference_cleaner import clean_references


def _create_strict_parser() -> etree.XMLParser:
    return etree.XMLParser(
        recover=False,
        remove_blank_text=False,
        resolve_entities=False,
        huge_tree=True,
    )


app = FastAPI(
    title="ReferenceCleaner XML Service",
    description="Cleans <references>/<reference> blocks and normalizes refinfo/text.",
    version="1.0.0",
)


@app.post("/process")
async def process(file: UploadFile) -> Response:
    data = await file.read()
    parser = _create_strict_parser()
    tree = etree.fromstring(data, parser)  # type: ignore[arg-type]
    xml_tree = etree.ElementTree(tree)

    clean_references(xml_tree)

    out_bytes = etree.tostring(
        xml_tree,
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=False,
    )

    in_name = Path(file.filename or "input.xml").name
    out_name = f"{Path(in_name).stem}_processed.xml"
    return Response(
        content=out_bytes,
        media_type="application/xml",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
    )

