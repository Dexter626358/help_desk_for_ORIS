from pathlib import Path

from ipsas.modules.reference_processor import remove_reference_numbering


def test_remove_numbering_in_refinfo_text(tmp_path):
    xml_content = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<root>
  <references>
    <reference>
      <refInfo lang=\"RUS\">
        <text>1. Borisov A., Sokolov I. Optimal filtering of Markov jump processes given observations with state-dependent noises: Exact solution and stable numerical schemes // Mathematics. 2020. vol. 8. no. 4. </text>
      </refInfo>
    </reference>
  </references>
</root>
"""

    xml_path = tmp_path / "sample.xml"
    xml_path.write_text(xml_content, encoding="utf-8")

    result = remove_reference_numbering(xml_path)

    assert result["success"] is True
    assert result["processed_count"] == 1

    output_text = Path(result["output_path"]).read_text(encoding="utf-8")
    assert "<text>Borisov A., Sokolov I." in output_text
    assert "<text>1. Borisov A., Sokolov I." not in output_text
