from ipsas.web.app import create_app


def test_xml_report_redirects_to_validator():
    app = create_app(testing=True)
    client = app.test_client()
    response = client.get("/services/xml-report", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/services/xml-validator")


def test_xml_validator_page_combined():
    app = create_app(testing=True)
    client = app.test_client()
    response = client.get("/services/xml-validator")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Валидатор XML" in html
    assert "check_schema" in html
    assert "check_metadata" in html
