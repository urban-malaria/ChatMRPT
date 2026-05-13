from app.whatsapp.formatter import chunk_text, clean_whatsapp_text


def test_clean_whatsapp_text_normalizes_risk_analysis_command():
    text = 'Run *"malaria risk analysis"* to generate vulnerability rankings for any year.'

    cleaned = clean_whatsapp_text(text)

    assert cleaned == "Type: run malaria risk analysis to generate vulnerability rankings for any year."


def test_chunk_text_preserves_useful_line_breaks():
    text = "Analysis complete.\n\nWhat you can do next\n\n1. Plan ITN\n   Type: export results"

    chunks = chunk_text(text)

    assert chunks == [text]
