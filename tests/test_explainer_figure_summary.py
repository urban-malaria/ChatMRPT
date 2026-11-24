"""The Explain button describes workflow charts from the chart's own data."""
import pickle

import plotly.graph_objects as go

from app.visualization.explainer import figure_summary


def _figure():
    figure = go.Figure([go.Bar(name='RDT Tests', x=['Primary', 'Secondary'], y=[771209, 53294])])
    figure.update_layout(title='Test availability by level', yaxis_title='Tests')
    return figure


def test_reads_html_with_embedded_plotly_library(tmp_path):
    path = tmp_path / 'chart.html'
    _figure().write_html(str(path), include_plotlyjs=True)
    lines = figure_summary(str(path))
    assert '- Chart title: Test availability by level' in lines
    assert any('Primary: 771209' in line and 'Secondary: 53294' in line for line in lines)


def test_reads_pickled_histogram_and_counts_missing(tmp_path):
    path = tmp_path / 'chart.pickle'
    with open(path, 'wb') as handle:
        pickle.dump(go.Figure([go.Histogram(name='tested', x=[0, 0, 5, float('nan'), 991])]), handle)
    lines = figure_summary(str(path))
    assert any('4 values (1 missing), min 0, median 2.5, max 991' in line for line in lines)


def test_unreadable_file_gives_no_lines(tmp_path):
    path = tmp_path / 'broken.html'
    path.write_text('<html>no chart</html>')
    assert figure_summary(str(path)) == []
