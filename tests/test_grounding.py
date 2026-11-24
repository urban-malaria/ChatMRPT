"""Answers that quote data must come from a tool call in the same turn."""
import pandas as pd
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from app.agent.grounding import quotes_data, tool_used_this_turn, ward_names

DATA = [{'variable_name': 'df', 'data': pd.DataFrame({'WardName': ['Okeso', 'Ogele', 'Pategi 1', 'Are']})}]


def test_ward_names_ignores_short_names():
    assert ward_names(DATA) == {'okeso', 'ogele', 'pategi 1'}


def test_reply_naming_two_wards_quotes_data():
    names = ward_names(DATA)
    assert quotes_data("1. **Okeso** - 0.069\n2. **Ogele** - 0.071", names)
    assert not quotes_data("Okeso is a ward in Asa LGA.", names)


def test_three_decimal_figures_quote_data():
    assert quotes_data("Scores were 0.51, 0.49 and 0.48.", set())
    assert not quotes_data("Type yes to continue, or ask about version 2.", set())


def test_tool_use_is_counted_from_the_latest_user_message():
    earlier_tool = [HumanMessage(content='run risk analysis'),
                    AIMessage(content='', tool_calls=[{'name': 'run_risk_analysis', 'args': {}, 'id': '1'}]),
                    ToolMessage(content='done', tool_call_id='1'),
                    AIMessage(content='Analysis complete.'),
                    HumanMessage(content='show me the lowest risk wards')]
    assert not tool_used_this_turn(earlier_tool)
    this_turn = earlier_tool + [AIMessage(content='', tool_calls=[{'name': 'get_ward_rankings', 'args': {}, 'id': '2'}]),
                                ToolMessage(content='table', tool_call_id='2')]
    assert tool_used_this_turn(this_turn)
