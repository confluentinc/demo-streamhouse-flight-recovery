"""The recovery agent's SQL, Terraform, and deploy step agree on names and columns."""

import re
from pathlib import Path

from scripts import deploy, setup_rtce

DEMO = Path(__file__).parent.parent / "terraform" / "airline-demo"
SQL = DEMO / "sql"


def _sql(prefix: str) -> str:
    return next(SQL.glob(f"{prefix}-*.sql")).read_text()


def test_statements_chain_by_name():
    assert "CREATE MODEL IF NOT EXISTS passenger_recovery_model" in _sql("27")
    assert f"USING CONNECTION `{setup_rtce.AGENT_CONNECTION}`" in _sql("28")
    agent = _sql("29")
    assert "CREATE AGENT IF NOT EXISTS passenger_recovery_agent" in agent
    assert "USING MODEL passenger_recovery_model" in agent
    assert "USING TOOLS live_context" in agent
    assert "AI_RUN_AGENT(\n    'passenger_recovery_agent'" in _sql("30")
    assert "FROM passenger_state_changes" in _sql("30")


def test_insert_writes_every_recommendation_column_in_order():
    table = re.findall(r"^  `?(\w+)`? [A-Z]", _sql("23"), re.M)
    final_select = _sql("30").rsplit("\nSELECT\n", 1)[1].split("\nFROM offers")[0]
    written = [line.strip().rstrip(",").split()[-1].strip("`") for line in final_select.splitlines()]
    assert written == [c for c in table if c != "PRIMARY"]


def test_agent_reply_contract_matches_the_parser():
    fields = set(re.findall(r"O[12]_(\w+)=", _sql("29")))
    parsed = set(re.findall(r"CONCAT\('O', o\.n, '_(\w+)=", _sql("30")))
    assert fields == parsed == {"FLIGHT", "HOTEL", "COST"}


def test_deploy_targets_exist_and_sql_files_exist():
    main = (DEMO / "main.tf").read_text()
    for target in deploy.AGENT_TARGETS:
        kind, name = target.split(".")
        assert f'resource "{kind}" "{name}"' in main
    for path in re.findall(r'"(sql/[\w-]+\.sql)"', main):
        assert (DEMO / path).exists(), path
