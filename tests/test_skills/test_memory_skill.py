from __future__ import annotations

from skills.db_manager import DatabaseManager
from skills.strategic_memory.skill import StrategicMemorySkill


def test_write_read_round_trip(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    s = StrategicMemorySkill(db)
    brief_id = s.write_brief("task-1", "Title", "Summary")
    out = s.read_brief(brief_id)
    assert out["brief_id"] == brief_id
    assert out["title"] == "Title"
    assert out["task_id"] == "task-1"
    assert out["actionability"] == "INFORMATIONAL"


def test_list_briefs_preserves_tags_and_provenance(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    s = StrategicMemorySkill(db)
    brief_id = s.write_brief(
        "task-2",
        "Tagged Brief",
        "Summary",
        tags=["ops", "runtime"],
        provenance_links=["source-1"],
        actionability="ACTION_RECOMMENDED",
    )

    rows = s.list_briefs(task_id="task-2", actionability="ACTION_RECOMMENDED")

    assert rows[0]["brief_id"] == brief_id
    assert rows[0]["tags"] == ["ops", "runtime"]
    assert rows[0]["provenance_links"] == ["source-1"]
