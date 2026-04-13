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
