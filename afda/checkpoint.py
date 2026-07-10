"""
checkpoint — AFDA 流水线断点续跑支持
=====================================
记录和恢复流水线执行进度。

用法：
    from afda.checkpoint import Checkpoint

    cp = Checkpoint(results_dir)
    cp.mark_done("step1_convert_xls_to_csv")
    cp.is_done("step1_convert_xls_to_csv")  # True
    cp.pending_steps(all_steps)  # ["step2_check_statements", ...]
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable

from afda.logging_config import get_logger

logger = get_logger(__name__)

_CHECKPOINT_FILENAME = ".pipeline_checkpoint.json"


class Checkpoint:
    """流水线检查点管理器。

    检查点文件存储在 results 目录下，记录已完成的步骤。
    """

    def __init__(self, results_dir: Path) -> None:
        self._results_dir = results_dir
        self._path = results_dir / _CHECKPOINT_FILENAME
        self._data: dict = self._load()

    def _load(self) -> dict:
        """从磁盘加载检查点数据。"""
        if self._path.exists():
            try:
                return json.loads(self._path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("检查点文件损坏，将忽略：%s", exc)
        return {"steps_completed": {}}

    def _save(self) -> None:
        """将检查点数据写入磁盘。"""
        self._results_dir.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def is_done(self, step_name: str) -> bool:
        """检查某步骤是否已完成。"""
        return step_name in self._data.get("steps_completed", {})

    def mark_done(self, step_name: str) -> None:
        """标记某步骤为已完成并保存。"""
        if "steps_completed" not in self._data:
            self._data["steps_completed"] = {}
        self._data["steps_completed"][step_name] = datetime.now().isoformat()
        self._save()
        logger.debug("检查点已更新：%s 完成", step_name)

    def pending_steps(self, all_steps: Iterable[str]) -> list[str]:
        """返回尚未完成的步骤列表（保持原始顺序）。"""
        return [s for s in all_steps if not self.is_done(s)]

    def clear(self) -> None:
        """清除检查点数据（覆写为空状态）。"""
        self._data = {"steps_completed": {}}
        if self._path.exists():
            self._path.write_text(
                json.dumps(self._data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        logger.info("已清除检查点数据")

    def completed_count(self) -> int:
        """返回已完成的步骤数。"""
        return len(self._data.get("steps_completed", {}))
