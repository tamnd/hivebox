"""HiveAgentLoop: verl's tool agent loop with a hivebox cell for each trajectory."""

from __future__ import annotations

import logging
from typing import Any

from verl.experimental.agent_loop.agent_loop import AgentLoopOutput, register
from verl.experimental.agent_loop.tool_agent_loop import ToolAgentLoop

from . import _trajectory

logger = logging.getLogger(__name__)


@register("hive_agent")
class HiveAgentLoop(ToolAgentLoop):
    """Runs verl's tool agent loop with the hivebox tools sharing one cell, then checks the cell's
    changes, if `submit` did not already, sets the sample's `reward_score` and stops the cell.

    The task is the hivebox tools' configured `task`, with the sample's `extra_info["hive"]` on
    top, so a dataset row carries only what differs, like its image and hidden tests. A sample
    hivebox failed gets a reward of 0 and `hive_infra_error` set in its extra fields, for the
    trainer to mask."""

    async def run(self, sampling_params: dict[str, Any], **kwargs) -> AgentLoopOutput:
        config = self._hive_config()
        extra = kwargs.get("extra_info") or {}
        task = _trajectory.task(config, extra.get("hive"))
        key = f"{task.get('task_id') or extra.get('index', '')}-{id(self):x}"
        t = _trajectory.Trajectory(_trajectory.client(config), task, key)
        token = _trajectory.current.set(t)
        try:
            output = await super().run(sampling_params, **kwargs)
            if task.get("verify_unsubmitted", True) or t.submitted:
                await t.verify()
            reward = t.reward
            output.reward_score = 0.0 if reward is None else reward
            output.extra_fields.update(t.fields())
            return output
        finally:
            _trajectory.current.reset(token)
            await t.close()

    def _hive_config(self) -> dict[str, Any]:
        """The config of the first hivebox tool, where the endpoint and the task defaults are."""
        for tool in self.tools.values():
            if tool.__class__.__module__.startswith("hivebox.verl"):
                return tool.config
        raise ValueError("HiveAgentLoop needs the hivebox tools, such as hivebox.verl.BashTool, in the tool config")
