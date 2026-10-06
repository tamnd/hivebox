"""The verl adapter against a real comb, with verl installed. Set HIVE_TEST_ENDPOINT and
HIVE_TEST_GIT_IMAGE as for test_live.py. The tools are called through verl's own tool dispatch,
and the model is a script of tool calls, since a real one needs a GPU."""

import json
import os
import types
import uuid

import pytest

verl = pytest.importorskip("verl")

from verl.experimental.agent_loop.agent_loop import AgentLoopMetrics, AgentLoopOutput  # noqa: E402
from verl.experimental.agent_loop.tool_agent_loop import ToolAgentLoop  # noqa: E402
from verl.experimental.agent_loop.tool_parser import FunctionCall  # noqa: E402

import hivebox  # noqa: E402
from hivebox.verl import BashTool, EditorTool, SubmitTool  # noqa: E402
from hivebox.verl.loop import HiveAgentLoop  # noqa: E402

ENDPOINT = os.environ.get("HIVE_TEST_ENDPOINT")
GIT_IMAGE = os.environ.get("HIVE_TEST_GIT_IMAGE")
WORKDIR = os.environ.get("HIVE_TEST_GIT_WORKDIR", "/testbed")
pytestmark = pytest.mark.skipif(not (ENDPOINT and GIT_IMAGE), reason="set HIVE_TEST_ENDPOINT and HIVE_TEST_GIT_IMAGE")

CONFIG = {
    "type": "native",
    "endpoint": ENDPOINT,
    "project": "verl-test",
    "task": {
        "image": GIT_IMAGE,
        "workdir": WORKDIR,
        "cell": {"mem_mib": 512, "vcpu_milli": 1000},
        "limits": {"command_timeout_s": 60, "max_wall_s": 600},
        "verify": {
            "argv": ["bash", "-c", "grep -qx 'value = 2' probe.py && test ! -e guarded.txt && grep -q 7 hidden.txt"],
            "protected_paths": ["guarded.txt"],
            "files": {"hidden.txt": "7\n"},
        },
    },
}


def loop_with_tools(cls=ToolAgentLoop, config=CONFIG):
    # Only what _call_tool reads, so verl's own dispatch runs without a model or a trainer.
    loop = cls.__new__(cls)
    tools = [BashTool(config), EditorTool(config), SubmitTool(config)]
    loop.tools = {t.name: t for t in tools}
    loop.max_tool_response_length = 100_000
    loop.tool_response_truncate_side = "middle"
    return loop


async def call(loop, agent_data, name, **args):
    response, reward, metrics = await loop._call_tool(FunctionCall(name=name, arguments=json.dumps(args)), {}, agent_data)
    return response.text, reward, metrics


def agent_data():
    return types.SimpleNamespace(request_id=uuid.uuid4().hex, extra_fields={})


async def test_the_tools_share_a_cell_and_a_shell_and_submit_verifies():
    loop = loop_with_tools()
    a = agent_data()
    text, _, m = await call(loop, a, "bash", command="printf 'value = 1\\n' > probe.py && cd /tmp && export X=7")
    assert m["exit_code"] == 0, text
    text, _, _ = await call(loop, a, "bash", command="echo $PWD $X")
    assert text == "/tmp 7\n"
    text, _, _ = await call(loop, a, "str_replace_editor", command="view", path="probe.py")
    assert text == "     1\tvalue = 1"
    text, _, _ = await call(loop, a, "str_replace_editor", command="str_replace", path="probe.py", old_str="value = 1", new_str="value = 9")
    assert text.startswith("Edited")
    text, _, _ = await call(loop, a, "str_replace_editor", command="str_replace", path="probe.py", old_str="value = 1", new_str="x")
    assert "appears 0 times" in text
    text, _, _ = await call(loop, a, "str_replace_editor", command="undo_edit", path="probe.py")
    assert text == f"Took back the last edit to {WORKDIR}/probe.py."
    await call(loop, a, "str_replace_editor", command="str_replace", path="probe.py", old_str="value = 1", new_str="value = 2")
    text, _, _ = await call(loop, a, "str_replace_editor", command="view", path=f"{WORKDIR}/probe.py", view_range=[1, 1])
    assert text == "     1\tvalue = 2"
    text, _, _ = await call(loop, a, "str_replace_editor", command="view", path="missing.py")
    assert "missing.py" in text
    text, _, _ = await call(loop, a, "bash", command="exit 3")
    assert text.endswith("(exit code 3)")

    text, reward, fields = await call(loop, a, "submit")
    assert (text, reward) == ("Your changes were submitted.", 1.0)
    assert fields["hive_passed"] and not fields["hive_infra_error"] and fields["hive_tampered"] == 0
    text, reward, _ = await call(loop, a, "submit")
    assert (text, reward) == ("Your changes were already submitted.", 0.0)
    # The cell was stopped by submit, and a call after it does not make a new one.
    text, _, _ = await call(loop, a, "bash", command="true")
    assert text.startswith("The command could not be run")


async def test_a_command_that_times_out_gets_a_new_shell():
    config = {**CONFIG, "task": {**CONFIG["task"], "limits": {"command_timeout_s": 1}}}
    loop = loop_with_tools(config=config)
    a = agent_data()
    text, _, _ = await call(loop, a, "bash", command="sleep 5")
    assert text.endswith("(the command was stopped after 1 s)")
    text, _, m = await call(loop, a, "bash", command="echo $PWD")
    assert (text, m["exit_code"]) == (WORKDIR + "\n", 0)
    _, reward, fields = await call(loop, a, "submit")
    assert reward == 0.0 and not fields["hive_passed"]


async def run_scripted(script, **task):
    loop = loop_with_tools(HiveAgentLoop)
    seen = {}

    async def fake_run(self, sampling_params, **kwargs):
        # Stands in for the model: the scripted tool calls, then an output as ToolAgentLoop makes it.
        a = agent_data()
        for name, args in script:
            seen.setdefault("texts", []).append((await call(self, a, name, **args))[0])
        seen["cell"] = hivebox.verl._trajectory.current.get().cell
        return AgentLoopOutput(prompt_ids=[1], response_ids=[2], response_mask=[1], metrics=AgentLoopMetrics(), extra_fields={})

    orig, ToolAgentLoop.run = ToolAgentLoop.run, fake_run
    try:
        out = await loop.run({}, extra_info={"hive": task, "index": 3})
    finally:
        ToolAgentLoop.run = orig
    return out, seen


async def test_the_loop_rewards_and_stops_each_trajectory():
    write = ("bash", {"command": "printf 'value = 2\\n' > probe.py"})
    out, seen = await run_scripted([write, ("submit", {})], task_id="t-pass")
    assert out.reward_score == 1.0 and out.extra_fields["hive_submitted"] and out.extra_fields["hive_passed"]
    assert (await seen["cell"].refresh()).state == "stopped"

    # Not submitted, so checked when the loop ends.
    out, seen = await run_scripted([write], task_id="t-unsubmitted")
    assert out.reward_score == 1.0 and not out.extra_fields["hive_submitted"]

    out, _ = await run_scripted([write, ("bash", {"command": "echo x > guarded.txt"}), ("submit", {})], task_id="t-tamper")
    assert out.reward_score == 0.0 and out.extra_fields["hive_tampered"] == 1

    out, _ = await run_scripted([("bash", {"command": "printf 'value = 3\\n' > probe.py"})], task_id="t-wrong")
    assert out.reward_score == 0.0 and not out.extra_fields["hive_passed"]

    # The sample's own task replaces the hidden file, so the same change now fails.
    out, _ = await run_scripted([write], task_id="t-hidden", verify={"files": {"hidden.txt": "8\n"}})
    assert out.reward_score == 0.0


async def test_a_task_whose_image_is_missing_gets_nothing_and_says_why():
    out, _ = await run_scripted([("bash", {"command": "true"})], task_id="t-infra", image="no-such-image")
    assert out.reward_score == 0.0
    f = out.extra_fields
    assert f["hive_cell"] == "" and f["hive_error"], f
