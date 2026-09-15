"""P1-4 建议冷却指纹去重 —— 单元测试。

验证 should_emit 按"结构指纹"去重（绝不用 description/title），
且命中冷却窗口时抑制重复推送；不同结构方案仍可推送。
"""
from types import SimpleNamespace
import pytest

from src.analysis import advisor as advisor_mod
from src.analysis.advisor import SmartAdvisor


def _trade(code, direction, target_weight):
    return SimpleNamespace(code=code, direction=direction,
                            target_weight=target_weight, current_weight=0.0)


def _plan(trades, turnover, related_codes=None):
    return SimpleNamespace(
        trades=trades,
        turnover=turnover,
        related_codes=related_codes if related_codes is not None
        else [t.code for t in trades],
        action_needed=True,
    )


@pytest.fixture(autouse=True)
def _reset_buffer():
    # 隔离：每个用例前后清空模块级环形缓冲，避免跨用例污染
    advisor_mod._RECENT_PLAN_FINGERPRINTS.clear()
    yield
    advisor_mod._RECENT_PLAN_FINGERPRINTS.clear()


def test_should_emit_blocks_repeated_structural_plan():
    adv = SmartAdvisor(None)
    plan = _plan([_trade("510300", "买入", 0.2),
                  _trade("159915", "卖出", 0.1)], turnover=0.15)

    assert adv.should_emit(plan) is True          # 首次：应推送
    adv._record_emit(plan)
    assert adv.should_emit(plan) is False         # 冷却窗口内：抑制


def test_should_emit_allows_different_structure():
    adv = SmartAdvisor(None)
    plan_a = _plan([_trade("510300", "买入", 0.2)], turnover=0.15)
    plan_b = _plan([_trade("510300", "买入", 0.3)], turnover=0.35)  # 结构不同

    assert adv.should_emit(plan_a) is True
    adv._record_emit(plan_a)
    assert adv.should_emit(plan_b) is True        # 新结构：仍推送


def test_fingerprint_ignores_description_noise():
    # 同一决策、不同文本/顺序，指纹必须相同 -> 仍被冷却
    adv = SmartAdvisor(None)
    p1 = _plan([_trade("510300", "买入", 0.2),
                _trade("159915", "卖出", 0.1)], turnover=0.15)
    p2 = _plan([_trade("159915", "卖出", 0.1),
                _trade("510300", "买入", 0.2)], turnover=0.15)  # 顺序不同
    assert advisor_mod.SmartAdvisor._plan_fingerprint(p1) == \
        advisor_mod.SmartAdvisor._plan_fingerprint(p2)

    assert adv.should_emit(p1) is True
    adv._record_emit(p1)
    assert adv.should_emit(p2) is False            # 结构相同(仅顺序不同) -> 抑制


def test_generate_rebalance_plan_wraps_cooling():
    # generate_rebalance_plan 在 action_needed 后包裹 should_emit：
    # 连续两次相同结构 -> 第二次返回 None（被冷却抑制）
    import sys, types
    import src.analysis.advisor as adv_mod

    class FakePlan:
        action_needed = True
        turnover = 0.15
        trades = [_trade("510300", "买入", 0.2)]

    # 用 sys.modules 注入假引擎模块，避免重依赖；测试后还原
    mod_name = "src.analysis.rebalance_engine"
    saved = sys.modules.get(mod_name)
    fake_mod = types.ModuleType(mod_name)
    fake_mod.compute_rebalance_suggestion = \
        staticmethod(lambda db, as_of_date=None, strategy="layered": FakePlan())
    sys.modules[mod_name] = fake_mod

    adv = SmartAdvisor(None)
    try:
        r1 = adv.generate_rebalance_plan()
        r2 = adv.generate_rebalance_plan()
    finally:
        if saved is None:
            sys.modules.pop(mod_name, None)
        else:
            sys.modules[mod_name] = saved

    assert r1 is not None
    assert r2 is None          # 第二次相同结构被冷却抑制
