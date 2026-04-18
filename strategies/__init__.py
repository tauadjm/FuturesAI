"""
strategies/__init__.py — 策略注册表

通过环境变量 STRATEGY_ID 选择活跃策略（默认 "price_action"）。
新策略：
  1. 新建 strategies/my_strategy.py，继承 BaseStrategy
  2. 在 _registry 中注册
  3. .env 设置 STRATEGY_ID=my_strategy，重启即切换
"""

import os
from strategies.base import BaseStrategy
from strategies.price_action import BrooksStrategy

_registry: dict[str, BaseStrategy] = {
    "price_action": BrooksStrategy(),
}

_strategy_id = os.getenv("STRATEGY_ID", "price_action")
if _strategy_id not in _registry:
    raise ValueError(
        f"STRATEGY_ID={_strategy_id!r} 未注册，可用策略：{list(_registry)}"
    )
_active: BaseStrategy = _registry[_strategy_id]


def get_strategy() -> BaseStrategy:
    """返回当前活跃策略实例（单例）。"""
    return _active


def get_strategy_by_id(strategy_id: str) -> BaseStrategy:
    """按 ID 返回策略实例；ID 不存在时返回活跃策略。"""
    return _registry.get(strategy_id, _active)


def list_strategies() -> list[str]:
    """返回所有已注册策略 ID 列表。"""
    return list(_registry.keys())
