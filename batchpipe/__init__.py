"""Batchpipe - 流式批处理流水线

使用 collections 和 itertools 构建可组合的流式批处理管道。

核心组件:
- Pipeline: 流水线主类，支持阶段组合
- Stages: 内置处理阶段（规范化、聚合、滑动窗口、格式化等）

模块说明:
- core.py: 核心数据结构和类型定义
- pipeline.py: 流水线框架和阶段实现
- cli.py: 命令行接口
- bench.py: 基准测试逻辑
"""

__version__ = "0.1.0"

from batchpipe.core import Record, PipelineResult
from batchpipe.pipeline import (
    Pipeline,
    Stage,
    NormalizeStage,
    AggregateStage,
    SlidingWindowStage,
    FormatStage,
    SplitStage,
    CollectStage,
)

__all__ = [
    "Record",
    "PipelineResult",
    "Pipeline",
    "Stage",
    "NormalizeStage",
    "AggregateStage",
    "SlidingWindowStage",
    "FormatStage",
    "SplitStage",
    "CollectStage",
]
