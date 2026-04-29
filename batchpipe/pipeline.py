"""流水线框架和阶段实现。

本模块实现了可组合的流式批处理流水线，使用 collections 和 itertools 构建。

collections 使用:
- deque: 滑动窗口统计
- Counter: 计数聚合
- defaultdict: 分组聚合
- ChainMap: 配置合并

itertools 使用:
- groupby: 按键分组
- islice: 切片/分批处理
- chain: 迭代器链
- tee: 迭代器复制
"""

from abc import ABC, abstractmethod
from collections import deque, Counter, defaultdict, ChainMap
from itertools import groupby, islice, chain, tee
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from batchpipe.core import Record, PipelineResult, StageError, ensure_iterator


T = TypeVar("T")


class Stage(ABC):
    """处理阶段基类。

    所有自定义处理阶段都应继承此类并实现 process 方法。
    """

    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__

    @abstractmethod
    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """处理记录流。

        Args:
            records: 输入记录迭代器

        Yields:
            处理后的记录
        """
        pass

    def __call__(self, records: Iterator[Record]) -> Iterator[Record]:
        return self.process(records)


class SplitStage(Stage):
    """分割阶段：将原始文本流分割为记录。

    支持按行分割或自定义分隔符分割。

    Attributes:
        delimiter: 记录分隔符，默认为换行符
        skip_blank: 是否跳过空白行
    """

    def __init__(
        self,
        delimiter: str = "\n",
        skip_blank: bool = True,
        name: Optional[str] = None,
    ):
        super().__init__(name)
        self.delimiter = delimiter
        self.skip_blank = skip_blank

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """分割原始文本为记录。

        使用 chain.from_iterable 展平分割后的结果。
        """
        for record in records:
            if not record.valid:
                yield record
                continue

            raw_text = record.raw
            if self.delimiter == "\n":
                lines = raw_text.splitlines()
            else:
                lines = raw_text.split(self.delimiter)

            for line in lines:
                stripped = line.strip()
                if self.skip_blank and not stripped:
                    continue

                yield Record(
                    raw=line,
                    data=stripped if stripped else None,
                    metadata={**record.metadata, "original_index": record.metadata.get("index", 0)},
                    valid=bool(stripped),
                )


class NormalizeStage(Stage):
    """规范化阶段：对记录数据进行标准化处理。

    支持多种规范化操作：
    - 大小写转换
    - 空白处理
    - 字段解析（如 key:value 格式）

    使用 ChainMap 合并默认配置和用户配置。
    """

    DEFAULT_CONFIG = {
        "lowercase": True,
        "uppercase": False,
        "strip": True,
        "parse_fields": False,
        "field_sep": ":",
    }

    def __init__(
        self,
        lowercase: Optional[bool] = None,
        uppercase: Optional[bool] = None,
        strip: Optional[bool] = None,
        parse_fields: Optional[bool] = None,
        field_sep: str = ":",
        name: Optional[str] = None,
    ):
        super().__init__(name)
        user_config = {
            k: v
            for k, v in {
                "lowercase": lowercase,
                "uppercase": uppercase,
                "strip": strip,
                "parse_fields": parse_fields,
                "field_sep": field_sep,
            }.items()
            if v is not None
        }
        self.config = ChainMap(user_config, self.DEFAULT_CONFIG)

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """规范化处理每条记录。"""
        for record in records:
            if not record.valid:
                yield record
                continue

            data = record.data if record.data is not None else record.raw

            if self.config["strip"] and isinstance(data, str):
                data = data.strip()

            if self.config["lowercase"] and isinstance(data, str):
                data = data.lower()
            elif self.config["uppercase"] and isinstance(data, str):
                data = data.upper()

            if self.config["parse_fields"] and isinstance(data, str):
                parts = data.split(self.config["field_sep"], 1)
                if len(parts) == 2:
                    data = {"key": parts[0].strip(), "value": parts[1].strip()}
                else:
                    data = {"key": data, "value": ""}

            yield Record(
                raw=record.raw,
                data=data,
                metadata={**record.metadata, "normalized": True},
                valid=record.valid,
            )


class AggregateStage(Stage):
    """聚合阶段：按键聚合记录。

    使用多种 collections 工具：
    - groupby: 迭代器分组
    - Counter: 计数统计
    - defaultdict: 分组存储

    注意：groupby 需要输入已排序，此阶段会先按键排序。
    """

    def __init__(
        self,
        key_extractor: Optional[Callable[[Any], Any]] = None,
        aggregate_func: Optional[str] = "count",
        name: Optional[str] = None,
    ):
        super().__init__(name)
        self.key_extractor = key_extractor or self._default_key_extractor
        self.aggregate_func = aggregate_func

    @staticmethod
    def _default_key_extractor(data: Any) -> Any:
        """默认键提取器：从字典或字符串提取键。"""
        if isinstance(data, dict):
            return data.get("key", data)
        return data

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """聚合记录。

        流程：
        1. 使用 tee 复制迭代器（用于统计）
        2. 按键排序（groupby 要求）
        3. 使用 groupby 分组
        4. 使用 Counter/defaultdict 聚合
        """
        records = ensure_iterator(records)
        records1, records2 = tee(records, 2)

        valid_records = (r for r in records1 if r.valid)

        keyed_records = []
        for record in valid_records:
            key = self.key_extractor(record.data)
            keyed_records.append((key, record))

        keyed_records.sort(key=lambda x: x[0])

        for key, group in groupby(keyed_records, key=lambda x: x[0]):
            group_records = [r[1] for r in group]

            if self.aggregate_func == "count":
                aggregated = {"key": key, "count": len(group_records)}
            elif self.aggregate_func == "sum":
                values = []
                for r in group_records:
                    if isinstance(r.data, dict) and "value" in r.data:
                        try:
                            values.append(float(r.data["value"]))
                        except (ValueError, TypeError):
                            pass
                aggregated = {"key": key, "sum": sum(values), "count": len(group_records)}
            elif self.aggregate_func == "collect":
                aggregated = {
                    "key": key,
                    "values": [r.data for r in group_records],
                    "count": len(group_records),
                }
            else:
                aggregated = {"key": key, "count": len(group_records)}

            yield Record(
                raw=f"{key}: {aggregated}",
                data=aggregated,
                metadata={
                    "group_key": key,
                    "source_count": len(group_records),
                    "aggregate_func": self.aggregate_func,
                },
                valid=True,
            )


class SlidingWindowStage(Stage):
    """滑动窗口阶段：计算滑动窗口统计。

    使用 deque 实现高效的滑动窗口：
    - append/popleft 都是 O(1)
    - 支持固定大小窗口

    使用 islice 处理窗口切片。
    """

    def __init__(
        self,
        window_size: int = 5,
        value_extractor: Optional[Callable[[Any], float]] = None,
        statistics: Tuple[str, ...] = ("mean",),
        name: Optional[str] = None,
    ):
        super().__init__(name)
        self.window_size = window_size
        self.value_extractor = value_extractor or self._default_value_extractor
        self.statistics = statistics
        self._window: deque = deque(maxlen=window_size)

    @staticmethod
    def _default_value_extractor(data: Any) -> float:
        """默认值提取器：从字典或值提取数值。"""
        if isinstance(data, dict):
            val = data.get("value", data.get("count", 0))
        else:
            val = data

        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """计算滑动窗口统计。

        使用 deque 维护窗口，当窗口填满后输出统计。
        """
        window: deque = deque(maxlen=self.window_size)
        index = 0

        for record in records:
            if not record.valid:
                yield record
                continue

            value = self.value_extractor(record.data)
            window.append(value)
            index += 1

            if len(window) == self.window_size:
                stats: Dict[str, Any] = {"window_index": index - self.window_size + 1}

                window_list = list(window)

                if "mean" in self.statistics:
                    stats["mean"] = sum(window_list) / len(window_list)

                if "sum" in self.statistics:
                    stats["sum"] = sum(window_list)

                if "min" in self.statistics:
                    stats["min"] = min(window_list)

                if "max" in self.statistics:
                    stats["max"] = max(window_list)

                if "count" in self.statistics:
                    stats["count"] = len(window_list)

                stats["window_values"] = window_list

                yield Record(
                    raw=f"Window {stats['window_index']}: {stats}",
                    data=stats,
                    metadata={
                        **record.metadata,
                        "window_size": self.window_size,
                        "window_index": stats["window_index"],
                    },
                    valid=True,
                )


class FormatStage(Stage):
    """格式化阶段：将记录格式化为输出字符串。

    支持多种格式：json、csv、plain 等。
    """

    def __init__(
        self,
        format_type: str = "plain",
        separator: str = "\t",
        name: Optional[str] = None,
    ):
        super().__init__(name)
        self.format_type = format_type
        self.separator = separator

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """格式化记录。"""
        import json

        for record in records:
            if not record.valid:
                yield record
                continue

            data = record.data

            if self.format_type == "json":
                formatted = json.dumps(data, ensure_ascii=False)
            elif self.format_type == "csv":
                if isinstance(data, dict):
                    formatted = self.separator.join(str(v) for v in data.values())
                elif isinstance(data, (list, tuple)):
                    formatted = self.separator.join(str(v) for v in data)
                else:
                    formatted = str(data)
            elif self.format_type == "key_value":
                if isinstance(data, dict):
                    formatted = ", ".join(f"{k}={v}" for k, v in data.items())
                else:
                    formatted = str(data)
            else:
                formatted = str(data)

            yield Record(
                raw=formatted,
                data=formatted,
                metadata={**record.metadata, "formatted": True, "format_type": self.format_type},
                valid=record.valid,
            )


class CollectStage(Stage):
    """收集阶段：将迭代器物化为列表。

    用于基准测试，对比流式处理和物化处理的性能差异。
    """

    def __init__(self, name: Optional[str] = None):
        super().__init__(name or "Collect")

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """将迭代器物化为列表后再返回迭代器。

        这会消耗内存，用于对比测试。
        """
        materialized = list(records)
        return iter(materialized)


class FilterStage(Stage):
    """过滤阶段：根据条件过滤记录。

    使用迭代器链实现流式过滤。
    """

    def __init__(
        self,
        predicate: Callable[[Record], bool],
        name: Optional[str] = None,
    ):
        super().__init__(name)
        self.predicate = predicate

    def process(self, records: Iterator[Record]) -> Iterator[Record]:
        """过滤记录。"""
        return filter(self.predicate, records)


class Pipeline:
    """流水线主类：组合多个处理阶段。

    支持链式调用方式组合阶段，流式处理记录。

    Example:
        pipeline = Pipeline([
            SplitStage(),
            NormalizeStage(),
            AggregateStage(),
            FormatStage(),
        ])
        result = pipeline.run(input_iterator)
    """

    def __init__(self, stages: Optional[List[Stage]] = None):
        self.stages: List[Stage] = stages or []

    def add_stage(self, stage: Stage) -> "Pipeline":
        """添加一个处理阶段。"""
        self.stages.append(stage)
        return self

    def __or__(self, stage: Stage) -> "Pipeline":
        """支持 | 运算符添加阶段。"""
        return self.add_stage(stage)

    def run(self, input_stream: Iterator[Record]) -> PipelineResult:
        """运行流水线。

        Args:
            input_stream: 输入记录迭代器

        Returns:
            PipelineResult: 包含输出记录和统计信息
        """
        current = ensure_iterator(input_stream)
        stats: Dict[str, Any] = {"stages": [s.name for s in self.stages]}

        for stage in self.stages:
            try:
                current = stage.process(current)
            except Exception as e:
                raise StageError(stage.name, str(e), e)

        return PipelineResult(records=current, stats=stats)

    def run_with_materialization(self, input_stream: Iterator[Record]) -> PipelineResult:
        """运行流水线，但在每个阶段之间物化。

        用于基准测试，对比纯流式处理。
        """
        current = ensure_iterator(input_stream)
        stats: Dict[str, Any] = {"stages": [s.name for s in self.stages], "materialized": True}

        for stage in self.stages:
            try:
                current = stage.process(current)
                current = iter(list(current))
            except Exception as e:
                raise StageError(stage.name, str(e), e)

        return PipelineResult(records=current, stats=stats)


def create_basic_pipeline(
    delimiter: str = "\n",
    lowercase: bool = True,
    aggregate_key: Optional[str] = None,
    output_format: str = "plain",
) -> Pipeline:
    """创建基本三阶段流水线。

    阶段：
    1. SplitStage: 分割输入
    2. NormalizeStage: 规范化
    3. AggregateStage 或 SlidingWindowStage: 聚合/统计
    4. FormatStage: 格式化输出

    Args:
        delimiter: 记录分隔符
        lowercase: 是否转为小写
        aggregate_key: 聚合键（如为 None 则使用滑动窗口）
        output_format: 输出格式

    Returns:
        Pipeline: 配置好的流水线
    """
    pipeline = Pipeline()
    pipeline.add_stage(SplitStage(delimiter=delimiter, skip_blank=True))
    pipeline.add_stage(NormalizeStage(lowercase=lowercase, parse_fields=bool(aggregate_key)))

    if aggregate_key:
        pipeline.add_stage(AggregateStage(aggregate_func="count"))
    else:
        pipeline.add_stage(SlidingWindowStage(window_size=5, statistics=("mean", "sum")))

    pipeline.add_stage(FormatStage(format_type=output_format))

    return pipeline
