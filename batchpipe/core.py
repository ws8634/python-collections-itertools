"""核心数据结构和类型定义。

本模块定义了批处理流水线使用的核心数据结构。
"""

from dataclasses import dataclass, field
from typing import Any, Iterator, Optional, Union, Dict, List
from collections.abc import Iterable


@dataclass
class Record:
    """单条记录数据结构。

    用于流水线中传递的数据单元，包含原始数据、解析后的数据和元数据。

    Attributes:
        raw: 原始输入数据（如原始行字符串）
        data: 解析/处理后的数据（字典或任意类型）
        metadata: 元数据字典，用于传递阶段间的额外信息
        valid: 记录是否有效（用于过滤无效记录）
    """

    raw: str
    data: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    valid: bool = True

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class PipelineResult:
    """流水线执行结果。

    包含处理后的记录流和统计信息。

    Attributes:
        records: 处理后的记录迭代器
        stats: 统计信息字典（记录数、处理时间等）
    """

    records: Iterator[Record]
    stats: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.stats is None:
            self.stats = {}


class StageError(Exception):
    """流水线阶段执行错误。

    当某个处理阶段发生错误时抛出，包含阶段名称和原始错误信息。
    """

    def __init__(self, stage_name: str, message: str, original_error: Optional[Exception] = None):
        self.stage_name = stage_name
        self.message = message
        self.original_error = original_error
        super().__init__(f"[{stage_name}] {message}")


class DecodeError(StageError):
    """UTF-8 解码错误。

    当输入数据无法解码为 UTF-8 时抛出。
    """

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__("Decode", message, original_error)


class EmptyInputError(StageError):
    """空输入错误。

    当输入流为空且不允许空输入时抛出。
    """

    def __init__(self, message: str = "Empty input received"):
        super().__init__("Input", message)


def ensure_iterator(obj: Union[Iterable, Iterator]) -> Iterator:
    """确保对象是迭代器。

    Args:
        obj: 可迭代对象或迭代器

    Returns:
        迭代器对象
    """
    if isinstance(obj, Iterator):
        return obj
    return iter(obj)
