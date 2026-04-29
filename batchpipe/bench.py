"""基准测试模块。

对比流式处理（纯迭代器）和物化处理（中间使用 list()）的性能差异。

度量口径说明:
- 时间: 使用 time.perf_counter() 测量的墙钟时间（秒）
- 内存: 使用 tracemalloc.get_traced_memory()[1] 获取的峰值内存（字节）

内存测量说明:
- tracemalloc 测量的是 Python 分配器追踪的内存
- 峰值内存是执行期间达到的最大值
- 两次运行使用相同种子时，结果应该一致（允许 <1% 的容差）
"""

import random
import time
import tracemalloc
from typing import Dict, Iterator, List, Tuple

from batchpipe.core import Record
from batchpipe.pipeline import (
    Pipeline,
    SplitStage,
    NormalizeStage,
    AggregateStage,
    FormatStage,
)


CATEGORIES = ["login", "logout", "action", "error", "warning", "info", "debug"]
USERS = [f"user{i}" for i in range(100)]


def generate_test_records(num_records: int, seed: int = 42) -> Iterator[Record]:
    """生成测试记录。

    生成格式为 "userN: category" 的记录，用于基准测试。

    Args:
        num_records: 生成记录数
        seed: 随机种子

    Yields:
        Record 对象
    """
    rng = random.Random(seed)

    for i in range(num_records):
        user = rng.choice(USERS)
        category = rng.choice(CATEGORIES)
        line = f"{user}: {category}\n"
        yield Record(raw=line, data=line, metadata={"index": i})


def create_bench_pipeline() -> Pipeline:
    """创建基准测试用的流水线。

    三阶段流水线:
    1. SplitStage: 分割输入
    2. NormalizeStage: 规范化并解析字段
    3. AggregateStage: 按 user 统计计数
    4. FormatStage: 格式化输出
    """
    pipeline = Pipeline()
    pipeline.add_stage(SplitStage(delimiter="\n", skip_blank=True))
    pipeline.add_stage(NormalizeStage(lowercase=True, parse_fields=True, field_sep=":"))
    pipeline.add_stage(AggregateStage(aggregate_func="count"))
    pipeline.add_stage(FormatStage(format_type="json"))
    return pipeline


def run_streaming(
    records: Iterator[Record],
    pipeline: Pipeline,
) -> Tuple[float, int, List[str]]:
    """运行流式处理版本。

    使用纯迭代器链接，不中间物化。

    Args:
        records: 输入记录迭代器
        pipeline: 流水线

    Returns:
        (耗时秒数, 峰值内存字节, 输出列表)
    """
    tracemalloc.start()
    tracemalloc.clear_traces()

    start_time = time.perf_counter()

    result = pipeline.run(records)

    outputs = []
    for record in result.records:
        if record.valid:
            outputs.append(record.raw)

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return elapsed, peak, outputs


def run_materialized(
    records: Iterator[Record],
    pipeline: Pipeline,
) -> Tuple[float, int, List[str]]:
    """运行物化处理版本。

    在每个阶段之间显式使用 list() 物化。

    Args:
        records: 输入记录迭代器
        pipeline: 流水线

    Returns:
        (耗时秒数, 峰值内存字节, 输出列表)
    """
    tracemalloc.start()
    tracemalloc.clear_traces()

    start_time = time.perf_counter()

    result = pipeline.run_with_materialization(records)

    outputs = []
    for record in result.records:
        if record.valid:
            outputs.append(record.raw)

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return elapsed, peak, outputs


def format_result_line(
    mode: str,
    elapsed: float,
    memory_bytes: int,
    output_count: int,
) -> str:
    """格式化结果行。

    格式: "mode: time=X.XXXXs memory=XXXXKB outputs=XXX"

    Args:
        mode: 模式名称（streaming/materialized）
        elapsed: 耗时秒
        memory_bytes: 内存字节
        output_count: 输出记录数

    Returns:
        格式化的字符串
    """
    memory_kb = memory_bytes / 1024
    return f"{mode}: time={elapsed:.4f}s memory={memory_kb:.2f}KB outputs={output_count}"


def run_benchmark(
    num_records: int = 100000,
    seed: int = 42,
    warmup: int = 1,
    runs: int = 3,
) -> Dict[str, str]:
    """运行完整基准测试。

    执行预热后，进行多次测试取平均值。

    固定种子保证可重复性:
    - 使用相同 seed 时，生成的测试数据完全一致
    - 两次运行的时间和内存应一致（允许 <1% 的容差）

    Args:
        num_records: 测试记录数
        seed: 随机种子
        warmup: 预热次数
        runs: 测试次数

    Returns:
        包含 streaming 和 materialized 结果行的字典
    """
    for _ in range(warmup):
        records = list(generate_test_records(num_records, seed))
        pipeline = create_bench_pipeline()

        run_streaming(iter(records), pipeline)
        run_materialized(iter(list(records)), create_bench_pipeline())

    streaming_times: List[float] = []
    streaming_mems: List[int] = []
    streaming_outputs: List[str] = []

    materialized_times: List[float] = []
    materialized_mems: List[int] = []
    materialized_outputs: List[str] = []

    for run_idx in range(runs):
        run_seed = seed + run_idx

        records = list(generate_test_records(num_records, run_seed))

        pipeline1 = create_bench_pipeline()
        elapsed_s, mem_s, outputs_s = run_streaming(iter(records), pipeline1)
        streaming_times.append(elapsed_s)
        streaming_mems.append(mem_s)
        if not streaming_outputs:
            streaming_outputs = outputs_s

        pipeline2 = create_bench_pipeline()
        elapsed_m, mem_m, outputs_m = run_materialized(iter(list(records)), pipeline2)
        materialized_times.append(elapsed_m)
        materialized_mems.append(mem_m)
        if not materialized_outputs:
            materialized_outputs = outputs_m

    avg_streaming_time = sum(streaming_times) / len(streaming_times)
    avg_streaming_mem = sum(streaming_mems) // len(streaming_mems)

    avg_materialized_time = sum(materialized_times) / len(materialized_times)
    avg_materialized_mem = sum(materialized_mems) // len(materialized_mems)

    return {
        "streaming": format_result_line(
            "streaming",
            avg_streaming_time,
            avg_streaming_mem,
            len(streaming_outputs),
        ),
        "materialized": format_result_line(
            "materialized",
            avg_materialized_time,
            avg_materialized_mem,
            len(materialized_outputs),
        ),
    }
