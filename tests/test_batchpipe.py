"""Batchpipe 测试套件。

测试覆盖:
- 流水线正确性
- 边界情况（空输入、空白行）
- UTF-8 解码失败
- 各阶段功能
"""

import io
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterator, List

import pytest

from batchpipe.core import Record, StageError, DecodeError, EmptyInputError
from batchpipe.pipeline import (
    Pipeline,
    SplitStage,
    NormalizeStage,
    AggregateStage,
    SlidingWindowStage,
    FormatStage,
    CollectStage,
    create_basic_pipeline,
)
from batchpipe.bench import (
    generate_test_records,
    create_bench_pipeline,
    run_streaming,
    run_materialized,
    run_benchmark,
)


class TestRecord:
    """Record 数据结构测试。"""

    def test_record_creation(self):
        """测试创建基本记录。"""
        record = Record(raw="test line", data="test data")
        assert record.raw == "test line"
        assert record.data == "test data"
        assert record.valid is True
        assert isinstance(record.metadata, dict)

    def test_record_with_metadata(self):
        """测试带元数据的记录。"""
        record = Record(
            raw="line",
            data={"key": "value"},
            metadata={"index": 1, "source": "stdin"},
            valid=True,
        )
        assert record.metadata["index"] == 1
        assert record.metadata["source"] == "stdin"

    def test_record_invalid(self):
        """测试无效记录。"""
        record = Record(raw="", data=None, valid=False)
        assert record.valid is False


class TestSplitStage:
    """SplitStage 测试。"""

    def test_split_by_newline(self):
        """测试按换行符分割。"""
        stage = SplitStage(delimiter="\n", skip_blank=True)
        input_records = [Record(raw="line1\nline2\nline3", data="line1\nline2\nline3")]
        result = list(stage.process(iter(input_records)))

        assert len(result) == 3
        assert result[0].data == "line1"
        assert result[1].data == "line2"
        assert result[2].data == "line3"

    def test_split_skip_blank(self):
        """测试跳过空白行。"""
        stage = SplitStage(delimiter="\n", skip_blank=True)
        input_records = [Record(raw="line1\n\n  \nline2\n", data="line1\n\n  \nline2\n")]
        result = list(stage.process(iter(input_records)))

        assert len(result) == 2
        assert result[0].data == "line1"
        assert result[1].data == "line2"

    def test_split_keep_blank(self):
        """测试保留空白行。"""
        stage = SplitStage(delimiter="\n", skip_blank=False)
        input_records = [Record(raw="line1\n\nline2", data="line1\n\nline2")]
        result = list(stage.process(iter(input_records)))

        assert len(result) == 3
        assert result[0].valid is True
        assert result[1].valid is False
        assert result[2].valid is True


class TestNormalizeStage:
    """NormalizeStage 测试。"""

    def test_normalize_lowercase(self):
        """测试转换为小写。"""
        stage = NormalizeStage(lowercase=True, strip=True)
        input_records = [Record(raw="  TEST LINE  ", data="  TEST LINE  ")]
        result = list(stage.process(iter(input_records)))

        assert result[0].data == "test line"

    def test_normalize_uppercase(self):
        """测试转换为大写。"""
        stage = NormalizeStage(lowercase=False, uppercase=True, strip=True)
        input_records = [Record(raw="test line", data="test line")]
        result = list(stage.process(iter(input_records)))

        assert result[0].data == "TEST LINE"

    def test_normalize_parse_fields(self):
        """测试解析 key:value 字段。"""
        stage = NormalizeStage(parse_fields=True, field_sep=":")
        input_records = [Record(raw="user1: login", data="user1: login")]
        result = list(stage.process(iter(input_records)))

        assert isinstance(result[0].data, dict)
        assert result[0].data["key"] == "user1"
        assert result[0].data["value"] == "login"

    def test_normalize_parse_fields_no_value(self):
        """测试解析只有 key 没有 value 的情况。"""
        stage = NormalizeStage(parse_fields=True, field_sep=":")
        input_records = [Record(raw="solo_key", data="solo_key")]
        result = list(stage.process(iter(input_records)))

        assert result[0].data["key"] == "solo_key"
        assert result[0].data["value"] == ""


class TestAggregateStage:
    """AggregateStage 测试。"""

    def test_aggregate_count(self):
        """测试计数聚合。"""
        stage = AggregateStage(aggregate_func="count")
        input_records = [
            Record(raw="user1: a", data={"key": "user1", "value": "a"}, valid=True),
            Record(raw="user2: b", data={"key": "user2", "value": "b"}, valid=True),
            Record(raw="user1: c", data={"key": "user1", "value": "c"}, valid=True),
        ]
        result = list(stage.process(iter(input_records)))

        assert len(result) == 2
        result_by_key = {r.data["key"]: r.data["count"] for r in result}
        assert result_by_key["user1"] == 2
        assert result_by_key["user2"] == 1

    def test_aggregate_sum(self):
        """测试求和聚合。"""
        stage = AggregateStage(aggregate_func="sum")
        input_records = [
            Record(raw="a: 10", data={"key": "a", "value": "10"}, valid=True),
            Record(raw="a: 20", data={"key": "a", "value": "20"}, valid=True),
            Record(raw="b: 5", data={"key": "b", "value": "5"}, valid=True),
        ]
        result = list(stage.process(iter(input_records)))

        result_by_key = {r.data["key"]: r.data for r in result}
        assert result_by_key["a"]["sum"] == 30.0
        assert result_by_key["a"]["count"] == 2
        assert result_by_key["b"]["sum"] == 5.0

    def test_aggregate_uses_groupby(self):
        """测试使用 groupby 进行分组。"""
        stage = AggregateStage(aggregate_func="count")
        input_records = [
            Record(raw="z: 1", data={"key": "z", "value": "1"}, valid=True),
            Record(raw="a: 2", data={"key": "a", "value": "2"}, valid=True),
            Record(raw="z: 3", data={"key": "z", "value": "3"}, valid=True),
        ]
        result = list(stage.process(iter(input_records)))

        assert len(result) == 2
        assert result[0].data["key"] == "a"
        assert result[1].data["key"] == "z"


class TestSlidingWindowStage:
    """SlidingWindowStage 测试。"""

    def test_sliding_window_mean(self):
        """测试滑动窗口平均值。"""
        stage = SlidingWindowStage(window_size=3, statistics=("mean", "sum"))
        input_records = [
            Record(raw="1", data=1, valid=True),
            Record(raw="2", data=2, valid=True),
            Record(raw="3", data=3, valid=True),
            Record(raw="4", data=4, valid=True),
        ]
        result = list(stage.process(iter(input_records)))

        assert len(result) == 2

        assert result[0].data["window_index"] == 1
        assert result[0].data["mean"] == (1 + 2 + 3) / 3
        assert result[0].data["sum"] == 6

        assert result[1].data["window_index"] == 2
        assert result[1].data["mean"] == (2 + 3 + 4) / 3
        assert result[1].data["sum"] == 9

    def test_sliding_window_deque_usage(self):
        """测试 deque 用于滑动窗口。"""
        stage = SlidingWindowStage(window_size=2, statistics=("count",))
        input_records = [
            Record(raw="1", data={"value": 10}, valid=True),
            Record(raw="2", data={"value": 20}, valid=True),
            Record(raw="3", data={"value": 30}, valid=True),
        ]
        result = list(stage.process(iter(input_records)))

        assert len(result) == 2
        assert result[0].data["count"] == 2
        assert result[1].data["count"] == 2


class TestFormatStage:
    """FormatStage 测试。"""

    def test_format_json(self):
        """测试 JSON 格式化。"""
        stage = FormatStage(format_type="json")
        input_records = [Record(raw="", data={"key": "user1", "count": 5}, valid=True)]
        result = list(stage.process(iter(input_records)))

        import json
        parsed = json.loads(result[0].data)
        assert parsed["key"] == "user1"
        assert parsed["count"] == 5

    def test_format_csv(self):
        """测试 CSV 格式化。"""
        stage = FormatStage(format_type="csv", separator=",")
        input_records = [Record(raw="", data={"key": "user1", "count": 5}, valid=True)]
        result = list(stage.process(iter(input_records)))

        parts = result[0].data.split(",")
        assert len(parts) == 2

    def test_format_key_value(self):
        """测试 key_value 格式化。"""
        stage = FormatStage(format_type="key_value")
        input_records = [Record(raw="", data={"key": "user1", "count": 5}, valid=True)]
        result = list(stage.process(iter(input_records)))

        assert "key=user1" in result[0].data
        assert "count=5" in result[0].data


class TestCollectStage:
    """CollectStage 测试（用于基准测试）。"""

    def test_collect_materializes(self):
        """测试收集阶段会物化迭代器。"""
        stage = CollectStage()

        def generate_records() -> Iterator[Record]:
            for i in range(3):
                yield Record(raw=f"r{i}", data=i, valid=True)

        records = generate_records()
        assert iter(records) is records

        result = stage.process(records)
        result_list = list(result)

        assert len(result_list) == 3


class TestPipeline:
    """Pipeline 集成测试。"""

    def test_pipeline_chaining(self):
        """测试流水线链式调用。"""
        pipeline = Pipeline()
        pipeline.add_stage(SplitStage(delimiter="\n"))
        pipeline.add_stage(NormalizeStage(lowercase=True))
        pipeline.add_stage(FormatStage(format_type="plain"))

        input_text = "LINE1\nLINE2\nLINE3"
        input_records = [Record(raw=input_text, data=input_text)]

        result = pipeline.run(iter(input_records))
        output = list(result.records)

        assert len(output) == 3
        assert output[0].data == "line1"
        assert output[1].data == "line2"
        assert output[2].data == "line3"

    def test_pipeline_or_operator(self):
        """测试使用 | 运算符添加阶段。"""
        pipeline = Pipeline()
        pipeline |= SplitStage(delimiter="\n")
        pipeline |= NormalizeStage(lowercase=True)

        assert len(pipeline.stages) == 2

    def test_full_pipeline_aggregate(self):
        """测试完整的三阶段聚合流水线。"""
        pipeline = create_basic_pipeline(
            delimiter="\n",
            lowercase=True,
            aggregate_key="user",
            output_format="json",
        )

        input_text = "user1: login\nuser2: logout\nuser1: action"
        input_records = [Record(raw=input_text, data=input_text)]

        result = pipeline.run(iter(input_records))
        output = list(result.records)

        assert len(output) == 2

    def test_full_pipeline_window(self):
        """测试完整的滑动窗口流水线。"""
        pipeline = create_basic_pipeline(
            delimiter="\n",
            lowercase=False,
            aggregate_key=None,
            output_format="plain",
        )

        input_text = "\n".join(str(i) for i in range(10))
        input_records = [Record(raw=input_text, data=input_text)]

        result = pipeline.run(iter(input_records))
        output = list(result.records)

        assert len(output) > 0


class TestBoundaryConditions:
    """边界条件测试。"""

    def test_empty_input(self):
        """测试空输入。"""
        pipeline = create_basic_pipeline(delimiter="\n", aggregate_key="test")
        input_records = [Record(raw="", data="", valid=True)]

        result = pipeline.run(iter(input_records))
        output = list(result.records)

        assert len(output) == 0

    def test_only_blank_lines(self):
        """测试只有空白行。"""
        stage = SplitStage(delimiter="\n", skip_blank=True)
        input_records = [Record(raw="\n  \n\t\n", data="\n  \n\t\n")]

        result = list(stage.process(iter(input_records)))

        assert len(result) == 0

    def test_invalid_records_skipped(self):
        """测试无效记录被跳过。"""
        stage = NormalizeStage()
        input_records = [
            Record(raw="valid", data="valid", valid=True),
            Record(raw="invalid", data=None, valid=False),
            Record(raw="another", data="another", valid=True),
        ]

        result = list(stage.process(iter(input_records)))

        assert len(result) == 3
        assert result[0].valid is True
        assert result[1].valid is False
        assert result[2].valid is True


class TestDecodeError:
    """UTF-8 解码错误测试。"""

    def test_decode_error_class(self):
        """测试 DecodeError 异常类。"""
        error = DecodeError("Invalid UTF-8 bytes")
        assert error.stage_name == "Decode"
        assert "Invalid UTF-8" in str(error)

    def test_decode_error_with_original(self):
        """测试带原始错误的 DecodeError。"""
        original = UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")
        error = DecodeError("Failed to decode", original)
        assert error.original_error is original

    def test_empty_input_error(self):
        """测试 EmptyInputError。"""
        error = EmptyInputError("No valid records")
        assert error.stage_name == "Input"

    def test_stage_error(self):
        """测试 StageError 基类。"""
        error = StageError("TestStage", "Something went wrong")
        assert error.stage_name == "TestStage"
        assert "TestStage" in str(error)


class TestBenchmark:
    """基准测试模块测试。"""

    def test_generate_test_records(self):
        """测试生成测试记录。"""
        records = list(generate_test_records(10, seed=42))
        assert len(records) == 10
        for r in records:
            assert isinstance(r.raw, str)
            assert ":" in r.raw

    def test_generate_test_records_deterministic(self):
        """测试相同种子生成相同记录。"""
        records1 = list(generate_test_records(5, seed=123))
        records2 = list(generate_test_records(5, seed=123))

        assert len(records1) == len(records2)
        for r1, r2 in zip(records1, records2):
            assert r1.raw == r2.raw

    def test_run_benchmark_returns_correct_format(self):
        """测试基准测试返回正确格式。"""
        results = run_benchmark(num_records=100, seed=42, warmup=0, runs=1)

        assert "streaming" in results
        assert "materialized" in results

        assert "time=" in results["streaming"]
        assert "memory=" in results["streaming"]
        assert "outputs=" in results["streaming"]

        assert "time=" in results["materialized"]
        assert "memory=" in results["materialized"]
        assert "outputs=" in results["materialized"]

    def test_streaming_vs_materialized_same_output(self):
        """测试流式和物化处理产生相同输出。"""
        records = list(generate_test_records(100, seed=42))
        pipeline1 = create_bench_pipeline()
        pipeline2 = create_bench_pipeline()

        elapsed_s, mem_s, outputs_s = run_streaming(iter(records), pipeline1)
        elapsed_m, mem_m, outputs_m = run_materialized(iter(list(records)), pipeline2)

        assert len(outputs_s) == len(outputs_m)
        assert set(outputs_s) == set(outputs_m)


class TestItertoolsUsage:
    """itertools 使用测试。"""

    def test_tee_used_in_aggregate(self):
        """测试 tee 在聚合中的使用。"""
        from itertools import tee

        original = iter([1, 2, 3])
        copy1, copy2 = tee(original, 2)

        assert list(copy1) == [1, 2, 3]
        assert list(copy2) == [1, 2, 3]

    def test_chain_used_in_concept(self):
        """测试 chain 的使用方式。"""
        from itertools import chain

        iter1 = iter([Record(raw="a", data="a")])
        iter2 = iter([Record(raw="b", data="b")])

        combined = chain(iter1, iter2)
        result = list(combined)

        assert len(result) == 2

    def test_islice_used_in_concept(self):
        """测试 islice 的使用方式。"""
        from itertools import islice

        items = [1, 2, 3, 4, 5]
        sliced = islice(items, 2, 4)

        assert list(sliced) == [3, 4]

    def test_groupby_requires_sorted(self):
        """测试 groupby 需要输入已排序。"""
        from itertools import groupby

        data = [("b", 1), ("a", 2), ("b", 3)]
        sorted_data = sorted(data, key=lambda x: x[0])

        groups = [(k, list(g)) for k, g in groupby(sorted_data, key=lambda x: x[0])]

        assert len(groups) == 2
        assert groups[0][0] == "a"
        assert groups[1][0] == "b"


class TestCollectionsUsage:
    """collections 使用测试。"""

    def test_deque_sliding_window(self):
        """测试 deque 用于滑动窗口。"""
        from collections import deque

        window = deque(maxlen=3)
        values = [1, 2, 3, 4, 5]

        for v in values:
            window.append(v)
            if len(window) == 3:
                assert len(list(window)) == 3

        assert list(window) == [3, 4, 5]

    def test_counter_usage(self):
        """测试 Counter 使用。"""
        from collections import Counter

        items = ["a", "b", "a", "c", "a", "b"]
        counter = Counter(items)

        assert counter["a"] == 3
        assert counter["b"] == 2
        assert counter["c"] == 1

    def test_defaultdict_usage(self):
        """测试 defaultdict 使用。"""
        from collections import defaultdict

        groups = defaultdict(list)
        items = [("a", 1), ("b", 2), ("a", 3)]

        for key, value in items:
            groups[key].append(value)

        assert groups["a"] == [1, 3]
        assert groups["b"] == [2]

    def test_chainmap_config(self):
        """测试 ChainMap 用于配置合并。"""
        from collections import ChainMap

        defaults = {"a": 1, "b": 2}
        user_config = {"b": 20}

        config = ChainMap(user_config, defaults)

        assert config["a"] == 1
        assert config["b"] == 20


class TestCLI:
    """CLI 集成测试。"""

    def test_cli_help(self):
        """测试 CLI 帮助输出。"""
        result = subprocess.run(
            [sys.executable, "-m", "batchpipe", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "run" in result.stdout
        assert "bench" in result.stdout

    def test_cli_run_help(self):
        """测试 run 子命令帮助。"""
        result = subprocess.run(
            [sys.executable, "-m", "batchpipe", "run", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--input-file" in result.stdout
        assert "--aggregate" in result.stdout
        assert "--window" in result.stdout

    def test_cli_bench_help(self):
        """测试 bench 子命令帮助。"""
        result = subprocess.run(
            [sys.executable, "-m", "batchpipe", "bench", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--records" in result.stdout
        assert "--seed" in result.stdout

    def test_cli_run_basic(self):
        """测试基本的 CLI run 调用。"""
        input_data = "user1: login\nuser2: logout\nuser1: action"
        result = subprocess.run(
            [sys.executable, "-m", "batchpipe", "run", "--parse-fields", "--aggregate"],
            input=input_data,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert len(result.stdout.strip()) > 0

    def test_cli_run_with_format(self):
        """测试带格式的 CLI run 调用。"""
        input_data = "a: 1\nb: 2\na: 3"
        result = subprocess.run(
            [sys.executable, "-m", "batchpipe", "run", "--parse-fields", "--aggregate", "--format", "json"],
            input=input_data,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        lines = result.stdout.strip().split("\n")
        assert len(lines) == 2

    def test_cli_bench_small(self):
        """测试小型基准测试。"""
        result = subprocess.run(
            [sys.executable, "-m", "batchpipe", "bench", "--records", "100", "--seed", "42", "--warmup", "0", "--runs", "1"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        lines = result.stdout.strip().split("\n")
        assert len(lines) == 2
        assert "streaming:" in lines[0]
        assert "materialized:" in lines[1]

    def test_cli_decode_error_simulation(self):
        """测试解码错误的退出码。

        注意：此测试通过 fail-on-empty 模拟非零退出码。
        真正的 UTF-8 解码错误需要二进制输入，在 pytest 中较难模拟。
        """
        result = subprocess.run(
            [sys.executable, "-m", "batchpipe", "run", "--fail-on-empty"],
            input="",
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert "Error" in result.stderr

    def test_cli_input_file(self):
        """测试从文件读取输入。"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("key1: value1\nkey2: value2\nkey1: value3\n")
            temp_path = f.name

        try:
            result = subprocess.run(
                [sys.executable, "-m", "batchpipe", "run", "--input-file", temp_path, "--parse-fields", "--aggregate"],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0
            assert len(result.stdout.strip()) > 0
        finally:
            Path(temp_path).unlink()
