"""命令行接口模块。

提供 run 和 bench 两个子命令：
- run: 运行流水线处理输入
- bench: 基准测试，对比流式处理和物化处理
"""

import argparse
import codecs
import sys
from typing import Iterator, Optional, TextIO

from batchpipe.core import Record, StageError, DecodeError, EmptyInputError
from batchpipe.pipeline import (
    Pipeline,
    SplitStage,
    NormalizeStage,
    AggregateStage,
    SlidingWindowStage,
    FormatStage,
    CollectStage,
)
from batchpipe.bench import run_benchmark, generate_test_records


EXIT_SUCCESS = 0
EXIT_ERROR = 1


def read_input(
    input_file: Optional[str] = None,
    encoding: str = "utf-8",
    errors: str = "strict",
) -> Iterator[str]:
    """读取输入流。

    从 stdin 或文件读取，支持编码配置。

    Args:
        input_file: 输入文件路径，None 表示从 stdin 读取
        encoding: 文件编码
        errors: 错误处理方式：strict/replace/ignore

    Yields:
        每行字符串

    Raises:
        DecodeError: UTF-8 解码失败
    """
    source: TextIO

    if input_file:
        try:
            source = open(input_file, "r", encoding=encoding, errors=errors)
        except FileNotFoundError:
            print(f"Error: Input file not found: {input_file}", file=sys.stderr)
            sys.exit(EXIT_ERROR)
        except UnicodeDecodeError as e:
            raise DecodeError(f"Failed to decode input file: {e}", e)
    else:
        source = sys.stdin

    try:
        for line in source:
            yield line
    except UnicodeDecodeError as e:
        raise DecodeError(f"Failed to decode input: {e}", e)
    finally:
        if input_file and source is not sys.stdin:
            source.close()


def create_run_pipeline(
    delimiter: str = "\n",
    skip_blank: bool = True,
    lowercase: bool = True,
    parse_fields: bool = False,
    field_sep: str = ":",
    aggregate: bool = False,
    aggregate_func: str = "count",
    window: bool = False,
    window_size: int = 5,
    output_format: str = "plain",
) -> Pipeline:
    """创建 run 命令使用的流水线。

    至少三个阶段：
    1. SplitStage: 分割输入
    2. NormalizeStage: 规范化
    3. AggregateStage 或 SlidingWindowStage: 聚合/统计
    4. FormatStage: 格式化输出
    """
    pipeline = Pipeline()

    pipeline.add_stage(SplitStage(delimiter=delimiter, skip_blank=skip_blank))

    pipeline.add_stage(
        NormalizeStage(
            lowercase=lowercase,
            parse_fields=parse_fields,
            field_sep=field_sep,
        )
    )

    if aggregate:
        pipeline.add_stage(AggregateStage(aggregate_func=aggregate_func))
    elif window:
        pipeline.add_stage(
            SlidingWindowStage(
                window_size=window_size,
                statistics=("mean", "sum", "min", "max"),
            )
        )

    pipeline.add_stage(FormatStage(format_type=output_format))

    return pipeline


def run_command(args: argparse.Namespace) -> int:
    """执行 run 子命令。

    从 stdin 或文件读取输入，经过流水线处理后输出到 stdout。

    Args:
        args: 命令行参数

    Returns:
        退出码
    """
    try:
        input_lines = read_input(
            input_file=args.input_file,
            encoding=args.encoding,
            errors=args.errors,
        )

        input_records = (Record(raw=line, data=line) for line in input_lines)

        pipeline = create_run_pipeline(
            delimiter=args.delimiter,
            skip_blank=args.skip_blank,
            lowercase=not args.no_lowercase,
            parse_fields=args.parse_fields,
            field_sep=args.field_sep,
            aggregate=args.aggregate,
            aggregate_func=args.aggregate_func,
            window=args.window,
            window_size=args.window_size,
            output_format=args.format,
        )

        result = pipeline.run(input_records)

        output_count = 0
        for record in result.records:
            if record.valid:
                print(record.raw)
                output_count += 1

        if output_count == 0 and args.fail_on_empty:
            raise EmptyInputError("No valid output records produced")

        return EXIT_SUCCESS

    except DecodeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return EXIT_ERROR
    except EmptyInputError as e:
        print(f"Error: {e}", file=sys.stderr)
        return EXIT_ERROR
    except StageError as e:
        print(f"Error: Pipeline stage failed - {e}", file=sys.stderr)
        return EXIT_ERROR
    except Exception as e:
        print(f"Error: Unexpected error - {e}", file=sys.stderr)
        return EXIT_ERROR


def bench_command(args: argparse.Namespace) -> int:
    """执行 bench 子命令。

    生成测试数据，对比流式处理和物化处理的性能。

    Args:
        args: 命令行参数

    Returns:
        退出码
    """
    try:
        import random

        random.seed(args.seed)

        if args.records <= 0:
            print("Error: --records must be greater than 0", file=sys.stderr)
            return EXIT_ERROR

        results = run_benchmark(
            num_records=args.records,
            seed=args.seed,
            warmup=args.warmup,
            runs=args.runs,
        )

        print(results["streaming"])
        print(results["materialized"])

        return EXIT_SUCCESS

    except Exception as e:
        print(f"Error: Benchmark failed - {e}", file=sys.stderr)
        return EXIT_ERROR


def create_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="batchpipe",
        description="流式批处理流水线 - 使用 collections 和 itertools 构建",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 从 stdin 读取，按键聚合
  echo -e "user1: login\\nuser2: logout\\nuser1: action" | python -m batchpipe run --parse-fields --aggregate

  # 从文件读取，滑动窗口统计
  python -m batchpipe run --input-file data.txt --window --window-size 10

  # 基准测试
  python -m batchpipe bench --records 100000 --seed 42
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    run_parser = subparsers.add_parser("run", help="运行流水线处理输入")
    run_parser.add_argument(
        "--input-file",
        "-i",
        type=str,
        default=None,
        help="输入文件路径（默认从 stdin 读取）",
    )
    run_parser.add_argument(
        "--delimiter",
        "-d",
        type=str,
        default="\n",
        help="记录分隔符（默认换行符）",
    )
    run_parser.add_argument(
        "--no-lowercase",
        action="store_true",
        help="不转换为小写",
    )
    run_parser.add_argument(
        "--skip-blank",
        action="store_true",
        default=True,
        help="跳过空白行（默认启用）",
    )
    run_parser.add_argument(
        "--no-skip-blank",
        action="store_false",
        dest="skip_blank",
        help="不跳过空白行",
    )
    run_parser.add_argument(
        "--parse-fields",
        action="store_true",
        help="解析 key:value 格式字段",
    )
    run_parser.add_argument(
        "--field-sep",
        type=str,
        default=":",
        help="字段分隔符（默认:）",
    )
    run_parser.add_argument(
        "--aggregate",
        action="store_true",
        help="启用聚合（按键统计计数）",
    )
    run_parser.add_argument(
        "--aggregate-func",
        type=str,
        default="count",
        choices=["count", "sum", "collect"],
        help="聚合函数（默认 count）",
    )
    run_parser.add_argument(
        "--window",
        action="store_true",
        help="启用滑动窗口统计",
    )
    run_parser.add_argument(
        "--window-size",
        type=int,
        default=5,
        help="滑动窗口大小（默认 5）",
    )
    run_parser.add_argument(
        "--format",
        "-f",
        type=str,
        default="plain",
        choices=["plain", "json", "csv", "key_value"],
        help="输出格式（默认 plain）",
    )
    run_parser.add_argument(
        "--encoding",
        type=str,
        default="utf-8",
        help="输入编码（默认 utf-8）",
    )
    run_parser.add_argument(
        "--errors",
        type=str,
        default="strict",
        choices=["strict", "replace", "ignore"],
        help="解码错误处理方式（默认 strict）",
    )
    run_parser.add_argument(
        "--fail-on-empty",
        action="store_true",
        help="空输入时返回非零退出码",
    )

    bench_parser = subparsers.add_parser("bench", help="基准测试：流式 vs 物化处理")
    bench_parser.add_argument(
        "--records",
        "-n",
        type=int,
        default=100000,
        help="测试记录数（默认 100000）",
    )
    bench_parser.add_argument(
        "--seed",
        "-s",
        type=int,
        default=42,
        help="随机种子（默认 42），固定种子可重复结果",
    )
    bench_parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="预热次数（默认 1）",
    )
    bench_parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="测试次数（默认 3），取平均值",
    )

    return parser


def main() -> int:
    """主入口函数。"""
    parser = create_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return EXIT_ERROR

    if args.command == "run":
        return run_command(args)
    elif args.command == "bench":
        return bench_command(args)
    else:
        parser.print_help()
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
