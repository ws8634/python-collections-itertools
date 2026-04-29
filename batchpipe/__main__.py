"""Batchpipe 命令行入口。

使用方式:
    python -m batchpipe [command] [options]

命令:
    run   - 运行流水线处理输入
    bench - 基准测试：流式 vs 物化处理

示例:
    python -m batchpipe run --help
    python -m batchpipe bench --help
"""

import sys

from batchpipe.cli import main


if __name__ == "__main__":
    sys.exit(main())
