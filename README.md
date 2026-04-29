# Batchpipe

一个使用 Python `collections` 和 `itertools` 构建的小型流式批处理流水线包。

## 特性

- **流式处理**: 主路径使用生成器/迭代器链接，不一次性将整份输入读入内存
- **多阶段流水线**: 支持组合多个处理阶段
- **命令行接口**: `run` 和 `bench` 两个子命令
- **基准测试**: 对比流式处理 vs 物化处理的性能差异

## 安装

```bash
# 从源码安装（开发模式）
pip install -e .

# 或者直接作为模块使用
python -m batchpipe
```

## 快速开始

### run 子命令

从 stdin 或文件读取输入，经过流水线处理后输出到 stdout。

```bash
# 从 stdin 读取，解析 key:value 格式并聚合
echo -e "user1: login\nuser2: logout\nuser1: action" | python -m batchpipe run --parse-fields --aggregate

# 从文件读取，滑动窗口统计
python -m batchpipe run --input-file data.txt --window --window-size 10

# JSON 格式输出
echo -e "a: 1\nb: 2\na: 3" | python -m batchpipe run --parse-fields --aggregate --format json
```

### bench 子命令

对比流式处理和物化处理的性能，输出两行结果便于对比。

```bash
# 基准测试（10万条记录）
python -m batchpipe bench --records 100000 --seed 42

# 输出示例:
# streaming: time=0.1234s memory=1234.56KB outputs=100
# materialized: time=0.2345s memory=5678.90KB outputs=100
```

## 命令行参数

### run 子命令

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--input-file, -i` | 输入文件路径 | 从 stdin 读取 |
| `--delimiter, -d` | 记录分隔符 | `\n`（换行符） |
| `--no-lowercase` | 不转换为小写 | - |
| `--skip-blank/--no-skip-blank` | 是否跳过空白行 | 跳过 |
| `--parse-fields` | 解析 `key:value` 格式字段 | - |
| `--field-sep` | 字段分隔符 | `:` |
| `--aggregate` | 启用聚合（按键统计计数） | - |
| `--aggregate-func` | 聚合函数: count/sum/collect | `count` |
| `--window` | 启用滑动窗口统计 | - |
| `--window-size` | 滑动窗口大小 | `5` |
| `--format, -f` | 输出格式: plain/json/csv/key_value | `plain` |
| `--encoding` | 输入编码 | `utf-8` |
| `--errors` | 解码错误处理: strict/replace/ignore | `strict` |
| `--fail-on-empty` | 空输入时返回非零退出码 | - |

### bench 子命令

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--records, -n` | 测试记录数 | `100000` |
| `--seed, -s` | 随机种子 | `42` |
| `--warmup` | 预热次数 | `1` |
| `--runs` | 测试次数（取平均值） | `3` |

## 核心设计

### 流水线阶段

标准三阶段流水线（实际四阶段）：

1. **SplitStage**: 将原始文本流分割为记录
   - 支持自定义分隔符
   - 可配置是否跳过空白行

2. **NormalizeStage**: 规范化处理
   - 大小写转换
   - 空白处理
   - 字段解析（key:value 格式）
   - 使用 `ChainMap` 合并默认配置和用户配置

3. **AggregateStage** / **SlidingWindowStage**: 聚合/统计
   - `AggregateStage`: 使用 `groupby` 按键分组，`Counter`/`defaultdict` 统计
   - `SlidingWindowStage`: 使用 `deque` 实现滑动窗口统计

4. **FormatStage**: 格式化输出
   - 支持 plain/json/csv/key_value 格式

### collections 使用

- **deque**: 滑动窗口统计（`append`/`popleft` 均为 O(1)）
- **Counter**: 计数聚合
- **defaultdict**: 分组存储
- **ChainMap**: 配置合并（默认配置 + 用户配置）

### itertools 使用

- **groupby**: 按键分组（要求输入已排序）
- **islice**: 切片/分批处理
- **chain**: 迭代器链（展平嵌套迭代器）
- **tee**: 迭代器复制（用于统计或多路处理）

## 基准测试度量口径

### 时间测量

- 使用 `time.perf_counter()` 测量墙钟时间
- 单位：秒（保留 4 位小数）
- 多次运行取平均值

### 内存测量

- 使用 `tracemalloc.get_traced_memory()[1]` 获取峰值内存
- 单位：KB（保留 2 位小数）
- **测量口径说明**：
  - `tracemalloc` 测量的是 Python 分配器追踪的内存
  - 峰值内存是执行期间达到的最大值
  - 包括所有中间对象的内存占用

### 可重复性

- 使用 `random.Random(seed)` 生成测试数据
- 固定种子时，两次运行的测试数据完全一致
- 容差说明：
  - 时间：允许 <5% 的浮动（受系统负载影响）
  - 内存：允许 <1% 的浮动（受 Python 内存分配器影响）

## 错误处理

### 退出码

- `0`: 成功
- `1`: 错误（见 stderr 详细信息）

### 错误类型

1. **UTF-8 解码失败**
   - 当输入包含无效 UTF-8 字节时
   - stderr: `Error: [Decode] Failed to decode input: ...`
   - 可通过 `--errors replace` 或 `--errors ignore` 处理

2. **空输入**
   - 当使用 `--fail-on-empty` 且没有有效输出时
   - stderr: `Error: [Input] No valid output records produced`

3. **文件未找到**
   - 当 `--input-file` 指定的文件不存在时
   - stderr: `Error: Input file not found: ...`

## 测试

运行所有测试：

```bash
pytest
```

测试覆盖：

- 流水线各阶段功能测试
- 边界条件（空输入、只有空白行）
- collections 和 itertools 使用验证
- CLI 集成测试
- 基准测试功能验证

## 项目结构

```
batchpipe/
├── __init__.py      # 包入口
├── __main__.py      # 命令行入口
├── core.py          # 核心数据结构（Record, PipelineResult, 异常）
├── pipeline.py      # 流水线框架和阶段实现
├── cli.py           # 命令行接口
└── bench.py         # 基准测试模块
tests/
├── __init__.py
└── test_batchpipe.py # 测试套件
pyproject.toml       # 包配置
README.md
```

## 示例

### 聚合统计

```bash
# 统计每个用户的操作次数
cat access.log | python -m batchpipe run --parse-fields --aggregate --format key_value
```

### 滑动窗口

```bash
# 计算每 10 个数值的滑动平均值
seq 1 100 | python -m batchpipe run --window --window-size 10
```

### 基准测试对比

```bash
# 运行基准测试并保存结果
python -m batchpipe bench --records 100000 --seed 42 > results.txt

# 查看结果
cat results.txt
# streaming: time=0.1523s memory=2345.67KB outputs=107
# materialized: time=0.2894s memory=8765.43KB outputs=107
```

## License

MIT
