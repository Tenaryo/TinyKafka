# TinyKafka v1 交付清单

> C++23 从零实现的 Kafka Broker。零外部依赖，仅 STL + POSIX sockets + epoll。

## 已完成模块

### M1: 工程质量基线

- 代码覆盖率体系（gcov/lcov，HTML 报告，CI ≥80% 门禁）
- 多编译器 CI 矩阵（GCC 13/14 + Clang 18，Debug/Release + ASan/UBSan/TSan）
- clang-tidy 静态分析（零告警）
- Kafka 风格 properties 配置文件 + CLI `--key=value` 覆盖
- 结构化日志（DEBUG/INFO/WARN/ERROR，含时间戳、线程 ID）
- Pre-commit hook（clang-format + clang-tidy）

### M2: Per-Partition Execution Engine

- epoll I/O 多路复用事件循环（替换 thread-per-connection）
- PartitionContext：每个 partition 独立执行上下文，同 partition 写操作串行化
- 请求路由：Produce/Fetch 统一通过 PartitionContext 处理
- 连接生命周期管理：背压控制（send buffer 上限 4MB）
- Broker 级 Metrics：请求数、字节吞吐、错误数、活跃连接，30s 定期日志

### M3: Producer 端到端 + Benchmark

- 新增协议 API：Metadata(3)、ListOffsets(2)
- Record Batch v2 解析器（164 LOC，支持多 batch 串联解析）
- Produce 响应完善（`log_append_time_ms` 真实时间戳）
- Benchmark 框架：`producer_bench` + `consumer_bench` + `run_all.sh` 一键全流程
- Producer 吞吐基准数据（4 消息大小 × 50K msgs）

### M4: Consumer Group 协调

- 新增协议 API：FindCoordinator(10)、JoinGroup(11)、SyncGroup(14)、Heartbeat(12)、LeaveGroup(13)、OffsetFetch(9)、OffsetCommit(8)
- GroupCoordinator 独立模块：成员管理 + generation 递增 + member_id 生成
- Group 状态机：Empty → AwaitingSync → Stable 转换
- Session 超时管理：`last_heartbeat` 时间戳 + `erase_if` 超时驱逐
- Rebalance 流程：generation 语义修正（Stable 时触发递增，AwaitingSync 时不变）

### M5: 存储引擎升级

- 日志分段 Segment Roll（`segment_bytes` 阈值 + `{offset:020d}.log` 文件命名）
- 稀疏 Offset 索引（每 1000 条采样，内存 index）
- 按需读取 Fetch（offset + max_bytes，index 定位 + seek）
- 批量写入优化（produce() 内单次 open + 批量 write）

### 架构整理（v1 收尾）

- Broker::handle() 拆分为 MetadataHandler + RecordHandler（297→48 LOC）
- Serializer 补齐缺失协议字段（MetadataResponse/OffsetCommit/OffsetFetch）
- Benchmark 补全多连接并发（C4/C8/B10/B100 场景）
- Fetch OOM 修复（读取限流 10MB）+ PartitionLog 存储抽象
- 测试 helper 提取到 `tests/common/test_helpers.hpp`

## 最终交付

| 指标 | 数值 |
|------|------|
| 支持 API Key | 13 个（0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 18, 75） |
| 测试用例 | 140 个，覆盖率 94.9% |
| 核心代码 | ~3,800 LOC |
| 外部依赖 | 零（仅 C++23 STL + POSIX sockets + epoll） |

### 基准数据（-O3 Release）

| Scenario | msg/s | MB/s | P50 |
|----------|-------|------|-----|
| 100B | 30,884 | 2.9 | 30μs |
| 1KB | 30,438 | 29.7 | 30μs |
| 10KB | 26,260 | 256.4 | 34μs |
| 100KB | 9,962 | 972.9 | 79μs |
| 1KB × C4 | 112,212 | 109.6 | 32μs |
| 1KB × C8 | 128,947 | 125.9 | 59μs |
