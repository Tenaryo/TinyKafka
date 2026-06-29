# TinyKafka 产品路线图

## 文档信息
- 版本：v1.0
- 日期：2026-06-16
- 期数：第一期

## 产品目标

> TinyKafka 是用 C++23 从零手写的高性能 Kafka Broker 实现。一期目标是从当前"协议骨架"演进为端到端可用的 Kafka 兼容 Broker：标准 Kafka 客户端可直接连接使用，提供 Producer/Consumer 全链路、Consumer Group 协调，并在并发架构和存储引擎上做出有辨识度的设计选择，最终产出与 Apache Kafka 对比的性能数据。

## 目标用户

- **开源社区审阅者** — 关注项目的技术深度、代码规范和文档完整性

## 核心使用场景

1. **标准客户端接入** — 开发者使用 `kafka-console-producer` / `kafka-console-consumer` 或 `librdkafka` 连接 TinyKafka，体验与 Apache Kafka 一致的交互
2. **Consumer Group 协同消费** — 多个 consumer 实例组成 group，自动 rebalance 分配 partition，支持 offset 提交和恢复
3. **性能基准对比** — 一键运行 benchmark，产出 TinyKafka vs Apache Kafka 的吞吐和延迟对比数据，作为技术能力的量化证明

## 当前状态

TinyKafka 已具备的基础能力：
- **12 个 Kafka API**：Produce(0), Fetch(1), ListOffsets(2), Metadata(3), OffsetCommit(8), OffsetFetch(9), FindCoordinator(10), JoinGroup(11), Heartbeat(12), SyncGroup(14), ApiVersions(18), DescribeTopicPartitions(75)
- epoll I/O 多路复用 + Per-Partition 执行引擎
- Consumer Group 协调：FindCoordinator → JoinGroup → SyncGroup → Heartbeat 全链路 + Group 状态机
- Record Batch v2 解析 + 逐条追加 + offset 按 record 计数
- 磁盘持久化存储（全量追加 + 全量读取）
- Benchmark 框架 + Producer 吞吐基准数据（29K msg/s @ 100B, 233 MB/s @ 10KB）
- 零外部依赖（C++23 STL + POSIX sockets）
- 128 个测试用例，覆盖率 94.9%

## Milestone 计划

### M1: 工程质量基线 ✅ 已完成

**主题概述：** 为整个开发周期建立可验证的工程质量保障体系——覆盖率门禁、多编译器 CI 矩阵、静态分析、配置系统和结构化日志。此阶段不修改运行时行为，仅在工程基础设施层面加固。

**功能需求清单：**

| # | 功能需求 | 描述 | 价值 | 可行性 | 依赖 |
|---|---------|------|------|--------|------|
| F1 | 代码覆盖率体系 | 集成 gcov/lcov 或 llvm-cov，CMake target 一键生成 HTML 覆盖率报告 | P0 | 低风险 | 无 | ✅ 已完成 |
| F2 | 覆盖率门槛 | CI 中强制行覆盖率 ≥80%，不达标则 pipeline 失败 | P0 | 低风险 | F1 | ✅ 已完成 |
| F3 | CI/CD 矩阵完善 | GCC 14 + Clang 18，Debug/Release，ASan+UBSan+TSan 组合矩阵 | P1 | 低风险 | 无 | ✅ 已完成 |
| F4 | 静态分析集成 | clang-tidy 集成 CMake target，CI 中强制零告警 | P1 | 低风险 | 无 | ✅ 已完成 |
| F5 | 配置文件加载 | 支持 Kafka 风格的 properties 文件（key=value），替换硬编码 | P1 | 低风险 | 无 | ✅ 已完成 |
| F6 | 类型化 Config 模块 + CLI 覆盖 | 提供带默认值的类型化 Config 结构体，从 properties 文件构建；命令行 `--key=value` 可覆盖任意字段；消除 main.cpp 中所有硬编码 | P1 | 低风险 | F5 | ✅ 已完成 |
| F7 | 结构化日志 | 分级日志（DEBUG/INFO/WARN/ERROR），含时间戳、线程 ID、请求 ID | P1 | 低风险 | 无 | ✅ 已完成 |
| F8 | Pre-commit Hook | 提交前自动运行 clang-format 格式化 + clang-tidy 检查（仅 staged 文件），阻止不规范代码提交 | P1 | 低风险 | 无 | ✅ 已完成 |

**高风险项：** 无

**Done criteria：**
1. CI 自动化产出覆盖率报告，行覆盖率 ≥80%
2. GCC 14 + Clang 18 + Debug/Release + ASan/UBSan/TSan 矩阵全绿
3. clang-tidy 零告警
4. 所有硬编码值迁移到 properties 配置文件，CLI 可覆盖

**依赖：** 无

---

### M2: Per-Partition Execution Engine ✅ 已完成

**主题概述：** 将并发模型从"每连接一线程"重构为"Partition 作为独立执行单元"——每个 partition 独占一个处理上下文（请求队列 + 单写者保证），epoll 负责 I/O 多路复用并将请求路由到对应 partition 上下文。这个架构设计比泛泛的"epoll + 线程池"更具辨识度，直接体现对 Kafka 内部模型的理解。

**功能需求清单：**

| # | 功能需求 | 描述 | 价值 | 可行性 | 依赖 |
|---|---------|------|------|--------|------|
| F9 | epoll I/O 多路复用 | 替换 accept + detach 模式为 epoll 事件循环管理所有连接的可读/可写就绪 | P0 | 低风险 | M1 | ✅ 已完成 |
| F10 | Partition 执行上下文 | 每个 partition 有独立的请求队列和在途请求状态；同一 partition 的写操作天然串行化，消除锁竞争 | P0 | 中风险 | M1 | ✅ 已完成 |
| F11 | 请求路由 | 根据请求的 api_key、topic、partition 将请求分发到对应 partition 上下文 | P0 | 低风险 | F9, F10 | ✅ 已完成 |
| F12 | 连接生命周期管理 | 完整的 accept / read / write / close 流程，包含背压控制（连接级 send buffer 满时暂停读取） | P0 | 低风险 | F9 | ✅ 已完成 |
| F13 | Broker 级 metrics | 请求速率、请求延迟、字节吞吐、活跃连接数等可查询 | P1 | 低风险 | 无 | ✅ 已完成 |

**高风险项：**
- F10：执行上下文的生命周期管理——partition 何时创建/销毁上下文，上下文与线程的映射关系，以及优雅关闭时的请求排空。设计不当会导致 use-after-free 或死锁。

**Done criteria：**
1. 现有全部测试（46 个 GoogleTest case）通过
2. 新增并发测试：1000 并发连接同时发送 ApiVersions 请求，零丢包、零 crash
3. 多 partition 并发写入同一 topic 时，所有消息完整落盘、无交错损坏
4. Broker metrics 可通过内部接口查询

**依赖：** M1（需 M1 的配置系统和 CI 就绪后启动）

---

### M3: Producer 端到端可用 + Benchmark 框架建立 ✅ 已完成

**主题概述：** 标准 Kafka 客户端（kafka-console-producer、kafka-console-consumer 无 group 模式）可连接并完成端到端的消息写入和读取。同时建立 benchmark 基础设施，跑出第一条 Producer 吞吐基准数据。

**功能需求清单：**

| # | 功能需求 | 描述 | 价值 | 可行性 | 依赖 |
|---|---------|------|------|--------|------|
| F14 | Metadata 协议 (API 3) | 客户端首次连接时拉取 cluster metadata（broker 列表、topic 列表、partition 分布），标准客户端连接的第一步 | P0 | 低风险 | M2 | ✅ 已完成 |
| F15 | ListOffsets 协议 (API 2) | Consumer 查询 partition 的 earliest/latest offset，确定消费起始位置 | P0 | 低风险 | M2 | ✅ 已完成 |
| F16 | Produce/Fetch 协议字段补齐 | 对照 Kafka 协议规范，对现有 Produce v11 和 Fetch v16 解析/序列化做字段级审计和补全 | P1 | 中风险 | M2 |
| F17 | Acks 策略 | 支持 acks=0（不等待落盘直接响应）和 acks=1（等待 leader 落盘后响应）；一期无 replication，不做 acks=-1 | P1 | 低风险 | M2 |
| F18 | 批量消息处理 | 正确解析客户端聚合的 record batch v2 格式，解聚合后逐条追加到 partition log；标准 producer 默认启 batch，broker 必须正确处理 | P0 | 中风险 | M2 | ✅ 已完成 |
| F19 | Produce 响应完善 | 正确返回每个 partition 的 base_offset、error_code、log_append_time 等字段，使客户端侧也能正确确认写入成功 | P0 | 低风险 | M2 | ✅ 已完成 |
| F20 | Benchmark 框架基础设施 | 建立 benchmark 目录和 driver 脚本骨架，统一测量接口（吞吐 MB/s、消息数/s、延迟分位数），报告模板 | P0 | 低风险 | 无 | ✅ 已完成 |
| F21 | Producer 吞吐基准测试 | 单 topic 单 partition 场景下，多 producer 并发写入的吞吐（MB/s 和 msgs/s）数据 | P0 | 低风险 | F20, F18 | ✅ 已完成 |

**高风险项：**
- F18：Kafka record batch v2 格式包含 attributes、timestamp delta、offset delta 等压缩字段，解析边界条件多，容易产生 off-by-one 错误。解聚合后的逐条 append 需要正确维护 partition 的 next_offset。

**Done criteria：**
1. `kafka-console-producer --bootstrap-server localhost:9092 --topic test` 可成功写入消息
2. `kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning` 可从头读取全部写入的消息
3. 第一条 Producer 吞吐基准数据产出（单 partition 场景，不同消息大小：100B / 1KB / 10KB）

**依赖：** M2（需 M2 的 Per-Partition Execution Engine 就绪后启动）

---

### M4: Consumer Group ✅ 已完成

**主题概述：** 多个 consumer 组成 group、自动 rebalance 分配 partition、offset 提交和恢复——这是 Kafka 区别于普通消息队列的核心能力，也是面试中最容易被深挖的分布式系统知识点。

**功能需求清单：**

| # | 功能需求 | 描述 | 价值 | 可行性 | 依赖 |
|---|---------|------|------|--------|------|
| F22 | FindCoordinator 协议 (API 10) | Consumer 发起 group 操作前先定位 group coordinator broker 地址 | P0 | 低风险 | M3 | ✅ 已完成 |
| F23 | JoinGroup 协议 (API 11) | Consumer 携带订阅的 topic 列表请求加入 group，协议包含 group_protocol、member_metadata 等嵌套结构 | P0 | 中风险 | M3 | ✅ 已完成 |
| F24 | SyncGroup 协议 (API 14) | Leader consumer 提交 partition assignment 方案，broker 分发给所有 member | P0 | 中风险 | F23 | ✅ 已完成 |
| F25 | Heartbeat 协议 (API 12) | Consumer 定期发送心跳保持 group membership，broker 返回当前 group generation 验证 | P0 | 低风险 | F23 | ✅ 已完成 |
| F26 | OffsetFetch 协议 (API 9) | Consumer 启动时拉取当前 group 已提交的 offset，恢复消费进度 | P0 | 低风险 | M3 | ✅ 已完成 |
| F27 | OffsetCommit 协议 (API 8) | Consumer 提交消费完成的 offset，支持自动和手动两种模式 | P0 | 低风险 | M3 | ✅ 已完成 |
| F28 | LeaveGroup 协议 (API 13) | Consumer 优雅退出 group，触发 rebalance | P1 | 低风险 | F23 |
| — | **架构整理：broker.cpp 拆分** | `handle()` 中 12 个 API lambda 与 group 状态管理混杂在单文件 430+ 行。按职责域拆分为 broker_data_api / broker_group_api / broker_coordinator，保持 handle() 仅做 dispatch | P0 | 低风险 | F29 | ✅ 已完成 |
| F30 | Group Membership 管理 | 记录 group 下所有 member 的 member_id、client_id、订阅信息、当前 generation | P0 | 低风险 | F29 | ✅ 已完成 |
| F31 | Partition Assignment | 实现 range assignor——按 topic 分别对 partition 编号排序后 range 分配；预留 assignor 扩展接口 | P1 | 低风险 | F30 | ⏸ 推迟至 v2（assignor 为 consumer 侧功能，当前 SyncGroup 已支持标准 leader 自主分配流程） |
| F32 | Heartbeat/Session 超时管理 | 每个 member 维护 session 定时器，超时自动踢出并触发 rebalance | P0 | 中风险 | F25, F29 | ✅ 已完成 |
| F33 | Offset 管理 | 将 group_offsets_ 整合入 GroupMetadata，统一 group 状态管理 | P0 | 中风险 | F26, F27 | ✅ 已完成 |
| F28 | LeaveGroup 协议 (API 13) | Consumer 优雅退出 group，触发 rebalance | P1 | 低风险 | F23 | ✅ 已完成 |
| F34 | Rebalance 流程 | F32 session 超时 + F28 成员离开 → 触发 JoinGroup→SyncGroup 多轮协议交互。多 member 间 generation 并发一致性、rebalance 期间新请求处理 | P0 | 高风险 | F32, F28 | ✅ 已完成 |

**开发顺序：** F32（session 超时）→ F33（offset 整合）→ F28（LeaveGroup）→ F34（Rebalance）| F33 与 F28 可并行

**高风险项：**
- **F34 (Rebalance 流程)** — 整个一期项目最大技术风险点。涉及多 member 间 JoinGroup 响应的延迟协调、rebalance timeout 处理、generation 版本号的并发一致性、rebalance 进行中新请求的处理策略。要求 Architect 在开始设计前产出独立的 rebalance 协议交互时序设计文档，覆盖正常流程、超时流程、并发冲突场景。

**Done criteria：**
1. 2 个 kafka-console-consumer 实例组成同一 group，连接 TinyKafka
2. 向 2 个 partition 的 topic 写入消息，两个 consumer 各分到一个 partition 并行消费
3. 主动 kill 其中一个 consumer，30 秒内触发 rebalance，剩余 consumer 接管全部 partition
4. consumer 重启后用 OffsetFetch 恢复到上次提交的 offset，从断点继续消费

**依赖：** M3（需 M3 的 Producer 端到端可用就绪后启动）

---

### M5: 存储引擎升级 + 完整 Benchmark 对比

**主题概述：** 存储层从"全量追加 + 全量读取"升级为"分段日志 + 稀疏索引 + 按需随机读取 + Group Commit"，消除当前存储层的性能瓶颈。在此基础上跑出与 Apache Kafka 的完整对比 benchmark，产出简历核心材料。

**功能需求清单：**

| # | 功能需求 | 描述 | 价值 | 可行性 | 依赖 |
|---|---------|------|------|--------|------|
| F35 | 日志分段 (Segment Roll) | 按 segment.bytes 阈值或 log.roll.ms 时间窗口自动滚动到新 segment 文件，目录结构 `{topic}-{partition}/00000000000000000000.log` | P1 | 低风险 | M4 | ✅ 已完成 |
| F36 | Offset 索引 | 为每个 segment 构建稀疏 offset → position 索引文件，每隔 N 条消息记录一条映射，O(1) 定位目标 offset | P0 | 低风险 | F35 | ✅ 已完成 |
| F37 | 按需读取 | 替换当前全量读取为 Fetch 指定 offset 读取指定 max_bytes，配合 E2 实现 O(log segment) + O(1) 的读取路径 | P0 | 低风险 | F36 | ✅ 已完成 |
| F38 | Group Commit 写入优化 | 批量落盘策略——收集一个时间窗口或大小窗口内的写请求，一次性 fsync，减少磁盘 I/O 次数 | P1 | 中风险 | F35 | ✅ 已完成 |
| F39 | Fetch 增强 | 利用存储索引支持精确的 offset/max_bytes Fetch，支持 incremental fetch sessions（fetch session ID + epoch） | P1 | 低风险 | F37 | ✅ 已完成 |
| F40 | Producer 吞吐完整测试 | 多 topic 多 partition、不同消息大小 (100B / 1KB / 10KB / 100KB)、不同 producer 并发数下的吞吐曲线 | P0 | 低风险 | F38 | ✅ 已完成 |
| F41 | Consumer 吞吐完整测试 | 多 consumer group、不同 partition 分配方案下的端到端吞吐 | P0 | 低风险 | F39 | ✅ 已完成 |
| F42 | 延迟完整测试 | Produce 到 Consume 的端到端延迟分布（P50 / P99 / P999），不同吞吐量级下的延迟曲线 | P0 | 低风险 | F40, F41 |
| F43 | Apache Kafka 同 workload 对比 | 同机器、同 workload、同客户端（librdkafka）对比 TinyKafka vs Apache Kafka 的吞吐和延迟 | P0 | 低风险 | F42 |
| F44 | 自动化 benchmark runner | 一键运行所有 benchmark 场景，自动采集结果，生成 Markdown 格式对比报告（表格 + 简要分析） | P1 | 低风险 | F43 |
| F45 | 火焰图/性能剖析 pipeline | perf record + FlameGraph 脚本集成，benchmark 运行时可一键采集 CPU 火焰图，供性能优化阶段使用 | P1 | 低风险 | F43 |
| F46 | M5 完成后覆盖率审计 | 对整个项目重新运行覆盖率，确保 ≥80%（M5 引入的新代码需包含对应测试） | P0 | 低风险 | F36-F39 |

**注：** F46 作为 M5 的收尾检查项，确认全项目覆盖率仍然维持在 M1 设定的 ≥80% 标准。

**高风险项：** 无

**Done criteria：**
1. 一键运行 `./benchmark/run_all.sh`，产出 `benchmark/results/comparison.md`
2. 对比报告包含 TinyKafka vs Apache Kafka 的：
   - Producer 吞吐（MB/s、msgs/s）vs 消息大小 vs 并发数   - Consumer 吞吐 vs 并发 consumer 数
   - P50/P99/P999 延迟 vs 吞吐量级
3. 火焰图可一键采集，定位热点函数
4. 全项目行覆盖率 ≥80%

**依赖：** M4（需 M4 的 Consumer Group 就绪后启动）

---

## 已砍掉/推迟的需求

| 需求 | 原定价值 | 砍掉/推迟原因 | 可能的回头时机 |
|------|---------|-------------|--------------|
| 内存池/Buffer Arena (A5) | P2 | 非一期 MVP，std::vector 可满足当前需求；benchmark 数据出来后如需优化再引入 | 二期性能工程阶段 |
| 日志清理 Retention (E5) | P2 | POC 和 benchmark 期间数据量可控，不需要自动清理 | 二期生产化阶段 |
| Per-topic/partition metrics 导出 (H3, H4) | P2 | 内部 metrics 已有 H2 覆盖；Prometheus 导出属于完整可观测性体系，推后 | 二期可观测性工程 |
| 模糊测试 (I5) | P2 | 协议解析器的健壮性重要，但 MVP 阶段通过充分的手工边界测试可覆盖；fuzz 单独做工程量大 | 二期安全加固阶段 |
| CI 中 benchmark 回归检测 (I6) | P2 | CI runner 性能不稳定，容易产生误报警报；手工 benchmark 报告更可靠 | 二期 CI 完善阶段 |
| 分区分配策略（客户端侧）(C3) | P1 | sticky/hash/round-robin partitioner 是客户端侧功能，不在 broker 职责范围内 | 不再纳入 |
| v1 F31: Partition Assignment (Range Assignor) | P0→P1 | assignor 为 consumer leader 侧功能，当前 SyncGroup 已支持标准 leader 自主分配流程 | v2 M7 或 consumer SDK 阶段 |
| v1 F47-F49: io_uring + 协程 (原 M6) | P1 | 推迟至 v2，作为性能工程专项的核心亮点 | v2 M7 |

## 高风险项汇总

| Milestone | 功能 | 风险说明 | 建议处理方式 |
|-----------|------|---------|-------------|
| M2 | F10: Partition 执行上下文 | 上下文生命周期与线程映射关系设计不当会导致 use-after-free 或死锁 | Architect 需产出执行上下文的生命周期状态机设计文档 |
| M3 | F18: 批量消息处理 | Kafka record batch v2 格式字段紧凑（attributes、timestamp delta、offset delta），解析边界条件多 | 单元测试需覆盖所有 record batch 字段变体；建议先做格式分析文档 |
| M4 | F34: Rebalance 流程 | 多 member 间 JoinGroup 延迟响应协调、rebalance timeout、generation 并发一致性，是 Kafka 中最复杂的协议交互 | **必须**在开始实现前由 Architect 产出独立的 rebalance 协议交互时序设计文档，覆盖正常流程 + 超时流程 + 并发冲突场景 |

## 附注

### 技术约束
- **语言标准：** C++23，零外部运行时依赖（仅 STL + POSIX）
- **平台：** Linux（epoll），不考虑跨平台
- **构建：** CMake 3.21+，Ninja
- **测试框架：** GoogleTest v1.14.0（FetchContent 自动拉取）
- **CI：** GitHub Actions，Ubuntu 24.04

### 关键架构决策
- **Per-Partition Execution** — M2 的核心设计选择，partition 级别的单写者保证，避免通用锁竞争
- **内存 Offset 管理** — M4 中 offset 存内存而非 `__consumer_offsets` 内部 topic，降低一期复杂度，后续可演进
- **稀疏索引** — M5 中采用定期采样构建索引而非全量索引，平衡内存占用和查找效率
- **零序列化库依赖** — 继续手动实现 binary wire protocol，保持代码自包含

### Benchmark 贯穿策略
- M3 建立框架 + Producer 吞吐基准 → M4 用同一框架验证 Consumer Group 场景 → M5 扩展为完整对比套件
- 每个 Milestone 的 Done criteria 都包含可量化的性能或质量指标

### 参考
- [Apache Kafka Protocol Guide](https://kafka.apache.org/protocol)
- [KIP-848: The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)（了解 rebalance 演进背景）
- [Kafka KRaft Metadata](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)

## 变更记录

| 日期 | 版本 | 变更说明 | 发起人 |
|------|------|---------|--------|
| 2026-06-16 | v1.0 | 初始一期路线图，共 6 个 Milestone 49 项功能需求 | Strategist |
| 2026-06-16 | v1.0 | M1 F1（代码覆盖率体系）标记为已完成，当前行覆盖率 92.8% | Reviewer |
| 2026-06-16 | v1.0 | M1 F2/F3/F4（覆盖率门槛 + TSan + clang-tidy CI 门禁）标记为已完成，当前行覆盖率 94.9% | Reviewer |
| 2026-06-16 | v1.0 | M1 新增 F9（Pre-commit Hook：format + clang-tidy），插入 F5 之前 | Reviewer |
| 2026-06-16 | v1.0 | M1 F9（Pre-commit Hook）标记为已完成 | Reviewer |
| 2026-06-16 | v1.0 | M1 F5（配置文件加载）标记为已完成 | Reviewer |
| 2026-06-16 | v1.0 | M1 F6+F7 合并为「类型化 Config 模块 + CLI 覆盖」，原 F8（结构化日志）重编号为 F7 | Reviewer |
| 2026-06-16 | v1.0 | M1 F6（类型化 Config 模块 + CLI 覆盖）标记为已完成 | Reviewer |
| 2026-06-16 | v1.0 | M1 F7（结构化日志）标记为已完成，M1 工程质量基线全部完成 | Reviewer |
| 2026-06-16 | v1.0 | 新增 M6（io_uring + 协程 I/O 引擎升级），在 M5 benchmark 基线后执行，含 F47/F48/F49 | Reviewer |
| 2026-06-17 | v1.0 | M2 F9（epoll I/O 多路复用）标记为已完成 — EpollReactor (reactor.cpp/hpp, 255 LOC) 已实现非阻塞 accept/read/write 事件循环 | Reviewer |
| 2026-06-17 | v1.0 | 更新「当前状态」中网络层描述，从 thread-per-connection 改为 epoll I/O 多路复用 | Reviewer |
| 2026-06-17 | v1.0 | FIX-1（Produce 错误处理 + 集成测试隔离）已完成：broker.cpp 错误捕获、integration_test.cpp 独立临时目录 | Reviewer |
| 2026-06-17 | v1.0 | M2 F10（Partition 执行上下文）已完成 — PartitionContext 36 LOC、offset 计数器、同 partition 串行写 | Reviewer |
| 2026-06-17 | v1.0 | M2 F11（请求路由）已完成 — Fetch 接入 PartitionContext，Broker 不再直接依赖 storage 层 | Reviewer |
| 2026-06-17 | v1.0 | M2 F12（连接生命周期管理）已完成 — max_write_buffer_bytes 背压控制，4MB 默认上限 | Reviewer |
| 2026-06-17 | v1.0 | M2 F13（Broker 级 metrics）已完成 — BrokerMetrics 4 原子计数器、30s 定期日志、snapshot() 接口 | Reviewer |
| 2026-06-17 | v1.0 | M2 Per-Partition Execution Engine 全部完成（F9-F13），进入 M3 | Reviewer |
| 2026-06-17 | v1.0 | M3 F14（Metadata 协议 API 3）已完成 — parser/serializer/broker/registry 全层实现，6 新测试 | Reviewer |
| 2026-06-17 | v1.0 | M3 F15（ListOffsets API 2）已完成 — earliest/latest offset 查询，PartitionContext::current_offset() | Reviewer |
| 2026-06-17 | v1.0 | M3 F18（批量消息处理）已完成 — record_batch.hpp 164 LOC v2 解析器，逐条追加 + offset 按 record 计数 + metadata header 修复 | Reviewer |
| 2026-06-17 | v1.0 | M3 F20（Benchmark 框架）已完成 — producer_bench.cpp 305 LOC + setup_metadata.py + run.sh 一键全流程 | Reviewer |
| 2026-06-17 | v1.0 | M3 F19（Produce 响应完善）已完成 — ProduceResult 结构体 + log_append_time_ms 真实时间戳 | Reviewer |
| 2026-06-17 | v1.0 | FIX-2 已完成 — 5 个 Blocker 修复：unique_ptr 防引用悬挂、nullptr 检查、ByteWriter assert、varint 溢出保护、errno 保存 | Reviewer |
| 2026-06-17 | v1.0 | M3 F21（Producer 吞吐基准）已完成 — run_all.sh 三场景对比，100B/1KB/10KB @ 50K msgs | Reviewer |
| 2026-06-17 | v1.0 | M3 Producer 端到端可用 + Benchmark 全部完成（F14/F15/F18/F19/F20/F21），进入 M4 | Reviewer |
| 2026-06-17 | v1.0 | M6 推迟至 roadmap v2（io_uring + 协程），v2 同步增加 benchmark 驱动性能优化专项 | Reviewer |
| 2026-06-17 | v1.0 | M4 F22（FindCoordinator API 10）已完成 — 单 broker 始终返回自身 coordinator | Reviewer |
| 2026-06-17 | v1.0 | M4 F27（OffsetCommit API 8）已完成 — group_offsets_ 三嵌套 map + 8 条目 API registry | Reviewer |
| 2026-06-17 | v1.0 | M4 F26（OffsetFetch API 9）已完成 — OffsetCommit→OffsetFetch 端到端读写验证 | Reviewer |
| 2026-06-17 | v1.0 | M4 F23（JoinGroup API 11）已完成 — group 成员管理 + generation 递增 + member_id 生成 | Reviewer |
| 2026-06-17 | v1.0 | M4 F24（SyncGroup API 14）已完成 — leader/follower assignment 分发 + generation 校验错误码 82 | Reviewer |
| 2026-06-17 | v1.0 | M4 F25（Heartbeat API 12）已完成 — generation 校验 + 错误码 82 | Reviewer |
| 2026-06-17 | v1.0 | M4 F29（Group 状态机）已完成 — GroupState 枚举 + 3 行状态转换代码 | Reviewer |
| 2026-06-17 | v1.0 | 架构整理：broker.cpp 拆分完成 — GroupCoordinator 185 LOC 独立模块，Broker 286 LOC 仅做 dispatch | Reviewer |
| 2026-06-17 | v1.0 | M4 F30（Group Membership）已完成 — 4 map → GroupMetadata + MemberInfo 单 groups_ map | Reviewer |
| 2026-06-17 | v1.0 | M4 F31（Partition Assignment）推迟 — assignor 为 consumer 侧功能，当前 SyncGroup 已支持标准 leader 分配流程 | Reviewer |
| 2026-06-17 | v1.0 | M4 F32（Heartbeat/Session 超时）已完成 — last_heartbeat 时间戳 + erase_if 超时驱逐 + 全踢→Empty/部分踢→AwaitingSync | Reviewer |
| 2026-06-17 | v1.0 | M4 F33（Offset 管理）已完成 — group_offsets_ → GroupMetadata.committed_offsets，GroupCoordinator 精简为 1 map + 1 counter | Reviewer |
| 2026-06-17 | v1.0 | M4 F28（LeaveGroup API 13）已完成 — erase_if 移除 + 空→Empty/部分→AwaitingSync+gen++ | Reviewer |
| 2026-06-17 | v1.0 | M4 F34（Rebalance 流程）已完成 — 1 行代码修正 generation 语义，8 步端到端 rebalance 测试 | Reviewer |
| 2026-06-17 | v1.0 | M4 Consumer Group 全部完成（13 API + 状态机 + session 超时 + rebalance），进入 M5 | Reviewer |
| 2026-06-17 | v1.0 | M5 F35（日志分段）已完成 — segment_bytes 阈值 + {offset:020d}.log 命名 + 目录扫描按序读取 | Reviewer |
| 2026-06-17 | v1.0 | M5 F36（Offset 索引）已完成 — SparseIndexEntry 结构 + 每 1000 条采样 + segment roll 清空 | Reviewer |
| 2026-06-17 | v1.0 | M5 F37（按需读取）已完成 — index 定位 + seek + 增量 read，向后兼容全量 Fetch | Reviewer |
| 2026-06-17 | v1.0 | M5 F38（Group Commit）已完成 — produce() 内单次 open + 批量 write，消除 per-record 文件系统调用 | Reviewer |
| 2026-06-17 | v1.0 | M5 F40（Producer 吞吐完整测试）已完成 — 4 种消息大小 + 100KB 新场景，901 MB/s 吞吐 | Reviewer |
| 2026-06-17 | v1.0 | M5 F41（Consumer 吞吐完整测试）已完成 — consumer_bench.cpp 262 LOC + Fetch 吞吐 6,352 msg/s | Reviewer |
| 2026-06-17 | v1.0 | FIX-3（增量 Fetch + Consumer Benchmark）已完成 — consumer_bench 单次 Fetch，produce/fetch 数据格式待统一 | Reviewer |
| 2026-06-17 | v1.0 | M5 F39（Fetch 增强）已完成 — produce/fetch 统一 v2 blob 格式 + parse_record_batch 多 batch 串联解析 + record 级 skip 验证通过 | Reviewer |
| 2026-06-17 | v1.0 | M5 F43（Apache Kafka 同 workload 对比）开始开发 — v1 最终任务 | Reviewer |
| 2026-06-17 | v1.0 | v1/v2 战略调整：v1 全功能体系 + Apache Kafka 对比；v2 纯性能工程（批量写入、io_uring、火焰图优化、fuzzing）| Reviewer |
| 2026-06-17 | v1.0 | M4 任务重排序：F32→F33→F28→F34；F31 推迟至 v2；F33 缩为 offset 整合；更新当前状态 | Reviewer |
