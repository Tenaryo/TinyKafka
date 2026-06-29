# TinyKafka 产品路线图 v2

## 文档信息
- 版本：v2.0（草案）
- 日期：2026-06-29
- 期数：第二期

## 产品目标

> v2 定位：**纯性能工程，零新功能**。基于 v1 完成的 benchmark 体系，用 io_uring + 协程替换 epoll、存储引擎批量写入、火焰图驱动优化，产出 epoll vs io_uring 的性能对比数据。为面试提供一个完整的"从实现到优化"的技术叙事。

## v1 交付基线

v1 完成后将具备：
- 12 个 Kafka API + Consumer Group 协调（JoinGroup/SyncGroup/Heartbeat/Rebalance）
- Per-Partition 执行引擎 + 分段日志 + 稀疏索引
- Apache Kafka 同 workload 吞吐/延迟对比数据
- 128+ 测试用例 + 94%+ 覆盖率

v2 在此基础之上不增加任何新协议或新功能。

## Milestone 计划

### M6: 存储性能优化

**主题概述：** 消除当前 per-record open/write/close 的开销——单次 Produce batch 内所有 record 只做一次文件系统调用。引入文件句柄复用和内存池，使存储层不再是 benchmark 瓶颈。

**功能需求清单：**

| # | 功能需求 | 描述 | 价值 | 可行性 | 依赖 |
|---|---------|------|------|--------|------|
| F47 | 批量写入 | 替换 per-record open/write/close 为单次 open → 批量 write → close | P0 | 中风险 | v1 M5 |
| F48 | 文件句柄复用 | PartitionContext 保持 log 文件句柄打开，消除重复 open/close | P0 | 低风险 | F47 |
| F49 | 内存池/Buffer Arena | 预分配读写缓冲区池，消除 per-request 分配 | P1 | 中风险 | F48 |
| F50 | 存储性能对比 | 批量写入前后吞吐对比基准 | P0 | 低风险 | F47 |

**依赖：** v1 M5（需 v1 分段日志和稀疏索引导入后启动）

---

### M7: io_uring + C++20 协程

**主题概述：** 将 I/O 引擎从 epoll 替换为 io_uring 并用 C++20 协程重构异步代码。io_uring 通过 submission/completion queue 共享内存环形缓冲实现批量 I/O、零拷贝、极低 syscall 开销；协程在此之上用 `co_await` 消除回调。

**功能需求清单：**

| # | 功能需求 | 描述 | 价值 | 可行性 | 依赖 |
|---|---------|------|------|--------|------|
| F51 | io_uring 事件循环 | epoll → io_uring：multishot accept、read/write 批量提交和收割 | P0 | 高 | v1 M5 |
| F52 | C++20 协程集成 | 封装 `co_await` I/O 原语，请求处理变为同步风格 | P0 | 高 | F51 |
| F53 | io_uring 性能对比 | 同一 workload 下 epoll vs io_uring+协程 的吞吐和延迟对比报告 | P0 | 低 | F52 |

**高风险项：**
- **F51**：io_uring 的 sqe/cqe 生命周期管理——sqe 提交后不可再修改、cqe 收割后需正确释放、fixed buffer 注册和注销与生命周期耦合
- **F52**：协程的 `promise_type` 和 `awaiter` 体系与 io_uring 的完成通知机制对接——需要在 cqe 收割循环中唤醒挂起的协程

**依赖：** v1 M5（需 v1 的完整 benchmark 数据和基础设施就绪后启动）

---

### M8: 火焰图 + Benchmark 驱动优化 + 安全加固

**主题概述：** 利用 perf + FlameGraph 定位 CPU 热点函数，针对性优化。引入模糊测试加固协议解析器。作为 v2 收尾，产出优化前后完整对比报告。

**功能需求清单：**

| # | 功能需求 | 描述 | 价值 | 可行性 | 依赖 |
|---|---------|------|------|--------|------|
| F54 | 火焰图 pipeline | perf record + FlameGraph 一键采集，定位 CPU 热点 | P0 | 低 | v1 M5 |
| F55 | 热点优化 | 基于火焰图定位 top-3 热点函数 → 针对性优化并产出前后对比 | P0 | 中 | F54 |
| F56 | 模糊测试 | libFuzzer / AFL++ 对协议解析器做 fuzz，修复发现的 crash/UB | P1 | 中 | v1 M5 |
| F57 | v2 覆盖率审计 | 全项目覆盖率 ≥80% | P0 | 低 | F55 |

**依赖：** v1 M5（需 v1 benchmark 体系和存储引擎就绪后启动）

---

## 面试叙事线

```
v1: "从零实现 Kafka broker，12 个 API + Consumer Group + 存储引擎 + Kafka 对比"
v2: "io_uring + 协程替换 epoll，火焰图定位瓶颈优化 X%，fuzzing 加固协议层"
```

