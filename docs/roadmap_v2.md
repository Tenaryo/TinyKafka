# TinyKafka v2 路线图

> 主题：**性能工程专项**。基于 v1 benchmark 瓶颈分析，以 io_uring + 异步 I/O 为主线的性能优化。

## v1 瓶颈分析

B1(50K req ×1) 和 B100(50K req ×100) 吞吐完全一致（~31K msg/s），CPU 和磁盘带宽均未饱和。**瓶颈是 50K 次同步 send→recv 往返等待**——每次请求必须等响应返回才发下一个。

## Phase 1: I/O 路径优化

**目标：** 消除同步往返等待和 per-request 文件系统开销。

| # | 任务 | 描述 | 收益 |
|---|------|------|------|
| F1 | io_uring 事件循环 | epoll → io_uring，批量提交/收割 I/O 请求 | 减少 syscall 数量，降低往返延迟 |
| F2 | Request Pipeline | 不等响应就发下一个请求，recv 到达时异步匹配 | 消除空闲等待，理论吞吐 = 1/(单程延迟) |
| F3 | 文件句柄复用 | PartitionContext 保持 `ofstream` 打开 | 消除 per-batch 的 open/close 系统调用 |
| F4 | io_uring 性能对比 | 同一 workload epoll vs io_uring+pipeline 对比报告 | v2 核心交付物 |

## Phase 2: 内存 + 零拷贝

**目标：** 减少内存分配和数据拷贝。

| # | 任务 | 描述 |
|---|------|------|
| F5 | Arena Allocator | 预分配内存池，批量回收，消除散碎的 per-request 分配 |
| F6 | Response 缓冲区复用 | reactor 复用预分配的序列化缓冲区 |
| F7 | sendfile Fetch | disk→socket 零拷贝传输（大消息的高吞吐场景） |

## Phase 3: 收尾

| # | 任务 | 描述 |
|---|------|------|
| F8 | 协议解析零拷贝 | `parse_record_batch` 返回 `span<const uint8_t>` |
| F9 | v2 覆盖率审计 + 火焰图 | 全项目 ≥80%，热点定位 |

## 交付标准

- Phase 1 完成时产出 epoll vs io_uring+pipeline 吞吐对比
- 全量 140 测试通过
- 覆盖率 ≥80%
