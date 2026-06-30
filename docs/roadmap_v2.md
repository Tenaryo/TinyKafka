# TinyKafka v2 路线图

> 主题：**性能工程专项**。v1 瓶颈分析：50K 次同步 send→recv 往返等待是真正瓶颈，CPU 和磁盘带宽均未饱和。

## v1 瓶颈分析

B1 和 B100 吞吐完全一致（~31K msg/s）。100×数据量同时长——CPU 和磁盘均未饱和。**瓶颈是 50K 次同步往返等待**。

## Phase 1: 消除同步往返 + 文件系统开销

| # | 任务 | 描述 | 预期收益 |
|---|------|------|---------|
| F1 | Request Pipeline | 不等响应就发下一个请求，recv 到达时异步匹配 correlation_id | 核心收益，消除空闲等待 |
| F2 | 文件句柄复用 | PartitionContext 保持 `ofstream` 打开 | 消除 per-batch open/close |
| F3 | 缓冲区预分配 | reactor 预分配 read/write 缓冲区池 | 消除 per-request alloc |

> 注：io_uring 延迟到 Phase 2。当前同步模型下 `write()` 写 page cache 即返回（~1μs），io_uring 需等 CQE 反而更慢。Pipeline 建立异步收发模型后，io_uring 异步磁盘 I/O + sendfile splice 才能发挥价值。

## Phase 2: io_uring 异步磁盘 I/O

**前置：** Phase 1 Pipeline 已建立异步收/发模型。

| # | 任务 | 描述 |
|---|------|------|
| F4 | io_uring 异步写入 | produce() 提交 write SQE，CQE 到达时确认（替代 ofstream） |
| F5 | io_uring 异步读取 | fetch() 提交 read SQE，CQE 交付数据 |
| F6 | sendfile splice | disk fd → socket fd 零拷贝（数据不经过用户态） |

## Phase 3: 内存 + 高级优化

| # | 任务 | 描述 |
|---|------|------|
| F7 | Arena Allocator | 预分配内存池，批量回收 |
| F8 | 协议解析零拷贝 | `parse_record_batch` 返回 `span<const uint8_t>` |
| F9 | v2 覆盖率审计 | 全项目 ≥80%，火焰图热点定位 |

## 交付标准

- Phase 1 完成时 Pipeline vs 同步往返吞吐对比
- 全量 140 测试通过
- 覆盖率 ≥80%
