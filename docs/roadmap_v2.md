# TinyKafka v2 交付清单

> v1 瓶颈分析：50K 次同步 send→recv 往返等待。CPU 和磁盘均未饱和。

## Phase 1: I/O 路径优化

### Pipeline（EPOLLIN | EPOLLOUT + write_queue_）

- `handle_read` 处理完请求后同时注册 EPOLLIN 和 EPOLLOUT
- 写响应期间仍可接收下一个请求数据
- 新响应通过 `write_queue_` 排队，`handle_write` FIFO 依次发送
- 收益：B1 +12%（34K msg/s）

### 文件句柄复用

- `PartitionContext` 跨 `produce()` 调用保持 fd 打开
- segment roll 时 close 旧 fd → open 新 fd
- 消除 per-batch 的 open/close 系统调用
- 收益：C4 +21%（128K msg/s）

### 响应缓冲区复用

- 每个 Connection 预分配 `resp_pool` 缓冲区
- `serialize()` 返回值 move 入池，后续请求复用已分配容量

## Phase 2: io_uring splice 零拷贝

### io_uring 基础设施

- `io_uring_queue_init` 创建 ring
- `io_uring_peek_cqe` 非阻塞收割 CQE

### splice Fetch 零拷贝

- Fetch 大响应（records > 64KB）：header send → `io_uring_prep_splice(disk_fd, socket_fd)` → trailer send
- 数据从磁盘到 socket 不经过用户态
- `PartitionContext::splice_info()` 通过 sparse index 定位文件偏移
- `record_handler` 先判断 splice 可行再决定是否 `fetch()`

## Phase 3: 解析优化

### record_batch_count

- 新增 `record_batch_count()` 函数：仅读 v2 batch header 提取 record 数量
- `produce()` 改为调用 `record_batch_count`（16 行）替代 `parse_record_batch`（164 行）
- 不再解析 record body（key、value、headers），只取 count 用于 offset 递增
- 收益：10KB +13%

## 最终交付

| 指标 | v1 | v2 |
|------|:--:|:--:|
| B1 100B | 30,884 | 34,344 |
| B1 1KB | 30,438 | 28,039 |
| C4 | 112,212 | 128,001 |
| C8 | 128,947 | 148,686 |
| 覆盖率 | 94.9% | 93.3% |
| 测试 | 140 | 140 |

## 尝试后回退的方案

| 方案 | 原因 |
|------|------|
| 协程（Task<> + co_await） | frame 分配开销 ~20μs > I/O 等待 ~1μs，纯负收益 |
| Arena Allocator | `arena.allocate()` 分支判断开销 > 省掉的 per-vector alloc |
| io_uring 异步读写 | 同步 page cache write 即返回(1μs)，io_uring 等 CQE 反而更慢 |
