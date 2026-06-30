# TinyKafka v2 路线图

> 主题：**性能工程专项**。v1 瓶颈：50K 次同步往返等待。

## Phase 1: 消除同步往返 + 文件系统开销 ✅ 已完成

| # | 任务 | 描述 | 结果 |
|---|------|------|------|
| F1 | Pipeline | EPOLLIN\|EPOLLOUT + write_queue_ | B1 +12%, C8 +14% |
| F2 | 文件句柄复用 | ofstream 跨 produce() 保持打开 | C4 +21% |
| F3 | 缓冲区复用 | per-connection resp_pool | 减少 alloc |

## Phase 2: io_uring splice 零拷贝 Fetch

> 注：splice 不需要协程——单次 io_uring_submit + 等 CQE，纯同步操作。

| # | 任务 | 描述 |
|---|------|------|
| F4 | io_uring splice | Fetch 大响应：response header → splice(disk_fd, socket_fd) → 零拷贝传输 |

## Phase 3: 收尾

| # | 任务 | 描述 |
|---|------|------|
| F5 | Arena Allocator | 预分配内存池 |
| F6 | v2 覆盖率审计 | ≥80% |

## 交付标准

- 全量 140 测试通过
- 覆盖率 ≥80%
