# async_executor 逻辑与超低延迟 / 无锁竞争 审查

## 1. 逻辑正确性

- **execute_parallel 流程**：预计算 task_configs → 建一次 shared + collector → 选 queue/notify（专属池或 tokio 池）→ 填 tip_cache、组 SwqosJob、全部 push → notify_waiters → 根据 wait_transaction_confirmed 调 wait_for_success 或 wait_for_all_submitted。逻辑正确。
- **ResultCollector**：submit 用无锁 ArrayQueue + 原子标志；wait_for_success 先看 success_flag/landed_failed_flag 再 drain results。先成功即返回，未 drain 到的后续结果会随 collector 丢弃，符合「任一成功即返回」语义。
- **ensure_swqos_pool**：SWQOS_WORKERS_STARTED 用 swap 保证只初始化一次；队列由 execute_parallel 侧 get_or_init，再传给 ensure_swqos_pool，先起 worker 再 push，顺序正确。
- **ensure_dedicated_pool**：在 Mutex 内判空、创建 queue/notify、spawn 线程、存 JoinHandles，返回 (queue, notify)。首次调用持锁完成初始化，后续调用每次持锁取 (queue, notify) 再返回。
- **tip_cache**：用 `Arc::as_ptr(&swqos_client)` 做 key，按 client 身份去重，正确。

## 2. 锁竞争与超低延迟

- **DEDICATED_POOL 的 Mutex**：专属线程池开启时，**每次** execute_parallel 都会调 ensure_dedicated_pool，从而 **每次** 对 DEDICATED_POOL 加锁一次，仅为了读已有的 (queue, notify)。高并发下会成为争用点。
- **优化**：初始化完成后，queue/notify 存到 OnceCell，热路径只做 OnceCell::get + Arc::clone，不再碰 Mutex。
- **其余**：ArrayQueue（无锁 MPMC）、ResultCollector（ArrayQueue + 原子变量）、Notify 均无额外锁，合适。

## 3. 已实现的优化

- **专属池热路径无锁**：`DEDICATED_QUEUE` / `DEDICATED_NOTIFY` 改为 `OnceCell` 存储；`DEDICATED_INIT` 仅存 `JoinHandle` 并在初始化时持锁。热路径先读 OnceCell，命中则直接 `Arc::clone` 返回，不再加锁；未命中时再持锁做一次性初始化并 set OnceCell。

## 4. 其他说明

- **wait_for_success 与 drain**：先读 `success_flag` 再 `while let Some(...) = results.pop()` 时，若某 worker 刚 store(success) 尚未 push(result)，可能本轮 drain 为空，则 `!signatures.is_empty()` 不成立，不会 return，下一轮轮询会再 drain，逻辑正确。
- **SWQOS_QUEUE / SWQOS_NOTIFY**：tokio 池侧已用 OnceCell，无每次加锁。
