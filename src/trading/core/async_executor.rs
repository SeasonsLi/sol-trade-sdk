//! Parallel executor for multi-SWQOS submit.
//!
//! - **Pool**: Pre-spawned workers (default 18); hot path only enqueues jobs (no per-call tokio::spawn).
//! - **Dedicated threads** (opt-in via TradeConfig): When `use_dedicated_sender_threads` is true, N OS threads (default 18) run sender work only, optionally pinned to cores via `sender_thread_cores`, reducing scheduling contention when sending many txs.
//! - **Arc**: Shared data is behind `Arc` so "clone" is just a refcount increment (no data copy).
//! - **Refs**: `build_transaction` takes `&Arc<..>`, `Option<&DurableNonceInfo>`, `Option<&AddressLookupTableAccount>` so the worker passes refs only (zero clone on worker path).

use anyhow::{anyhow, Result};
use crossbeam_queue::ArrayQueue;
use once_cell::sync::OnceCell;
use solana_hash::Hash;
use solana_sdk::message::AddressLookupTableAccount;
use solana_sdk::{
    instruction::Instruction, pubkey::Pubkey, signature::Keypair, signature::Signature,
};
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::{str::FromStr, sync::Arc, time::Instant};
use tokio::sync::Notify;

use fnv::FnvHasher;

type FnvHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

use crate::{
    common::nonce_cache::DurableNonceInfo,
    common::{GasFeeStrategy, SolanaRpcClient},
    swqos::{SwqosClient, SwqosType, TradeType},
    trading::{common::build_transaction, MiddlewareManager},
};

/// 与 transaction_pool::PARALLEL_SENDER_COUNT 一致，保证多路 build 不串行
const SWQOS_POOL_WORKERS: usize = 18;
const SWQOS_QUEUE_CAP: usize = 128;
const SWQOS_DEDICATED_DEFAULT_THREADS: usize = 18;

/// Shared across all jobs in one batch; built once, cloned as single Arc per job (minimal hot-path clone).
struct SwqosSharedContext {
    payer: Arc<Keypair>,
    instructions: Arc<Vec<Instruction>>,
    rpc: Option<Arc<SolanaRpcClient>>,
    address_lookup_table_account: Option<AddressLookupTableAccount>,
    recent_blockhash: Option<Hash>,
    durable_nonce: Option<DurableNonceInfo>,
    middleware_manager: Option<Arc<MiddlewareManager>>,
    protocol_name: &'static str,
    is_buy: bool,
    wait_transaction_confirmed: bool,
    with_tip: bool,
    collector: Arc<ResultCollector>,
}

/// One SWQOS submit task; only per-task data + one Arc to shared (reduces hot-path clones).
struct SwqosJob {
    shared: Arc<SwqosSharedContext>,
    tip: f64,
    unit_limit: u32,
    unit_price: u64,
    tip_account: Arc<Pubkey>,
    swqos_client: Arc<SwqosClient>,
    swqos_type: SwqosType,
    core_id: Option<core_affinity::CoreId>,
    use_affinity: bool,
}

async fn run_one_swqos_job(job: SwqosJob) {
    let s = &job.shared;
    if job.use_affinity {
        if let Some(cid) = job.core_id {
            core_affinity::set_for_current(cid);
        }
    }

    let tip_amount = if s.with_tip { job.tip } else { 0.0 };

    let transaction = match build_transaction(
        &s.payer,
        s.rpc.as_ref(),
        job.unit_limit,
        job.unit_price,
        s.instructions.as_ref(),
        s.address_lookup_table_account.as_ref(),
        s.recent_blockhash,
        s.middleware_manager.as_ref(),
        s.protocol_name,
        s.is_buy,
        job.swqos_type != SwqosType::Default,
        &job.tip_account,
        tip_amount,
        s.durable_nonce.as_ref(),
    )
    .await
    {
        Ok(tx) => tx,
        Err(e) => {
            s.collector.submit(TaskResult {
                success: false,
                signature: Signature::default(),
                error: Some(e),
                swqos_type: job.swqos_type,
                landed_on_chain: false,
                submit_done_us: crate::common::clock::now_micros(),
            });
            return;
        }
    };

    let (success, err, landed_on_chain) = match job
        .swqos_client
        .send_transaction(
            if s.is_buy { TradeType::Buy } else { TradeType::Sell },
            &transaction,
            s.wait_transaction_confirmed,
        )
        .await
    {
        Ok(()) => (true, None, true),
        Err(e) => {
            let landed = is_landed_error(&e);
            (false, Some(e), landed)
        }
    };

    let sig = transaction.signatures.first().copied().unwrap_or_default();
    s.collector.submit(TaskResult {
        success,
        signature: sig,
        error: err,
        swqos_type: job.swqos_type,
        landed_on_chain,
        submit_done_us: crate::common::clock::now_micros(),
    });
}

async fn swqos_worker_loop(queue: Arc<ArrayQueue<SwqosJob>>, notify: Arc<Notify>) {
    loop {
        if let Some(job) = queue.pop() {
            run_one_swqos_job(job).await;
        } else {
            notify.notified().await;
        }
    }
}

static SWQOS_QUEUE: OnceCell<Arc<ArrayQueue<SwqosJob>>> = OnceCell::new();
static SWQOS_NOTIFY: OnceCell<Arc<Notify>> = OnceCell::new();
static SWQOS_WORKERS_STARTED: AtomicBool = AtomicBool::new(false);

/// Dedicated OS-thread sender pool. Queue and notify are in OnceCell so hot path never takes a lock after init.
static DEDICATED_QUEUE: OnceCell<Arc<ArrayQueue<SwqosJob>>> = OnceCell::new();
static DEDICATED_NOTIFY: OnceCell<Arc<Notify>> = OnceCell::new();
/// JoinHandles kept so dedicated threads are not detached; only touched during init under lock.
static DEDICATED_INIT: Mutex<Option<Vec<std::thread::JoinHandle<()>>>> = Mutex::new(None);

fn ensure_dedicated_pool(sender_thread_cores: Option<&[usize]>) -> (Arc<ArrayQueue<SwqosJob>>, Arc<Notify>) {
    if let (Some(q), Some(n)) = (DEDICATED_QUEUE.get(), DEDICATED_NOTIFY.get()) {
        return (q.clone(), n.clone());
    }
    let mut guard = DEDICATED_INIT.lock().expect("dedicated init mutex");
    if let (Some(q), Some(n)) = (DEDICATED_QUEUE.get(), DEDICATED_NOTIFY.get()) {
        return (q.clone(), n.clone());
    }
    let n = sender_thread_cores
        .map(|v| v.len())
        .unwrap_or(SWQOS_DEDICATED_DEFAULT_THREADS)
        .min(32);
    let queue = Arc::new(ArrayQueue::new(SWQOS_QUEUE_CAP));
    let notify = Arc::new(Notify::new());
    let core_ids: Vec<core_affinity::CoreId> = sender_thread_cores
        .and_then(|indices| {
            core_affinity::get_core_ids().map(|ids| {
                indices
                    .iter()
                    .filter_map(|&i| ids.get(i).cloned())
                    .collect()
            })
        })
        .unwrap_or_default();
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let queue = queue.clone();
        let notify = notify.clone();
        let core_id = core_ids.get(i).cloned();
        let handle = std::thread::spawn(move || {
            if let Some(cid) = core_id {
                core_affinity::set_for_current(cid);
            }
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("dedicated sender runtime");
            rt.block_on(swqos_worker_loop(queue, notify));
        });
        handles.push(handle);
    }
    let _ = DEDICATED_QUEUE.set(queue.clone());
    let _ = DEDICATED_NOTIFY.set(notify.clone());
    *guard = Some(handles);
    (queue, notify)
}

fn ensure_swqos_pool(queue: Arc<ArrayQueue<SwqosJob>>) {
    if SWQOS_WORKERS_STARTED.swap(true, Ordering::AcqRel) {
        return;
    }
    let notify = SWQOS_NOTIFY.get_or_init(|| Arc::new(Notify::new())).clone();
    for _ in 0..SWQOS_POOL_WORKERS {
        tokio::spawn(swqos_worker_loop(queue.clone(), notify.clone()));
    }
}

#[repr(align(64))]
struct TaskResult {
    success: bool,
    signature: Signature,
    error: Option<anyhow::Error>,
    swqos_type: SwqosType,
    landed_on_chain: bool,
    /// Microsecond timestamp when this task finished (SWQOS returned); for per-SWQOS event→submit timing.
    submit_done_us: i64,
}

/// Check if an error indicates the transaction landed on-chain (vs network/timeout error)
fn is_landed_error(error: &anyhow::Error) -> bool {
    use crate::swqos::common::TradeError;

    // If it's a TradeError with a non-zero code, the tx landed but failed on-chain
    if let Some(trade_error) = error.downcast_ref::<TradeError>() {
        // Code 500 with "timed out" message means tx never landed
        if trade_error.code == 500 && trade_error.message.contains("timed out") {
            return false;
        }
        // Any other TradeError means the tx landed (e.g., ExceededSlippage = 6004)
        return trade_error.code > 0;
    }

    // Check error message for timeout indication
    let msg = error.to_string();
    if msg.contains("timed out") || msg.contains("timeout") {
        return false;
    }

    // Assume other errors might indicate landed tx (be conservative)
    false
}

struct ResultCollector {
    results: Arc<ArrayQueue<TaskResult>>,
    success_flag: Arc<AtomicBool>,
    landed_failed_flag: Arc<AtomicBool>, // 🔧 Tx landed on-chain but failed (nonce consumed)
    completed_count: Arc<AtomicUsize>,
    total_tasks: usize,
}

impl ResultCollector {
    fn new(capacity: usize) -> Self {
        Self {
            results: Arc::new(ArrayQueue::new(capacity)),
            success_flag: Arc::new(AtomicBool::new(false)),
            landed_failed_flag: Arc::new(AtomicBool::new(false)),
            completed_count: Arc::new(AtomicUsize::new(0)),
            total_tasks: capacity,
        }
    }

    fn submit(&self, result: TaskResult) {
        // ArrayQueue is already synchronized; no extra fence needed
        let is_success = result.success;
        let is_landed_failed = result.landed_on_chain && !result.success;

        let _ = self.results.push(result);

        if is_success {
            self.success_flag.store(true, Ordering::Release);
        } else if is_landed_failed {
            // 🔧 Tx landed but failed (e.g., ExceededSlippage) - nonce is consumed, no point waiting
            self.landed_failed_flag.store(true, Ordering::Release);
        }

        self.completed_count.fetch_add(1, Ordering::Release);
    }

    async fn wait_for_success(
        &self,
    ) -> Option<(bool, Vec<Signature>, Option<anyhow::Error>, Vec<(SwqosType, i64)>)> {
        let start = Instant::now();
        let timeout = std::time::Duration::from_secs(5);
        let poll_interval = std::time::Duration::from_millis(1000);

        loop {
            if self.success_flag.load(Ordering::Acquire) {
                let mut signatures = Vec::new();
                let mut has_success = false;
                let mut submit_timings = Vec::new();
                while let Some(result) = self.results.pop() {
                    signatures.push(result.signature);
                    submit_timings.push((result.swqos_type, result.submit_done_us));
                    if result.success {
                        has_success = true;
                    }
                }
                if has_success && !signatures.is_empty() {
                    return Some((true, signatures, None, submit_timings));
                }
            }

            // Early exit: if a tx landed but failed (e.g., ExceededSlippage),
            // nonce is consumed and other channels can't succeed - return immediately
            if self.landed_failed_flag.load(Ordering::Acquire) {
                let mut signatures = Vec::new();
                let mut landed_error = None;
                let mut submit_timings = Vec::new();
                while let Some(result) = self.results.pop() {
                    signatures.push(result.signature);
                    submit_timings.push((result.swqos_type, result.submit_done_us));
                    // Prefer the error from the tx that actually landed
                    if result.landed_on_chain && result.error.is_some() {
                        landed_error = result.error;
                    }
                }
                if !signatures.is_empty() {
                    return Some((false, signatures, landed_error, submit_timings));
                }
            }

            let completed = self.completed_count.load(Ordering::Acquire);
            if completed >= self.total_tasks {
                let mut signatures = Vec::new();
                let mut last_error = None;
                let mut any_success = false;
                let mut submit_timings = Vec::new();
                while let Some(result) = self.results.pop() {
                    signatures.push(result.signature);
                    submit_timings.push((result.swqos_type, result.submit_done_us));
                    if result.success {
                        any_success = true;
                    }
                    if result.error.is_some() {
                        last_error = result.error;
                    }
                }
                if !signatures.is_empty() {
                    return Some((any_success, signatures, last_error, submit_timings));
                }
                return None;
            }

            if start.elapsed() > timeout {
                return None;
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    fn get_first(
        &self,
    ) -> Option<(bool, Vec<Signature>, Option<anyhow::Error>, Vec<(SwqosType, i64)>)> {
        let mut signatures = Vec::new();
        let mut has_success = false;
        let mut last_error = None;
        let mut submit_timings = Vec::new();

        while let Some(result) = self.results.pop() {
            signatures.push(result.signature);
            submit_timings.push((result.swqos_type, result.submit_done_us));
            if result.success {
                has_success = true;
            }
            if result.error.is_some() {
                last_error = result.error;
            }
        }

        if !signatures.is_empty() {
            Some((has_success, signatures, last_error, submit_timings))
        } else {
            None
        }
    }

    /// 等待全部任务完成（不等待链上确认），然后收集并返回所有签名。用于「多路提交」时返回多笔签名。
    /// 轮询间隔 2ms，避免 50ms 间隔在最后一笔返回时多等几十 ms 拉高 submit 耗时。
    async fn wait_for_all_submitted(
        &self,
        timeout_secs: u64,
    ) -> Option<(bool, Vec<Signature>, Option<anyhow::Error>, Vec<(SwqosType, i64)>)> {
        let start = Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let poll_interval = std::time::Duration::from_millis(2);
        while self.completed_count.load(Ordering::Acquire) < self.total_tasks {
            if start.elapsed() > timeout {
                break;
            }
            tokio::time::sleep(poll_interval).await;
        }
        self.get_first()
    }
}

/// Execute trade on multiple SWQOS clients in parallel; returns success flag, all signatures, and last error.
pub async fn execute_parallel(
    swqos_clients: &[Arc<SwqosClient>],
    payer: Arc<Keypair>,
    rpc: Option<&Arc<SolanaRpcClient>>,
    instructions: Vec<Instruction>,
    address_lookup_table_account: Option<AddressLookupTableAccount>,
    recent_blockhash: Option<Hash>,
    durable_nonce: Option<DurableNonceInfo>,
    middleware_manager: Option<Arc<MiddlewareManager>>,
    protocol_name: &'static str,
    is_buy: bool,
    wait_transaction_confirmed: bool,
    with_tip: bool,
    gas_fee_strategy: GasFeeStrategy,
    use_core_affinity: bool,
    use_dedicated_sender_threads: bool,
    sender_thread_cores: Option<&[usize]>,
    check_min_tip: bool,
) -> Result<(bool, Vec<Signature>, Option<anyhow::Error>, Vec<(SwqosType, i64)>)> {
    let _exec_start = Instant::now();

    if swqos_clients.is_empty() {
        return Err(anyhow!("swqos_clients is empty"));
    }

    if !with_tip
        && swqos_clients
            .iter()
            .find(|swqos| matches!(swqos.get_swqos_type(), SwqosType::Default))
            .is_none()
    {
        return Err(anyhow!("No Rpc Default Swqos configured."));
    }

    let cores = core_affinity::get_core_ids().unwrap_or_default();
    let instructions = Arc::new(instructions);

    // Precompute all valid (client, gas config) combinations
    let task_configs: Vec<_> = swqos_clients
        .iter()
        .enumerate()
        .filter(|(_, swqos_client)| {
            with_tip || matches!(swqos_client.get_swqos_type(), SwqosType::Default)
        })
        .flat_map(|(i, swqos_client)| {
            let swqos_type = swqos_client.get_swqos_type();
            let gas_fee_strategy_configs = gas_fee_strategy.get_strategies(if is_buy {
                TradeType::Buy
            } else {
                TradeType::Sell
            });
            let check_tip = with_tip && !matches!(swqos_type, SwqosType::Default) && check_min_tip;
            let min_tip = if check_tip { swqos_client.min_tip_sol() } else { 0.0 };
            gas_fee_strategy_configs
                .into_iter()
                .filter(move |config| config.0 == swqos_type)
                .filter(move |config| {
                    if check_tip {
                        if config.2.tip < min_tip && crate::common::sdk_log::sdk_log_enabled() {
                            println!(
                                "⚠️ Config filtered: {:?} tip {} is below minimum required {}",
                                config.0, config.2.tip, min_tip
                            );
                        }
                        config.2.tip >= min_tip
                    } else {
                        true
                    }
                })
                .map(move |config| (i, swqos_client.clone(), config))
        })
        .collect();

    if task_configs.is_empty() {
        return Err(anyhow!("No available gas fee strategy configs"));
    }

    if is_buy && task_configs.len() > 1 && durable_nonce.is_none() {
        return Err(anyhow!("Multiple swqos transactions require durable_nonce to be set.",));
    }

    // Task preparation completed: one shared context (clone once per batch), then minimal per-task data.
    let collector = Arc::new(ResultCollector::new(task_configs.len()));
    let shared = Arc::new(SwqosSharedContext {
        payer,
        instructions,
        rpc: rpc.cloned(),
        address_lookup_table_account,
        recent_blockhash,
        durable_nonce,
        middleware_manager,
        protocol_name,
        is_buy,
        wait_transaction_confirmed,
        with_tip,
        collector: collector.clone(),
    });

    let (queue, notify) = if use_dedicated_sender_threads {
        ensure_dedicated_pool(sender_thread_cores)
    } else {
        let q = SWQOS_QUEUE.get_or_init(|| Arc::new(ArrayQueue::new(SWQOS_QUEUE_CAP)));
        ensure_swqos_pool(q.clone());
        (q.clone(), SWQOS_NOTIFY.get_or_init(|| Arc::new(Notify::new())).clone())
    };

    {
        // Cache tip_account per client (one get_tip_account/from_str per unique client per batch). Dropped before await so future stays Send.
        let mut tip_cache: FnvHashMap<*const (), Arc<Pubkey>> =
            FnvHashMap::with_capacity_and_hasher(task_configs.len(), BuildHasherDefault::default());
        for (i, swqos_client, gas_fee_strategy_config) in task_configs {
            let core_id = cores.get(i % cores.len().max(1)).copied();
            let swqos_type = swqos_client.get_swqos_type();
            let key = Arc::as_ptr(&swqos_client) as *const ();
            let tip_account = match tip_cache.get(&key) {
                Some(tip) => tip.clone(),
                None => {
                    let s = swqos_client.get_tip_account()?;
                    let tip = Arc::new(Pubkey::from_str(&s).unwrap_or_default());
                    tip_cache.insert(key, tip.clone());
                    tip
                }
            };
            let (tip, unit_limit, unit_price) = (
                gas_fee_strategy_config.2.tip,
                gas_fee_strategy_config.2.cu_limit,
                gas_fee_strategy_config.2.cu_price,
            );
            let job = SwqosJob {
                shared: shared.clone(),
                tip,
                unit_limit,
                unit_price,
                tip_account,
                swqos_client,
                swqos_type,
                core_id,
                use_affinity: use_core_affinity,
            };
            let _ = queue.push(job);
        }
    }

    notify.notify_waiters();

    // All jobs enqueued (no spawn on hot path)

    if !wait_transaction_confirmed {
        const SUBMIT_TIMEOUT_SECS: u64 = 30;
        let ret = collector.wait_for_all_submitted(SUBMIT_TIMEOUT_SECS).await.unwrap_or((
            false,
            vec![],
            Some(anyhow!("No SWQOS result within {}s", SUBMIT_TIMEOUT_SECS)),
            vec![],
        ));
        let (success, signatures, last_error, submit_timings) = ret;
        return Ok((success, signatures, last_error, submit_timings));
    }

    if let Some(result) = collector.wait_for_success().await {
        let (success, signatures, last_error, submit_timings) = result;
        Ok((success, signatures, last_error, submit_timings))
    } else {
        Err(anyhow!("All transactions failed"))
    }
}
