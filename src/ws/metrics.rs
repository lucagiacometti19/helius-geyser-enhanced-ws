use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;
use tracing::debug;

#[derive(Default, Debug)]
pub(crate) struct Metrics {
    count: AtomicU64,
    // Separate fields for each timing type
    serde_sum_ns: AtomicU64,    // JSON deserialization time
    parsedtx_sum_ns: AtomicU64, // Structure extraction time
    queue_sum_ns: AtomicU64,    // Time in queue before processing

    // Track worst cases separately
    worst_serde_ns: AtomicU64,
    worst_parsedtx_ns: AtomicU64,
    worst_queue_ns: AtomicU64,
}

impl Metrics {
    pub(crate) fn observe(&self, serde_ns: u64, parsedtx_ns: u64, queue_ns: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.serde_sum_ns.fetch_add(serde_ns, Ordering::Relaxed);
        self.parsedtx_sum_ns
            .fetch_add(parsedtx_ns, Ordering::Relaxed);
        self.queue_sum_ns.fetch_add(queue_ns, Ordering::Relaxed);

        // Update worst case timings
        self.update_max(&self.worst_serde_ns, serde_ns);
        self.update_max(&self.worst_parsedtx_ns, parsedtx_ns);
        self.update_max(&self.worst_queue_ns, queue_ns);
    }

    pub(crate) fn start_metrics_collection(self: Arc<Self>) -> JoinHandle<()> {
        // Periodic reporter every 5s
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                ticker.tick().await;
                let n = self.count.load(Ordering::Relaxed);
                if n == 0 {
                    continue;
                }
                let serde_sum = self.serde_sum_ns.load(Ordering::Relaxed);
                let parsedtx_sum = self.parsedtx_sum_ns.load(Ordering::Relaxed);
                let queue_sum = self.queue_sum_ns.load(Ordering::Relaxed);

                let worst_serde = self.worst_serde_ns.load(Ordering::Relaxed);
                let worst_parsedtx = self.worst_parsedtx_ns.load(Ordering::Relaxed);
                let worst_queue = self.worst_queue_ns.load(Ordering::Relaxed);

                debug!(
                    "avg serde: {}ns | avg extract: {}ns | avg queue: {}ns | worst serde: {}ns | worst extract: {}ns | worst queue: {}ns (n={})",
                    serde_sum / n,
                    parsedtx_sum / n,
                    queue_sum / n,
                    worst_serde,
                    worst_parsedtx,
                    worst_queue,
                    n
                );
            }
        })
    }

    // This stays the same
    fn update_max(&self, slot: &AtomicU64, v: u64) {
        let mut cur = slot.load(Ordering::Relaxed);
        while v > cur
            && slot
                .compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            cur = slot.load(Ordering::Relaxed);
        }
    }
}
