use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info};

#[derive(Default, Debug)]
pub(crate) struct Metrics {
    count: AtomicU64,
    // Timings
    wait_sum_ns: AtomicU64,  // WS recv -> Worker start
    parse_sum_ns: AtomicU64, // JSON parsing
    logic_sum_ns: AtomicU64, // Handler/Logic/Redis
    e2e_sum_ns: AtomicU64,   // Total internal latency

    // Worst cases
    worst_wait_ns: AtomicU64,
    worst_parse_ns: AtomicU64,
    worst_logic_ns: AtomicU64,
    worst_e2e_ns: AtomicU64,
}

impl Metrics {
    pub(crate) fn observe(&self, wait_ns: u64, parse_ns: u64, logic_ns: u64) {
        let total_ns = wait_ns + parse_ns + logic_ns;
        self.count.fetch_add(1, Ordering::Relaxed);

        self.wait_sum_ns.fetch_add(wait_ns, Ordering::Relaxed);
        self.parse_sum_ns.fetch_add(parse_ns, Ordering::Relaxed);
        self.logic_sum_ns.fetch_add(logic_ns, Ordering::Relaxed);
        self.e2e_sum_ns.fetch_add(total_ns, Ordering::Relaxed);

        self.update_max(&self.worst_wait_ns, wait_ns);
        self.update_max(&self.worst_parse_ns, parse_ns);
        self.update_max(&self.worst_logic_ns, logic_ns);
        self.update_max(&self.worst_e2e_ns, total_ns);
    }

    pub(crate) fn start_metrics_collection(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                ticker.tick().await;
                let n = self.count.load(Ordering::Relaxed);
                if n == 0 {
                    continue;
                }
                let wait_sum = self.wait_sum_ns.load(Ordering::Relaxed);
                let parse_sum = self.parse_sum_ns.load(Ordering::Relaxed);
                let logic_sum = self.logic_sum_ns.load(Ordering::Relaxed);
                let e2e_sum = self.e2e_sum_ns.load(Ordering::Relaxed);

                let worst_wait = self.worst_wait_ns.load(Ordering::Relaxed);
                let worst_parse = self.worst_parse_ns.load(Ordering::Relaxed);
                let worst_logic = self.worst_logic_ns.load(Ordering::Relaxed);
                let worst_e2e = self.worst_e2e_ns.load(Ordering::Relaxed);

                info!(
                    "avg [wait: {}ns | parse: {}ns | logic: {}ns | total: {}ns] worst [wait: {}ns | parse: {}ns | logic: {}ns | total: {}ns] (n={})",
                    wait_sum / n,
                    parse_sum / n,
                    logic_sum / n,
                    e2e_sum / n,
                    worst_wait,
                    worst_parse,
                    worst_logic,
                    worst_e2e,
                    n
                );
            }
        });
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
