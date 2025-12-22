use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
#[cfg(feature = "json-output")]
use {std::fs::File, std::io, std::path::Path};

pub struct Stats {
    /// Test start time
    start_instant: Instant,
    /// Test start system time
    start: SystemTime,
    /// Durations of PUT operations
    put_duration: Histogram<u64>,
    /// Durations of GET operations
    get_duration: Histogram<u64>,
    /// Time from finishing PUT until receiving the first byte of GET response
    first_byte_latency: Histogram<u64>,
    /// Throughput for PUT operations
    put_throughput: Histogram<u64>,
    /// Throughput for GET operations
    get_throughput: Histogram<u64>,
    /// The total amount of requests executed
    requests: usize,
    /// Stats accumulated over each interval
    intervals: Vec<Interval>,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            start_instant: Instant::now(),
            start: SystemTime::now(),
            put_duration: Histogram::new(3).unwrap(),
            get_duration: Histogram::new(3).unwrap(),
            first_byte_latency: Histogram::new(3).unwrap(),
            put_throughput: Histogram::new(3).unwrap(),
            get_throughput: Histogram::new(3).unwrap(),
            requests: 0,
            intervals: vec![],
        }
    }
}

impl Stats {
    pub fn on_interval(&mut self, start: Instant, operation_stats: &OpenOperationStats) {
        let mut interval = Interval::new(start - self.start_instant, self.start_instant.elapsed());
        let mut guard = operation_stats.0.lock().unwrap();

        guard.retain(|op_stats| {
            self.record(op_stats.clone());
            interval.record_operation_stats(op_stats.clone());
            // Retain if not finished yet
            !op_stats.finished.load(Ordering::SeqCst)
        });

        self.intervals.push(interval);
    }

    fn record(&mut self, op_stats: Arc<OperationStats>) {
        if op_stats.finished.load(Ordering::SeqCst) {
            let duration = op_stats.duration.load(Ordering::SeqCst);
            let bps = throughput_bytes_per_second(duration, op_stats.data_size);

            if op_stats.is_put {
                self.put_throughput.record(bps as u64).unwrap();
                self.put_duration.record(duration).unwrap();
            } else {
                self.get_throughput.record(bps as u64).unwrap();
                self.get_duration.record(duration).unwrap();
                self.first_byte_latency
                    .record(op_stats.first_byte_latency.load(Ordering::SeqCst))
                    .unwrap();
                self.requests += 1;
            }
        }
    }

    pub fn print(&self) {
        let dt = self.start_instant.elapsed();
        let rps = self.requests as f64 / dt.as_secs_f64();

        println!("Overall stats:");
        println!(
            "RPS: {:.2} ({} requests in {:4.2?})",
            rps, self.requests, dt,
        );
        println!();

        println!("Operation metrics:\n");

        println!(
            "      │ PUT Duration    │ GET Duration     | FBL        │ PUT Throughput  │ GET Throughput"
        );
        println!(
            "──────┼─────────────────┼──────────────────┼────────────┼──────────────────┼────────────────"
        );

        let print_metric = |label: &'static str, get_metric: fn(&Histogram<u64>) -> u64| {
            println!(
                " {} │ {:>15.2?} │ {:>17.2?} │  {:>9.2?} │ {:12.2} Mb/s │ {:13.2} Mb/s",
                label,
                Duration::from_micros(get_metric(&self.put_duration)),
                Duration::from_micros(get_metric(&self.get_duration)),
                Duration::from_micros(get_metric(&self.first_byte_latency)),
                get_metric(&self.put_throughput) as f64 * 8.0 / 1000.0 / 1000.0,
                get_metric(&self.get_throughput) as f64 * 8.0 / 1000.0 / 1000.0,
            );
        };

        print_metric("AVG ", |hist| hist.mean() as u64);
        print_metric("P0  ", |hist| hist.value_at_quantile(0.00));
        print_metric("P10 ", |hist| hist.value_at_quantile(0.10));
        print_metric("P50 ", |hist| hist.value_at_quantile(0.50));
        print_metric("P90 ", |hist| hist.value_at_quantile(0.90));
        print_metric("P100", |hist| hist.value_at_quantile(1.00));
        println!();
    }

    #[cfg(feature = "json-output")]
    pub fn print_json(&self, path: &Path) -> io::Result<()> {
        match path {
            path if path == Path::new("-") => json::print(self, std::io::stdout()),
            _ => {
                let file = File::create(path)?;
                json::print(self, file)
            }
        }
        Ok(())
    }
}

/// Statistics for the currently open operations
#[derive(Clone, Default)]
pub struct OpenOperationStats(Arc<Mutex<Vec<Arc<OperationStats>>>>);

impl OpenOperationStats {
    pub fn new_put(&self, data_size: u64) -> Arc<OperationStats> {
        let put_stats = OperationStats {
            data_size,
            bytes: Default::default(),
            is_put: true,
            finished: Default::default(),
            duration: Default::default(),
            first_byte_latency: Default::default(),
        };
        let put_stats = Arc::new(put_stats);
        self.push(put_stats.clone());
        put_stats
    }

    pub fn new_get(&self, data_size: u64) -> Arc<OperationStats> {
        let get_stats = OperationStats {
            data_size,
            bytes: Default::default(),
            is_put: false,
            finished: Default::default(),
            duration: Default::default(),
            first_byte_latency: Default::default(),
        };
        let get_stats = Arc::new(get_stats);
        self.push(get_stats.clone());
        get_stats
    }

    fn push(&self, op_stats: Arc<OperationStats>) {
        self.0.lock().unwrap().push(op_stats);
    }
}

pub struct OperationStats {
    data_size: u64,
    bytes: std::sync::atomic::AtomicUsize,
    is_put: bool,
    finished: AtomicBool,
    duration: AtomicU64,
    first_byte_latency: AtomicU64,
}

impl OperationStats {
    pub fn on_first_byte(&self, latency: Duration) {
        self.first_byte_latency
            .store(latency.as_micros() as u64, Ordering::SeqCst);
    }

    pub fn on_bytes(&self, bytes: usize) {
        self.bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn finish(&self, duration: Duration) {
        self.duration
            .store(duration.as_micros() as u64, Ordering::SeqCst);
        self.finished.store(true, Ordering::SeqCst);
    }
}

struct Interval {
    operations: Vec<OperationIntervalStats>,
    period: IntervalPeriod,
}

impl Interval {
    fn new(start: Duration, end: Duration) -> Self {
        let period = IntervalPeriod {
            start: start.as_secs_f64(),
            end: end.as_secs_f64(),
            seconds: (end - start).as_secs_f64(),
        };

        Self {
            operations: vec![],
            period,
        }
    }

    fn record_operation_stats(&mut self, op_stats: Arc<OperationStats>) {
        let bytes = op_stats.bytes.swap(0, Ordering::SeqCst);
        self.operations.push(OperationIntervalStats {
            bytes,
            is_put: op_stats.is_put,
        })
    }
}

struct IntervalPeriod {
    start: f64,
    end: f64,
    seconds: f64,
}

struct OperationIntervalStats {
    bytes: usize,
    is_put: bool,
}

fn throughput_bytes_per_second(duration_in_micros: u64, size: u64) -> f64 {
    (size as f64) / (duration_in_micros as f64 / 1000000.0)
}

#[cfg(feature = "json-output")]
mod json {
    use crate::stats;
    use crate::stats::{Stats, OperationIntervalStats};
    use serde::{self, Serialize, Serializer, ser::SerializeStruct};
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    pub(crate) fn print<W: Write>(stats: &Stats, out: W) {
        let report = Report {
            start: Start {
                timestamp: stats.start,
            },
            intervals: &stats
                .intervals
                .iter()
                .map(Interval::from_stats_interval)
                .collect(),
        };

        serde_json::to_writer(out, &report).unwrap();
    }

    #[derive(Serialize)]
    struct Report<'a> {
        start: Start,
        intervals: &'a Vec<Interval>,
    }

    #[derive(Serialize)]
    struct Start {
        #[serde(serialize_with = "serialize_timestamp")]
        timestamp: SystemTime,
    }

    fn serialize_timestamp<S>(time: &SystemTime, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut state = s.serialize_map(Some(1))?;
        state.serialize_entry(
            "timesecs",
            &time.duration_since(UNIX_EPOCH).unwrap().as_secs(),
        )?;
        state.end()
    }

    struct Interval {
        operations: Vec<Operation>,
        put_sum: Sum,
        get_sum: Sum,
    }

    impl Interval {
        fn from_stats_interval(interval: &stats::Interval) -> Self {
            Self {
                operations: interval
                    .operations
                    .iter()
                    .map(|stats| Operation::from_operation_interval_stats(stats, &interval.period))
                    .collect(),
                put_sum: Sum::from_operation_interval_stats(
                    &interval.operations,
                    &interval.period,
                    true,
                ),
                get_sum: Sum::from_operation_interval_stats(
                    &interval.operations,
                    &interval.period,
                    false,
                ),
            }
        }
    }

    impl Serialize for Interval {
        fn serialize<S>(
            &self,
            serializer: S,
        ) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_struct("Interval", 3)?;
            state.serialize_field("operations", &self.operations)?;
            state.serialize_field("put_sum", &self.put_sum)?;
            state.serialize_field("get_sum", &self.get_sum)?;
            state.end()
        }
    }

    #[derive(Serialize)]
    struct Operation {
        start: f64,
        end: f64,
        seconds: f64,
        bytes: usize,
        bits_per_second: f64,
        is_put: bool,
    }

    impl Operation {
        fn from_operation_interval_stats(
            stats: &stats::OperationIntervalStats,
            period: &stats::IntervalPeriod,
        ) -> Self {
            let bits_per_second = stats.bytes as f64 * 8.0 / period.seconds;

            Self {
                start: period.start,
                end: period.end,
                seconds: period.seconds,
                bytes: stats.bytes,
                bits_per_second,
                is_put: stats.is_put,
            }
        }
    }

    #[derive(Serialize)]
    struct Sum {
        start: f64,
        end: f64,
        seconds: f64,
        bytes: usize,
        bits_per_second: f64,
        is_put: bool,
    }

    impl Sum {
        fn from_operation_interval_stats(
            stats: &[OperationIntervalStats],
            period: &stats::IntervalPeriod,
            is_put: bool,
        ) -> Self {
            let bytes = stats
                .iter()
                .filter(|stat| stat.is_put == is_put)
                .map(|stat| stat.bytes)
                .sum();
            let bits_per_second = bytes as f64 * 8.0 / period.seconds;

            Self {
                start: period.start,
                end: period.end,
                seconds: period.seconds,
                bytes,
                bits_per_second,
                is_put,
            }
        }
    }
}

