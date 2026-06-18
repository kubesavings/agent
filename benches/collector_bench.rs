use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

// Re-export the public functions we want to benchmark.
// The functions live in the `collector` module of the `kubesavings-agent` crate.
use kubesavings_agent::collector::{monthly_cost, parse_cpu_to_millicores, parse_memory_to_mib};

// ── parse_cpu_to_millicores ───────────────────────────────────────────────────

fn bench_parse_cpu(c: &mut Criterion) {
    let inputs = [
        ("millicores", "500m"),
        ("whole_cores", "2"),
        ("fractional_cores", "1.5"),
        ("nanocores", "1000000000n"),
        ("zero", "0"),
    ];

    let mut group = c.benchmark_group("parse_cpu_to_millicores");
    for (label, input) in inputs {
        group.bench_with_input(BenchmarkId::from_parameter(label), input, |b, s| {
            b.iter(|| parse_cpu_to_millicores(black_box(s)))
        });
    }
    group.finish();
}

// ── parse_memory_to_mib ───────────────────────────────────────────────────────

fn bench_parse_memory(c: &mut Criterion) {
    let inputs = [
        ("Mi", "512Mi"),
        ("Gi", "4Gi"),
        ("Ki", "1048576Ki"),
        ("bytes", "536870912"),
        ("Ti", "1Ti"),
    ];

    let mut group = c.benchmark_group("parse_memory_to_mib");
    for (label, input) in inputs {
        group.bench_with_input(BenchmarkId::from_parameter(label), input, |b, s| {
            b.iter(|| parse_memory_to_mib(black_box(s)))
        });
    }
    group.finish();
}

// ── monthly_cost ──────────────────────────────────────────────────────────────

fn bench_monthly_cost(c: &mut Criterion) {
    let cases: &[(u32, u32, u32)] = &[
        (1000, 1024, 1),
        (500, 512, 3),
        (2000, 8192, 10),
        (100, 128, 50),
    ];

    let mut group = c.benchmark_group("monthly_cost");
    for (cpu_m, mem_mi, replicas) in cases {
        let label = format!("cpu={cpu_m}m_mem={mem_mi}Mi_rep={replicas}");
        group.bench_with_input(
            BenchmarkId::from_parameter(&label),
            &(*cpu_m, *mem_mi, *replicas),
            |b, &(cpu, mem, rep)| {
                b.iter(|| monthly_cost(black_box(cpu), black_box(mem), black_box(rep)))
            },
        );
    }
    group.finish();
}

// ── batch workload parsing ────────────────────────────────────────────────────
//
// Simulates parsing a realistic snapshot: 100 workloads, each with CPU and
// memory resource strings that look like real Kubernetes values.

fn bench_batch_parse(c: &mut Criterion) {
    let cpu_strings: Vec<String> = (0..100).map(|i| format!("{}m", 100 + i * 50)).collect();
    let mem_strings: Vec<String> = (0..100).map(|i| format!("{}Mi", 128 + i * 64)).collect();

    c.bench_function("batch_parse_100_workloads", |b| {
        b.iter(|| {
            let mut total_cpu = 0u32;
            let mut total_mem = 0u32;
            for (cpu_s, mem_s) in cpu_strings.iter().zip(mem_strings.iter()) {
                total_cpu += parse_cpu_to_millicores(black_box(cpu_s));
                total_mem += parse_memory_to_mib(black_box(mem_s));
            }
            (total_cpu, total_mem)
        })
    });
}

criterion_group!(
    benches,
    bench_parse_cpu,
    bench_parse_memory,
    bench_monthly_cost,
    bench_batch_parse,
);
criterion_main!(benches);
