.PHONY: bench bench-baseline bench-check perf-test perf-validate

# Run benchmarks and save as baseline
bench-baseline:
	cargo bench --message-format=json | tee baseline_bench.json

# Run benchmarks and check for regressions
bench-check:
	cargo bench --message-format=json > current_bench.json
	./scripts/check_regression.sh current_bench.json

# Simple performance validation
perf-check:
	./scripts/perf_check.sh

# Run performance regression tests
perf-test:
	cargo test --test performance_regression --release

# Full performance validation
perf-validate: perf-test perf-check
	@echo "✅ All performance checks passed"
# Quick benchmark run
bench:
	cargo bench

git remote set-url origin git@github.com:nasrullo/blazecashe.git