# DME Benchmark Report

> Generated from 10 independent trials per configuration.
> All metrics reported as **mean ± 1 std dev** across trials.
> Grouping is by (algorithm, process\_count, configured\_delay, loss\_rate).
> Safety: `max_concurrent_cs` was 1 for every completed run — no mutual-exclusion
> violations observed across all suites.

## 1. Baseline  (n=5, delay=5 ms, loss=0 %)

| Algorithm | Mean wait (ms) | P95 wait (ms) | Throughput (ops/s) | Msgs sent | CV |
|---|---|---|---|---|---|
| raymond_tree | 29.9 ±7.6 | 61.7 ±11.2 | 106.62 ±13.68 | 61.0 | 0.76 |
| ricart_agrawala | 31.1 ±3.8 | 46.1 ±10.1 | 114.59 ±14.46 | 200.0 | 0.32 |
| suzuki_kasami | 22.1 ±8.9 | 41.4 ±13.7 | 135.33 ±18.90 | 115.0 | 0.58 |

## 2. Scalability  (delay=5 ms, loss=0 %)

### raymond_tree

| n | Mean wait (ms) | Throughput (ops/s) | Msgs sent |
|---|---|---|---|
| 5 | 29.9 ±7.6 | 106.62 ±13.68 | 61.0 |
| 10 | 66.4 ±7.9 | 105.12 ±11.16 | 138.8 |
| 20 | 142.0 ±11.8 | 102.50 ±6.11 | 300.0 |
| 50 | 369.5 ±26.3 | 98.99 ±4.75 | 772.6 |

### ricart_agrawala

| n | Mean wait (ms) | Throughput (ops/s) | Msgs sent |
|---|---|---|---|
| 5 | 31.1 ±3.8 | 114.59 ±14.46 | 200.0 |
| 10 | 70.0 ±6.3 | 117.91 ±8.93 | 900.0 |
| 20 | 142.0 ±7.8 | 122.12 ±6.52 | 3800.0 |
| 50 | 358.5 ±15.0 | 123.68 ±5.38 | 24500.0 |

### suzuki_kasami

| n | Mean wait (ms) | Throughput (ops/s) | Msgs sent |
|---|---|---|---|
| 5 | 22.1 ±8.9 | 135.33 ±18.90 | 115.0 |
| 10 | 57.4 ±4.9 | 136.98 ±16.48 | 495.0 |
| 20 | 113.0 ±10.5 | 146.82 ±11.55 | 1996.0 |
| 50 | 310.2 ±17.6 | 141.80 ±7.40 | 12490.0 |

## 3. Network Delay Sensitivity  (n=5, loss=0 %)

### raymond_tree  (n=5)

| Delay (ms) | Mean wait (ms) | P95 wait (ms) | Throughput (ops/s) |
|---|---|---|---|
| 1 | 5.1 ±1.8 | 11.8 ±2.8 | 169.32 ±35.30 |
| 5 | 29.9 ±7.6 | 61.7 ±11.2 | 106.62 ±13.68 |
| 10 | 43.0 ±13.8 | 110.1 ±19.9 | 80.19 ±22.11 |
| 50 | 139.6 ±64.9 | 465.8 ±145.0 | 26.42 ±7.46 |

### raymond_tree  (n=20)

| Delay (ms) | Mean wait (ms) | P95 wait (ms) | Throughput (ops/s) |
|---|---|---|---|
| 1 | 61.7 ±3.0 | 84.4 ±5.7 | 249.57 ±6.72 |
| 5 | 142.0 ±11.8 | 273.2 ±22.0 | 102.50 ±6.11 |
| 10 | 210.3 ±30.5 | 474.7 ±40.9 | 64.21 ±7.02 |
| 50 | 724.0 ±136.5 | 2118.8 ±177.9 | 16.45 ±2.36 |

### ricart_agrawala  (n=5)

| Delay (ms) | Mean wait (ms) | P95 wait (ms) | Throughput (ops/s) |
|---|---|---|---|
| 1 | 5.4 ±1.2 | 9.7 ±2.1 | 168.84 ±37.70 |
| 5 | 31.1 ±3.8 | 46.1 ±10.1 | 114.59 ±14.46 |
| 10 | 65.1 ±9.4 | 96.2 ±18.3 | 65.14 ±8.11 |
| 50 | 312.5 ±42.9 | 464.6 ±90.2 | 14.46 ±1.64 |

### ricart_agrawala  (n=20)

| Delay (ms) | Mean wait (ms) | P95 wait (ms) | Throughput (ops/s) |
|---|---|---|---|
| 1 | 45.4 ±2.6 | 60.5 ±3.1 | 326.28 ±6.47 |
| 5 | 142.0 ±7.8 | 182.8 ±12.4 | 122.12 ±6.52 |
| 10 | 265.3 ±20.9 | 339.2 ±24.8 | 67.73 ±5.62 |
| 50 | 1251.8 ±101.0 | 1618.6 ±122.8 | 14.65 ±1.33 |

### suzuki_kasami  (n=5)

| Delay (ms) | Mean wait (ms) | P95 wait (ms) | Throughput (ops/s) |
|---|---|---|---|
| 1 | 3.2 ±1.8 | 7.8 ±3.2 | 170.98 ±37.52 |
| 5 | 22.1 ±8.9 | 41.4 ±13.7 | 135.33 ±18.90 |
| 10 | 46.8 ±14.1 | 83.1 ±21.4 | 82.60 ±17.44 |
| 50 | 178.0 ±52.9 | 363.1 ±107.8 | 23.55 ±5.75 |

### suzuki_kasami  (n=20)

| Delay (ms) | Mean wait (ms) | P95 wait (ms) | Throughput (ops/s) |
|---|---|---|---|
| 1 | 43.3 ±3.7 | 61.1 ±3.0 | 330.01 ±12.67 |
| 5 | 113.0 ±10.5 | 157.7 ±12.8 | 146.82 ±11.55 |
| 10 | 202.5 ±16.8 | 276.0 ±26.2 | 85.03 ±7.48 |
| 50 | 910.1 ±84.7 | 1306.2 ±159.0 | 19.92 ±1.90 |

## 4. Message Loss Resilience  (delay=5 ms)

Incomplete runs occur when the protocol stalls before the simulation deadline (30 s).
`stalled_requests` = total expected operations minus completed operations.

### raymond_tree  (n=5)

| Loss % | Trials | Completed | Stalled requests (mean ± std) | Mean wait (ms) |
|---|---|---|---|---|
| 0% | 10 | 10 | 0.0 ±0.0 | 29.9 ±7.6 |
| 1% | 10 | 5 | 4.8 ±5.9 | 28.1 ±9.5 |
| 5% | 10 | 0 | 15.9 ±4.9 | N/A |

### raymond_tree  (n=20)

| Loss % | Trials | Completed | Stalled requests (mean ± std) | Mean wait (ms) |
|---|---|---|---|---|
| 0% | 10 | 10 | 0.0 ±0.0 | 142.0 ±11.8 |
| 1% | 10 | 1 | 67.2 ±28.6 | 151.5 |
| 5% | 10 | 0 | 89.3 ±12.8 | N/A |

### ricart_agrawala  (n=5)

| Loss % | Trials | Completed | Stalled requests (mean ± std) | Mean wait (ms) |
|---|---|---|---|---|
| 0% | 10 | 10 | 0.0 ±0.0 | 31.1 ±3.8 |
| 1% | 10 | 1 | 16.5 ±6.7 | 30.6 |
| 5% | 10 | 0 | 24.0 ±1.1 | N/A |

### ricart_agrawala  (n=20)

| Loss % | Trials | Completed | Stalled requests (mean ± std) | Mean wait (ms) |
|---|---|---|---|---|
| 0% | 10 | 10 | 0.0 ±0.0 | 142.0 ±7.8 |
| 1% | 10 | 0 | 98.5 ±1.5 | N/A |
| 5% | 10 | 0 | 100.0 ±0.0 | N/A |

### suzuki_kasami  (n=5)

| Loss % | Trials | Completed | Stalled requests (mean ± std) | Mean wait (ms) |
|---|---|---|---|---|
| 0% | 10 | 10 | 0.0 ±0.0 | 22.1 ±8.9 |
| 1% | 10 | 8 | 3.6 ±7.8 | 23.9 ±6.2 |
| 5% | 10 | 4 | 9.8 ±10.7 | 20.9 ±4.1 |

### suzuki_kasami  (n=20)

| Loss % | Trials | Completed | Stalled requests (mean ± std) | Mean wait (ms) |
|---|---|---|---|---|
| 0% | 10 | 10 | 0.0 ±0.0 | 113.0 ±10.5 |
| 1% | 10 | 6 | 14.2 ±28.5 | 117.2 ±14.1 |
| 5% | 10 | 0 | 66.8 ±29.3 | N/A |

## 5. Safety and Contention Audit

For every completed run across all suites, `max_concurrent_cs` (the peak number of
processes simultaneously inside the critical section) was **1**.  No mutual-exclusion
violations were detected.  `safety_violations` (tracked by the simulator's own checker)
was 0 for all runs.

| Suite | Runs | Safety violations | Max concurrent CS > 1 |
|---|---|---|---|
| baseline | 30 | 0 | 0 |
| scalability | 120 | 0 | 0 |
| network_delay | 240 | 0 | 0 |
| message_loss | 180 | 0 | 0 |

## 6. Fairness (Coefficient of Variation)

CV = std(wait) / mean(wait) per run, averaged across trials.
Lower CV indicates more uniform waiting times across processes.

### Baseline (n=5, delay=5 ms, loss=0 %)

| Algorithm | Mean CV ± std |
|---|---|
| raymond_tree | 0.76 ±0.10 |
| ricart_agrawala | 0.32 ±0.10 |
| suzuki_kasami | 0.58 ±0.16 |

## 7. Methodology

- **Simulator**: Discrete-event DME simulator (`src/simulation/`)
- **Trials**: 10 independent trials per configuration (seeds 42–51)
- **Workload**: 5 requests per process, exponential inter-arrival (mean 20 ms),
  critical section 2 ms
- **Simulation deadline**: 30 000 ms; runs that do not complete all requests
  within this window are classified as `incomplete`
- **Metrics**: mean/P95 wait, throughput, message counts, stalled requests,
  `max_concurrent_cs`, coefficient of variation (CV) of wait times
- **Aggregation**: grouped by (algorithm, process_count, configured_delay, loss_rate);
  wait/throughput means exclude incomplete runs; stalled request counts include all runs
- **Charts**: saved as `results/{suite}_{metric}.png`; shaded bands show ±1 std dev
