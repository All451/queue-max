# Stress Test Results

Resultados dos testes de estresse realizados em **2026-06-10** com a versão atual do Queue Max.

## Teste Moderado

Executado via `stress_test.py` — 5 cenários cobrindo throughput, concorrência, resiliência e persistência.

| Cenário | Config | Resultado |
|---------|--------|-----------|
| Throughput | 5.000 jobs, 8 workers, 8 shards | **3.317 jobs/sec** |
| Enqueue concorrente | 5 threads, 2.000 jobs | 2.000 enfileirados, 0 erros |
| Flaky (50% falha) | 1.000 jobs, 4 workers, max_retries=3 | 492 processados, 508 pendentes |
| Orphan recovery | 2 shards, worker morto | 199 órfãos recuperados |
| Persistência | Restart de Queue | ✅ Job sobreviveu |

## Teste Pesado

Executado via `stress_heavy.py` — 4 cenários para empurrar a fila ao limite.

### [1] Throughput Máximo — 50.000 jobs

| Item | Valor |
|------|-------|
| Workers / Shards | 12 / 12 |
| Rate limit | 10.000/min |
| Tempo de enqueue | 31,0s |
| Tempo de processamento | 228,8s |
| **Throughput** | **219 jobs/sec** |

> O enqueue foi o gargalo neste teste (31s para 50k jobs). A taxa de processamento reflete jobs com `sleep(0.0005)` — ou seja, micro-tarefas.

### [2] Contenção — 1 shard, 10 workers

| Item | Valor |
|------|-------|
| Jobs | 10.000 |
| Workers | 10 |
| Shards | 1 |
| **Throughput** | **1.660 jobs/sec** |

> Mesmo com contenção máxima em um único banco SQLite, a fila mantém boa vazão.

### [3] Caos — 30% de falha

| Item | Valor |
|------|-------|
| Jobs | 5.000 |
| Fail rate | 30% |
| Workers | 8 |
| Max retries | 2 |
| Processados (OK) | 3.537 |
| Failed (permanente) | 1.463 |
| **Circuit breaker** | **Closed** (não tripou) |

> O circuit breaker permaneceu fechado, indicando que o threshold padrão é adequado para 30% de falha com 2 retentativas.

### [4] Burst — Sem limite de taxa

| Item | Valor |
|------|-------|
| Jobs | 20.000 |
| Workers | 20 |
| Shards | 10 |
| Rate limit | 50.000/min |
| **Throughput** | **3.290 jobs/sec** |

> Cenário de burst puro com rate limit alto. A taxa real fica em ~3.300 jobs/sec.

## Teste de Escala — 100.000 jobs

| Item | Valor |
|------|-------|
| Workers / Shards | 16 / 8 |
| Jobs | 100.000 |
| Enqueue time | 59,9s |
| Enqueue rate | 1.668 jobs/s |
| Process time | 71,5s |
| Process rate | 1.400 jobs/s |
| Duplicatas | **0** |
| Perdas | **0** |
| Erros | **0** |

## Teste Máximo — 500.000 jobs

| Item | Valor |
|------|-------|
| Workers / Shards | 16 / 8 |
| Jobs | 500.000 |
| Enqueue time | 1.192s (~20 min) |
| Enqueue rate | 419 jobs/s |
| Process time | 1.408s (~23 min) |
| Process rate | 355 jobs/s |
| Duplicatas | **0** |
| Perdas | **0** |
| Erros | **0** |

> O throughput cai em 500k jobs (esperado para SQLite concorrente nesse volume), mas o dado crítico é: **zero corrupção, zero duplicatas, zero perda** em meio milhão de jobs com 16 workers concorrentes.

## Resumo

| Métrica | Valor |
|---------|-------|
| Throughput médio (burst) | ~3.300 jobs/sec |
| Throughput com contenção (1 shard) | ~1.660 jobs/sec |
| Throughput 100k jobs (8 shards) | **1.400 jobs/s — 0 duplicatas** |
| Throughput 500k jobs (8 shards) | **355 jobs/s — 0 duplicatas** |
| Throughput com 50% falha | ~490 jobs processados / 1.000 |
| Persistência | ✅ |
| Circuit breaker | Estável mesmo com 30% de falha |
| Exactly-once (500k jobs) | ✅ |
