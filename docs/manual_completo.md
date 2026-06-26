# Queue Max — Manual Didático Completo

> **Uma task queue em Python puro, com SQLite, sem dependências runtime.**
>
> Este manual explica todos os conceitos, padrões e decisões de design
> do Queue Max — desde o básico de filas até os detalhes de implementação.

---

## Índice

1. [O Problema](#1-o-problema)
2. [Conceitos Fundamentais](#2-conceitos-fundamentais)
3. [Visão Arquitetural](#3-visão-arquitetural)
4. [Sharding Físico](#4-sharding-físico)
5. [SQLite como Fila](#5-sqlite-como-fila)
6. [Módulo Core: Queue](#6-módulo-core-queue)
7. [Módulo Core: Database (ShardManager)](#7-módulo-core-database-shardmanager)
8. [Módulo Core: Worker](#8-módulo-core-worker)
9. [Módulo Core: Circuit Breaker](#9-módulo-core-circuit-breaker)
10. [Módulo Core: Rate Limiter](#10-módulo-core-rate-limiter)
11. [Módulo Core: Decorator (@task)](#11-módulo-core-decorator-task)
12. [Módulo Models: Job](#12-módulo-models-job)
13. [Módulo Utils: Helpers](#13-módulo-utils-helpers)
14. [Módulo Contrib: Events (bubus)](#14-módulo-contrib-events-bubus)
15. [Módulo Contrib: Integrações](#15-módulo-contrib-integrações)
16. [CLI](#16-cli)
17. [Exceções](#17-exceções)
18. [Padrões de Projeto Aplicados](#18-padrões-de-projeto-aplicados)
19. [Concorrência e Thread Safety](#19-concorrência-e-thread-safety)
20. [Diagrama de Fluxo Completo](#20-diagrama-de-fluxo-completo)
21. [Glossário](#21-glossário)

---

## 1. O Problema

### O Cenário

Você tem uma aplicação web (FastAPI, Flask, Django) que precisa:

- Enviar emails em background sem travar a resposta HTTP
- Processar webhooks de forma assíncrona
- Executar tarefas agendadas (limpeza, relatórios)
- Processar lotes de dados sem bloquear o usuário

### As Soluções Típicas

| Solução | Prós | Contras |
|---------|------|---------|
| **Celery + Redis/RabbitMQ** | Poderoso, distribuído | Infra pesada, many deps |
| **RQ + Redis** | Simples | Precisa Redis |
| **Threading manual** | Zero deps | Sem persistência, sem retry |
| **Queue Max** | Zero deps, persistente, resiliente | Single-host (por enquanto) |

### A Filosofia do Queue Max

> **"Para 90% dos projetos, Redis é overkill. SQLite resolve."**

Queue Max é uma **task queue** (fila de tarefas) que:

- Usa **SQLite** como backend (zero dependências)
- Suporta **múltiplos workers** concorrentes via threads
- Tem **rate limiting**, **circuit breaker**, **retry com backoff**
- Usa **sharding físico** (N arquivos .db) para escalar escrita
- Oferece **CLI** completa para operação
- Se integra com **Django, FastAPI e Flask**

---

## 2. Conceitos Fundamentais

### 2.1 O Que é uma Task Queue?

```
┌─────────┐    enqueue()    ┌──────────────┐    pop_job()    ┌─────────┐
│ Producer │ ──────────────>│    Queue     │ ──────────────>│ Worker  │
│ (sua app)│                │  (armazena)  │                │ (processa)│
└─────────┘                 └──────────────┘                └─────────┘
                                  │
                                  │ SQLite (disco)
                                  ▼
                             ┌──────────┐
                             │ shard_0  │
                             │ shard_1  │  ← N arquivos .db
                             │ ...      │
                             └──────────┘
```

**Produtor (Producer)**: Quem cria as tarefas. Ex: sua view FastAPI.

**Fila (Queue)**: Onde as tarefas esperam. Persistente em disco.

**Consumidor (Worker)**: Quem executa as tarefas. Roda em background.

**Tarefa (Job)**: Uma unidade de trabalho. Tem payload, prioridade, status.

### 2.2 Job Lifecycle

```
                     ┌─────────┐
                     │ PENDING │  ← acabou de ser enfileirado
                     └────┬────┘
                          │ pop_job()
                          ▼
                   ┌──────────────┐
                   │  PROCESSING  │  ← worker está executando
                   └──────┬───────┘
                          │
              ┌───────────┴───────────┐
              │                       │
              ▼                       ▼
       ┌───────────┐          ┌────────────┐
       │ COMPLETED │          │  FAILED    │
       │ (deletado)│          │            │
       └───────────┘          └──────┬─────┘
                                     │
                          ┌──────────┴──────────┐
                          │                     │
                          ▼                     ▼
                   ┌────────────┐      ┌──────────────┐
                   │ Retry (se  │      │ Dead Letter  │
                   │ max < lim) │      │ Queue (DLQ)  │
                   └────────────┘      └──────────────┘
```

### 2.3 Sharding

Sharding = dividir os dados em múltiplos bancos.

```
Queue(shards=6) cria:

  data/
  ├── shard_0.db    ← jobs 0, 6, 12, 18... (pagina_id % 6 == 0)
  ├── shard_1.db    ← jobs 1, 7, 13, 19...
  ├── shard_2.db
  ├── shard_3.db
  ├── shard_4.db
  └── shard_5.db    ← jobs 5, 11, 17, 23...
```

**Por que sharding?** SQLite tem lock global por arquivo. Com 6 shards,
6 workers podem escrever simultaneamente sem competir pelo mesmo lock.

**Roteamento**: `pagina_id % num_shards` ou aleatório (via Router).

---

## 3. Visão Arquitetural

```
src/queue_max/
│
├── __init__.py          # Public API (re-exports)
├── exceptions.py        # Hierarquia de exceções
├── cli.py               # CLI (argparse)
│
├── core/                # Núcleo do sistema
│   ├── __init__.py
│   ├── circuit_breaker.py   # Circuit Breaker (3 estados + probe único)
│   ├── rate_limiter.py      # Token Bucket (Condition-based)
│   │
│   ├── db/                  # Camada de persistência
│   │   ├── __init__.py
│   │   ├── constants.py     # SQL, schema, índices, PRAGMAs
│   │   ├── connection.py    # ConnectionManager (thread-local)
│   │   ├── manager.py       # ShardManager (facade)
│   │   ├── repository.py    # ShardRepository (CRUD + retry + métricas)
│   │   └── shard_group.py   # ShardGroup (scan otimizado)
│   │
│   ├── queue/
│   │   ├── __init__.py
│   │   └── queue.py         # Queue — API principal
│   │
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── base.py          # Worker + WorkerState
│   │   ├── async_worker.py  # AsyncWorker
│   │   └── pool.py          # WorkerPool (auto-scaling)
│   │
│   └── tasks/
│       ├── __init__.py
│       ├── base.py          # @task
│       ├── periodic.py      # @periodic_task
│       └── retryable.py     # @retryable_task
│
├── models/
│   ├── __init__.py
│   └── job.py           # Job, JobStatus, JobPriority, JobResult
│
├── utils/
│   ├── __init__.py
│   └── helpers.py       # now_iso, backoff_delay, validate_payload, etc.
│
└── contrib/             # Integrações opcionais
    ├── __init__.py
    ├── events.py        # Eventos tipados com bubus
    ├── django/          # @task para Django
    ├── fastapi/         # BackgroundQueue + QueueMiddleware
    └── flask/           # QueueExtension
```

### Dependências entre módulos

```
    cli.py
      │
      ▼
   queue/queue.py ───────────────────────────┐
      │                                       │
      ├──► db/manager.py (ShardManager)       │
      │       ├── db/connection.py            │
      │       ├── db/repository.py            │
      │       └── db/shard_group.py           │
      ├──► circuit_breaker.py                 │
      ├──► rate_limiter.py                    │
      ├──► helpers.py                         │
      └──► job.py                             │
                                              ▼
   workers/base.py ──► queue/queue.py   tasks/base.py ──► queue/queue.py
   workers/async_worker.py ──► base.py
   workers/pool.py ──► base.py
   tasks/periodic.py ──► tasks/base.py
   tasks/retryable.py ──► tasks/base.py
```

**Regra de ouro**: `core/` nunca importa `contrib/`. A dependência é
são sempre unidirecional: `contrib → core`.

---

## 4. Sharding Físico

### 4.1 Por Que Sharding?

SQLite é **single-writer**: uma transação por vez por arquivo. Com um
único `queue.db`, workers competem pelo mesmo lock — gargalo.

Com **N arquivos**, N workers podem escrever simultaneamente (cada um
no seu shard). O lock do SQLite é por arquivo, então shards diferentes
não competem.

### 4.2 ShardGroup — Otimização de Scan

Quando um worker faz `pop_job()`, ele não sabe em qual shard está o
próximo job. Ele precisa **escanear** shards até achar um.

O `ShardGroup` organiza os shards em grupos para minimizar o scan:

```python
# 6 shards:  1 grupo de 6  → scan linear de 6
# 8 shards:  4 grupos de 2 → cada worker varre no máximo 2 shards por grupo
# 16 shards: 4 grupos de 4 → cada worker varre no máximo 4 shards por grupo
# 32 shards: 8 grupos de 4 → cada worker varre no máximo 4 shards por grupo
```

A fórmula (queue.py:52):
```python
self.shards_per_group = max(1, min(4, num_shards // 4)) if num_shards >= 8 else num_shards
```

### 4.3 Como o Scan Funciona

```python
def pop_job(self, worker_id):
    # 1. Rate limiter
    rate_limiter.acquire()
    # 2. Circuit breaker
    circuit_breaker.is_allowed()
    # 3. Pega grupos em ordem aleatória
    groups = shard_groups.randomized_groups()
    # 4. Se 1 grupo só → scan linear simples
    if len(groups) == 1:
        for shard_id in shuffle(shards):
            job = shard_manager.pop_job(shard_id, worker_id)
            if job: return job
    # 5. Múltiplos grupos → um grupo por vez
    for group in groups:
        for shard_id in shuffle(group):
            job = shard_manager.pop_job(shard_id, worker_id)
            if job: return job  # Achou! Não olha os outros grupos
    return None  # Nada encontrado
```

**Por que aleatorizar?** Para evitar que todos os workers disputem o
mesmo shard ao mesmo tempo.

### 4.4 Consistent Hashing (Router)

No roadmap: substituir `pagina_id % num_shards` por hashing consistente.

**Problema do módulo**: Se você tem 6 shards e escala para 8, TODOS os
jobs mudam de shard (remapeamento total). Com consistent hashing, apenas
~1/4 dos jobs é movido.

---

## 5. SQLite como Fila

### 5.1 Configuração (PRAGMAs)

```python
PRAGMA journal_mode = WAL;       # Write-Ahead Log — leituras não bloqueiam escritas
PRAGMA synchronous = NORMAL;     # Durável o suficiente, muito mais rápido que FULL
PRAGMA cache_size = 10000;       # 10MB de cache por conexão
PRAGMA mmap_size = 268435456;    # 256MB de memória mapeada (I/O mais rápido)
PRAGMA temp_store = MEMORY;      # Tabelas temporárias na memória
PRAGMA busy_timeout = 30000;     # Espera 30s antes de desistir (evita "database is locked")
```

### 5.2 WAL Mode

**Sem WAL**: Leitura bloqueia escrita e vice-versa. Uma transação por vez.

**Com WAL**: Escritas vão para um log separado (WAL). Leituras podem
acontecer simultaneamente com escritas. Múltiplas leituras simultâneas
são possíveis.

```
Sem WAL:     [READ══════]  [WRITE════]  [READ══════]  → serial
Com WAL:     [READ═════════════════════]               → paralelo
             [WRITE══]    [WRITE══]    [WRITE══]
```

### 5.3 BEGIN IMMEDIATE

SQLite tem três tipos de transação:

| Tipo | Comportamento |
|------|---------------|
| `DEFERRED` (default) | Só pega lock quando precisa escrever. Pode morrer com deadlock. |
| `IMMEDIATE` | Pega lock de escrita imediatamente. Outros readers ainda podem ler. |
| `EXCLUSIVE` | Pega lock exclusivo. Ninguém mais faz nada. |

Queue Max usa **`BEGIN IMMEDIATE`** em todas as operações de escrita
para evitar deadlocks entre workers concorrentes.

```python
def pop_job(self, shard_id, worker_id):
    conn = self._get_connection(shard_id)
    conn.execute("BEGIN IMMEDIATE")   # ← lock de escrita AGORA
    row = conn.execute(SELECT...).fetchone()
    if row:
        conn.execute(UPDATE...)
    conn.commit()                     # ← libera o lock
```

### 5.4 Thread-Local Connections

Cada thread tem seu próprio conjunto de conexões SQLite. Isso evita
compartilhar objetos `Connection` entre threads (que não é thread-safe).

```python
self._local = threading.local()

def _get_connection(self, shard_id):
    if not hasattr(self._local, "connections"):
        self._local.connections = {}
    if shard_id not in self._local.connections:
        conn = sqlite3.connect(...)
        self._local.connections[shard_id] = conn
    return self._local.connections[shard_id]
```

Cada thread → N conexões (uma por shard) → lazy creation.

### 5.5 Schema

```sql
CREATE TABLE fila (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pagina_id INTEGER NULL,           -- ID para sharding consistente
    payload TEXT NOT NULL,             -- JSON com os dados do job
    status TEXT DEFAULT 'pending',     -- pending|processing|failed
    priority INTEGER DEFAULT 0,        -- 0=low, 1=medium, 2=high
    tentativas INTEGER DEFAULT 0,      -- tentativas de processamento
    max_tentativas INTEGER DEFAULT 3,  -- máximo de retentativas
    retry_delay INTEGER DEFAULT 60,    -- delay base para backoff
    last_error TEXT NULL,              -- última mensagem de erro
    error_type TEXT NULL,              -- classe do erro (ex: ValueError)
    error_stack TEXT NULL,             -- stack trace
    worker_id TEXT NULL,               -- quem está processando
    heartbeat TEXT NULL,               -- último sinal de vida
    created_at TEXT DEFAULT (datetime('now')),
    started_at TEXT NULL,
    completed_at TEXT NULL,
    next_retry_at TEXT NULL            -- quando tentar novamente
);

CREATE TABLE shard_metadata (
    shard_id INTEGER PRIMARY KEY,
    version INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),
    last_vacuum TEXT NULL,
    total_jobs_processed INTEGER DEFAULT 0,
    total_jobs_failed INTEGER DEFAULT 0
);

CREATE TABLE dead_letter_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_job_id INTEGER,
    payload TEXT NOT NULL,
    error TEXT NOT NULL,
    error_type TEXT NOT NULL,
    failed_at TEXT DEFAULT (datetime('now')),
    shard_id INTEGER
);
```

**Índices**:
```sql
CREATE INDEX idx_status_priority ON fila(status, priority DESC);
CREATE INDEX idx_next_retry ON fila(next_retry_at) WHERE status = 'pending';
CREATE INDEX idx_heartbeat ON fila(heartbeat) WHERE status = 'processing';
CREATE INDEX idx_created_at ON fila(created_at);
CREATE INDEX idx_status_created ON fila(status, created_at);
CREATE INDEX idx_dlq_failed_at ON dead_letter_queue(failed_at);
```

### 5.6 A Consulta POP

```sql
SELECT * FROM fila
WHERE status='pending'
  AND (next_retry_at IS NULL OR next_retry_at <= ?)
ORDER BY priority DESC, id ASC
LIMIT 1;
```

Pega o job **mais prioritário** e **mais antigo** dentro da prioridade
que está pronto pra processar (next_retry_at venceu ou é nulo).

---

## 6. Módulo Core: Queue

**Arquivo**: `src/queue_max/core/queue.py` (508 linhas)

### 6.1 Responsabilidade

A classe `Queue` é a **API pública principal** do sistema. Tudo que o
usuário faz passa por ela.

### 6.2 O Que Ela Faz

```
Queue
├── Gerenciar shards (ShardManager)
├── Controlar taxa (RateLimiter)
├── Proteger serviço (CircuitBreaker)
├── Emitir eventos (job_enqueued, job_completed, ...)
├── Agrupar shards (ShardGroup)
└── Travar por shard (threading.Lock por shard)
```

### 6.3 Construtor

```python
class Queue:
    def __init__(
        self,
        shards: int = None,           # Número de shards (default: 6 ou NUM_SHARDS env)
        rate_limit: int = None,       # Requisições por minuto (default: 160)
        max_retries: int = None,      # Máximo de retentativas (default: 3)
        data_dir: str = None,         # Diretório dos .db (default: ./data ou DATA_DIR env)
        circuit_breaker_threshold: int = None,  # Falhas p/ abrir (default: 5)
        circuit_breaker_timeout: float = None,  # Tempo p/ recuperar (default: 60s)
        rate_limiter_timeout: float = 5.0,      # Timeout p/ token (default: 5s)
    ):
```

### 6.4 Métodos Principais

| Método | Função |
|--------|--------|
| `enqueue(payload, pagina_id, priority, max_retries)` | Adiciona job |
| `enqueue_batch(jobs)` | Adiciona vários jobs (1 transação por shard) |
| `pop_job(worker_id)` | Pega próximo job (rate limited) |
| `complete_job(job_id, shard_id)` | Marca como concluído |
| `fail_job(job_id, shard_id, error, permanent)` | Marca como falha (com retry ou DLQ) |
| `get_stats()` | Estatísticas completas |
| `recover_orphans()` | Recupera jobs travados em "processing" |
| `purge_queue(status)` | Limpa jobs |
| `retry_failed_jobs(shard_id)` | Re-enfileira jobs falhos |
| `cleanup_old_jobs(days)` | Remove jobs antigos |
| `wait_until_empty(timeout)` | Bloqueia até fila esvaziar |
| `wait_for_jobs(count, timeout)` | Bloqueia até ter N jobs |
| `on(event, callback)` | Registra listener de evento |
| `batch()` | Context manager que silencia eventos |

### 6.5 Sistema de Eventos

```python
self._events = {
    "job_enqueued": [callback1, callback2, ...],
    "job_completed": [...],
    "job_failed": [...],
    "job_retried": [...],
    "alert": [...],
}
self._events_lock = threading.Lock()
```

`on()` registra um callback. `_emit()` chama todos os callbacks.
`batch()` temporariamente substitui `_emit` por um no-op
(útil para operações em massa que não precisam notificar).

### 6.6 ShardGroup — Algoritmo de Agrupamento

```python
# num_shards=6  → shards_per_group=6  → 1 grupo de 6
# num_shards=8  → shards_per_group=2  → 4 grupos de 2
# num_shards=12 → shards_per_group=3  → 4 grupos de 3
# num_shards=16 → shards_per_group=4  → 4 grupos de 4
# num_shards=24 → shards_per_group=4  → 6 grupos de 4
```

Garante **pelo menos 4 grupos** quando `num_shards >= 16` para
espalhar workers concorrentes.

---

## 7. Módulo Core: Database (ShardManager)

**Arquivos**:
- `src/queue_max/core/db/connection.py` — ConnectionManager (93 linhas)
- `src/queue_max/core/db/repository.py` — ShardRepository (183 linhas)
- `src/queue_max/core/db/manager.py` — ShardManager facade (49 linhas)
- `src/queue_max/core/db/constants.py` — SQL e configuração (34 linhas)
- `src/queue_max/core/db/shard_group.py` — ShardGroup (17 linhas)

### 7.1 Responsabilidade

A camada de banco foi decomposta em componentes especializados:

| Componente | Responsabilidade |
|------------|-----------------|
| `ConnectionManager` | Conexões thread-local, PRAGMAs, schema, migrações |
| `ShardRepository` | CRUD, retry, heartbeat, métricas, manutenção |
| `ShardManager` | Facade fina (delega para os acima) |
| `ShardGroup` | Agrupamento de shards para scan otimizado |
| `constants.py` | SQL, schema, índices, CHECK constraints |

### 7.2 Conexões Thread-Local

```python
self._local = threading.local()  # Cada thread tem seu próprio cache
self._all_connections: set = set()  # Rastreia TODAS as conexões (p/ close_all)
self._connections_lock = threading.Lock()
```

Cada thread cria suas conexões sob demanda. O `_all_connections`
permite fechar todas no shutdown independente da thread.

### 7.3 Métodos do ShardManager

| Método | Descrição |
|--------|-----------|
| `insert_job(shard, payload, pagina_id, priority, max_retries)` | Insere 1 job |
| `insert_jobs_batch(shard, jobs)` | Insere vários (1 transação) |
| `pop_job(shard, worker_id)` | Pega e marca 1 job (BEGIN IMMEDIATE) |
| `complete_job(shard, job_id)` | Deleta o job + atualiza metadados |
| `fail_job(shard, job_id, error, permanent)` | Falha com retry ou DLQ |
| `retry_failed_jobs(shard)` | Re-enfileira todos os failed |
| `retry_job(shard, job_id)` | Re-enfileira 1 job específico |
| `cleanup_old_jobs(shard, days)` | Remove old jobs + DLQ |
| `get_metrics(shard)` | Métricas do shard (pending, processing, failed) |
| `get_stats(shard)` | pending/processing/failed counts |
| `get_all_stats()` | Soma de todos os shards |
| `heartbeat(shard, worker_id)` | Atualiza heartbeat do worker |
| `recover_orphans(shard, stuck_timeout)` | Jobs em processing sem heartbeat recente → pending |
| `close_all()` | Fecha todas as conexões |
| `get_failed_jobs(shard, limit)` | Lista jobs falhos |
| `get_processing_jobs(shard)` | Lista jobs em processamento |
| `get_dead_letter_queue(shard, limit)` | Lista DLQ |

### 7.4 O Fluxo do pop_job (O Mais Importante)

```python
def pop_job(self, shard_id, worker_id):
    conn = self._get_connection(shard_id)
    now = now_iso()
    try:
        conn.execute("BEGIN IMMEDIATE")     # 1. Lock de escrita
        row = conn.execute(SELECT_SQL).fetchone()  # 2. SELECT
        if row is None:
            conn.commit()
            return None
        job_id = row["id"]
        conn.execute(CLAIM_JOB_SQL, ...)    # 3. UPDATE status='processing'
        conn.commit()                        # 4. Commit (libera lock)
        job = Job.from_row(dict(row), shard_id=shard_id)
        job.status = JobStatus.PROCESSING
        return job
    except sqlite3.OperationalError as e:
        conn.rollback()                      # 5. Se deu erro, rollback
        return None
```

**Por que SELECT + UPDATE em vez de UPDATE + RETURNING?**
Compatibilidade com Python 3.9. `UPDATE ... RETURNING` é SQLite 3.35+.

### 7.5 O Fluxo do fail_job (Decisão: Retry vs DLQ)

```python
def fail_job(self, shard_id, job_id, error, permanent):
    if permanent:
        # 1. Salva na DLQ (dead letter queue)
        conn.execute(MOVE_TO_DLQ_SQL, ...)
        # 2. Marca como failed
        conn.execute(FAIL_JOB_SQL, ...)
        # 3. Atualiza metadados
        conn.execute(UPDATE_META_FAILED_SQL, ...)
    else:
        # 1. Verifica tentativas
        row = conn.execute("SELECT tentativas, max_tentativas FROM fila...")
        t = row["tentativas"] + 1
        if t > row["max_tentativas"]:
            # Esgotou tentativas → DLQ + failed
            conn.execute(MOVE_TO_DLQ_SQL, ...)
            conn.execute(FAIL_JOB_SQL, ...)
            conn.execute(UPDATE_META_FAILED_SQL, ...)
        else:
            # Ainda tem tentativas → agenda retry com backoff
            delay = backoff_delay(t)  # 60s, 120s, 240s... (com jitter)
            next_retry = now + delay
            conn.execute(RETRY_SCHEDULE_SQL, next_retry, ...)
    conn.commit()
```

### 7.6 VACUUM Automático

```python
def _maybe_vacuum(self, shard_id):
    # Só faz VACUUM a cada 24h por shard
    if now - last_vacuum.get(shard_id, 0) < 24 * 3600:
        return
    conn.execute("VACUUM")  # Reclama espaço no SQLite
```

---

## 8. Módulo Core: Worker

**Arquivo**: `src/queue_max/core/worker.py` (441 linhas)

### 8.1 Três Níveis de Worker

```
Worker          → thread única, poll, job_timeout opcional
AsyncWorker     → Worker com event loop asyncio
WorkerPool      → Múltiplos Workers com auto-scaling
```

### 8.2 Worker — O Loop Principal

```python
def _run_loop(self):
    while not self._stop_event.is_set():   # ← Evento de parada (thread-safe)
        try:
            job = self.queue.pop_job(self.worker_id)
        except Exception:
            self._idle_wait()              # ← espera poll_interval
            continue

        if job is None:
            self._idle_wait()              # ← fila vazia, espera
            continue

        self._process_job(job)             # ← processa o job
        self._send_heartbeat()             # ← atualiza heartbeat no DB
```

### 8.3 State Machine

```
INITIALIZED → STARTING → RUNNING → STOPPING → STOPPED
                                  ↘           ↗
                                    ERROR
```

Cada transição é controlada por `_state: WorkerState`.

### 8.4 Processamento de Job

```python
def _process_job(self, job):
    # 1. Callback on_job_start
    if self.on_job_start:
        self.on_job_start(worker_id, job_id, payload)

    try:
        # 2. Executa a função do usuário
        if self.job_timeout:
            result = self._execute_with_timeout(payload)  # ThreadPoolExecutor
        else:
            result = self.process_function(payload)

        # 3. Sucesso → complete_job
        self.queue.complete_job(job.id, job.shard_id)

    except Exception as e:
        # 4. Falha → fail_job (decide retry vs permanente)
        permanent = not is_retryable_error(e)
        self.queue.fail_job(job.id, job.shard_id, e, permanent=permanent)

    finally:
        # 5. Limpa job atual
        self._current_job = None
```

### 8.5 Timeout com ThreadPoolExecutor

```python
def _execute_with_timeout(self, payload):
    # Executa a função em outra thread com timeout
    future = self._executor.submit(self.process_function, payload)
    try:
        return future.result(timeout=self.job_timeout)
    except FuturesTimeoutError:
        raise TimeoutError(f"Job excedeu {self.job_timeout}s")
```

O `ThreadPoolExecutor` é reutilizado entre jobs (criado uma vez no
`__init__`, destruído no `stop`).

### 8.6 AsyncWorker

```python
class AsyncWorker(Worker):
    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        try:
            while not self._stop_event.is_set():
                job = self.queue.pop_job(self.worker_id)
                if job:
                    self._loop.run_until_complete(
                        self._process_async(job)
                    )
        finally:
            self._loop.close()
```

Suporta funções async e sync:
```python
async def process_async(self, job):
    if asyncio.iscoroutinefunction(self.process_function):
        result = await self.process_function(job.payload)
    else:
        result = self.process_function(job.payload)  # sync em loop async
```

### 8.7 WorkerPool e Auto-Scaling

```python
class WorkerPool:
    def _check_and_scale(self):
        pending = self._queue.get_stats()["pending"]
        current = len(self.workers)

        if pending > scale_up_threshold and current < max_workers:
            self._scale_to(current + 1, f"pending={pending}")
        elif pending < scale_down_threshold and current > min_workers:
            self._scale_to(current - 1, f"pending={pending}")
```

- Sobe worker quando pending > 100
- Desce worker quando pending < 10
- Verifica a cada 60 segundos

---

## 9. Módulo Core: Circuit Breaker

**Arquivo**: `src/queue_max/core/circuit_breaker.py` (178 linhas)

### 9.1 O Padrão Circuit Breaker

Protege serviços externos contra sobrecarga. Três estados:

```
CLOSED ──(falhas consecutivas >= threshold)──► OPEN
  ▲                                               │
  │                                               │
  └──(sucesso)── HALF_OPEN ◄──(timeout expirou)───┘
```

### 9.2 Estados

| Estado | O Que Significa | O Queue Faz |
|--------|-----------------|-------------|
| `CLOSED` | Tudo normal | Pop passa direto |
| `OPEN` | Serviço pode estar fora | Pop retorna None |
| `HALF_OPEN` | Testando recuperação | Pop passa (1 tentativa) |

### 9.3 Implementação

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60.0):
        self.state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._mutex = threading.Lock()  # Thread-safe

    def record_success(self):
        with self._mutex:
            if self.state == HALF_OPEN:
                self._set_state(CLOSED)  # Serviço recuperou!
            self._failure_count = 0      # Reset

    def record_failure(self):
        with self._mutex:
            self._failure_count += 1
            if self.state == HALF_OPEN:
                self._set_state(OPEN)    # Ainda quebrado
            elif self.state == CLOSED and self._failure_count >= threshold:
                self._set_state(OPEN)    # Abriu!

    def is_allowed(self):
        with self._mutex:
            if self.state == OPEN:
                if time.monotonic() - self._last_failure_time >= recovery_timeout:
                    self._set_state(HALF_OPEN)
                    return True          # Tenta recuperar
                return False             # Rejeita
            return True                  # CLOSED ou HALF_OPEN
```

### 9.4 Onde é Chamado no Queue

```python
# pop_job (queue.py)
if not self.circuit_breaker.is_allowed():
    return None  # Não deixa nem tentar

# complete_job (queue.py)
self.circuit_breaker.record_success()

# fail_job (queue.py)
self.circuit_breaker.record_failure()
```

**Proteção contra falhas em cascata**: Se o processamento está falhando
muito, o circuit breaker abre e os workers param de pegar jobs. Dá
tempo do sistema se recuperar.

---

## 10. Módulo Core: Rate Limiter

**Arquivo**: `src/queue_max/core/rate_limiter.py` (214 linhas)

### 10.1 Token Bucket

O algoritmo mais comum para rate limiting:

```
    ┌─────────────────────────────────────┐
    │          Token Bucket                │
    │                                     │
    │   🫷 🫷 🫷 🫷 🫷 🫷 🫷 🫷              │
    │   bucket capacity = burst_capacity  │
    │   refill rate = rate_limit / minuto  │
    └─────────────────────────────────────┘
           ▲                        │
           │                        ▼
     acquire(token)            refill(continuamente)
```

### 10.2 Implementação

```python
class RateLimiter:
    def __init__(self, rate_limit=160, unit=PER_MINUTE):
        self.rate_limit = rate_limit          # 160 req/min
        self.interval = 60.0 / rate_limit     # 0.375s entre tokens
        self._tokens = float(burst_capacity)  # Tokens disponíveis
        self._last_refill = time.monotonic()
        self._mutex = threading.Lock()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        # 160 tokens / 60s = 2.67 tokens/s
        tokens_to_add = elapsed * (self.rate_limit / 60.0)
        if self.enable_jitter:
            tokens_to_add *= random.uniform(0.95, 1.05)  # +-5% jitter
        self._tokens = min(self.burst_capacity, self._tokens + tokens_to_add)
        self._last_refill = now

    def acquire(self, timeout=30.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._try_acquire():
                return True
            time.sleep(min(self.interval * 0.5, remaining * 0.1))
        raise RateLimitError(...)
```

### 10.3 Por Que Jitter?

Sem jitter, N workers acordam no mesmo instante e todos tentam
adquirir token ao mesmo tempo — **choque de rebanho** (thundering herd).
O jitter espalha as requisições no tempo.

### 10.4 Unidades Suportadas

```python
RateLimiter(160, PER_MINUTE)  # 160 req/min
RateLimiter(10, PER_SECOND)   # 10 req/s
RateLimiter(1000, PER_HOUR)   # 1000 req/h
```

---

## 11. Módulo Core: Decorator (@task)

**Arquivo**: `src/queue_max/core/decorator.py` (346 linhas)

### 11.1 O Decorator @task

Transforma uma função comum em uma tarefa enfileirável:

```python
@task(priority=2, max_retries=3)
def send_email(to: str, subject: str):
    # ... envia email ...

# Uso síncrono (executa agora)
send_email("user@example.com", "Hello")

# Uso assíncrono (enfileira)
send_email.delay("user@example.com", "Hello")

# Agendado
send_email.schedule_in(300, "user@example.com", "Hello")

# Paralelo
send_email.map(["a@b.com", "c@d.com"], "Welcome")
```

### 11.2 Como Funciona Internamente

```python
def task(queue=None, priority=0, max_retries=None, ...):
    def decorator(func):
        _queue = queue or Queue(...)
        task_name = f"{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Execução síncrona (chamada direta)
            sig.bind(*args, **kwargs)       # Valida argumentos
            if timeout:
                with ThreadPoolExecutor() as e:
                    return e.submit(func, *args, **kwargs).result(timeout=timeout)
            return func(*args, **kwargs)

        def delay(*args, **kwargs):
            sig.bind(*args, **kwargs)       # Valida antes de enfileirar
            payload = {
                "task": task_name,          # Nome totalmente qualificado
                "version": version,         # Para migrations de schema
                "args": args,
                "kwargs": kwargs,
                "timeout": timeout,
                "retry_delay": retry_delay,
            }
            return _queue.enqueue(payload, priority=priority, max_retries=max_retries)

        # Anexa métodos
        wrapper.delay = delay
        wrapper.schedule_at = schedule_at
        wrapper.schedule_in = schedule_in
        wrapper.map = map
        wrapper.bulk_delay = bulk_delay
        wrapper.get_queue = lambda: _queue
        wrapper.get_stats = get_stats
        wrapper.task_name = task_name
        return wrapper
    return decorator
```

### 11.3 @periodic_task

```python
@periodic_task(interval=3600, priority=1)
def cleanup():
    print("Limpando...")

cleanup.start_scheduler()  # Roda em daemon thread
```

### 11.4 @retryable_task

Retry **síncrono** (diferente do retry da fila):

```python
@retryable_task(max_retries=5, retry_on=[TimeoutError])
def fetch_data(url: str):
    # Se falhar com TimeoutError, tenta de novo até 5x
    return requests.get(url).json()
```

### 11.5 Hierarquia de Retry

```
1. Retryable da fila (fail_job → retry agendado no DB)
   → Quando: job falha, mas ainda tem tentativas
   → Quanto: backoff exponencial (60s, 120s, 240s...)

2. Retryable do worker (is_retryable_error)
   → Quando: erro é transitório (timeout, 5xx, conexão)
   → Quando NÃO: erro permanente (400, 404, ValidationError)

3. Retryable do decorator (@retryable_task)
   → Quando: exceção específica acontece na chamada síncrona
```

---

## 12. Módulo Models: Job

**Arquivo**: `src/queue_max/models/job.py` (338 linhas)

### 12.1 A Classe Job

```python
@dataclass
class Job:
    id: int
    payload: dict[str, Any]
    pagina_id: int | None = None
    status: JobStatus | str = JobStatus.PENDING
    priority: JobPriority | int = JobPriority.MEDIUM
    tentativas: int = 0
    max_tentativas: int = 3
    retry_delay: int = 60
    last_error: str | None = None
    error_type: str | None = None
    error_stack: str | None = None
    worker_id: str | None = None
    heartbeat: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    next_retry_at: str | None = None
    shard_id: int = 0
    # ... extended attributes
```

### 12.2 Por Que Dataclass?

- Imutabilidade parcial (é frozen após criação)
- `__post_init__` normaliza enums (string → `JobStatus`, int → `JobPriority`)
- Propriedades computadas (`status_str`, `priority_int`, `is_pending`, etc.)
- `from_row()` factory method (DB → objeto)
- `to_dict()` serialização

### 12.3 Enums

```python
class JobStatus(Enum):
    PENDING = "pending"       # Aguardando processamento
    PROCESSING = "processing" # Sendo executado
    COMPLETED = "completed"   # Concluído (removido do DB)
    FAILED = "failed"         # Falhou permanentemente
    CANCELLED = "cancelled"   # Cancelado

class JobPriority(Enum):
    LOW = 0
    MEDIUM = 1
    HIGH = 2
```

### 12.4 O Post-Init

```python
def __post_init__(self):
    if isinstance(self.status, str):
        # "pending" → JobStatus.PENDING
        self.status = JobStatus(self.status)
    if isinstance(self.priority, int):
        # 0 → JobPriority.LOW, 1 → MEDIUM, 2 → HIGH
        self.priority = JobPriority.from_int(self.priority)
    if self.created_at is None:
        self.created_at = now_iso()
```

### 12.5 from_row — Factory do Banco

```python
@classmethod
def from_row(cls, row: dict, shard_id: int = 0) -> "Job":
    # Payload pode vir como string JSON → desserializa
    raw = row.get("payload")
    if isinstance(raw, str):
        payload = json.loads(raw)
    else:
        payload = raw or {}

    # Tags e metadata também podem ser JSON strings
    tags = json.loads(row.get("tags", "[]")) if isinstance(row.get("tags"), str) else []

    return cls(
        id=row["id"],
        payload=payload,
        status=row.get("status", "pending"),
        priority=row.get("priority", 1),
        ...
    )
```

---

## 13. Módulo Utils: Helpers

**Arquivo**: `src/queue_max/utils/helpers.py` (156 linhas)

### 13.1 Funções

| Função | O Que Faz |
|--------|-----------|
| `now_iso()` | Timestamp UTC em ISO 8601 (`2026-06-20T10:30:00.000Z`) |
| `parse_iso(value)` | ISO string → datetime |
| `validate_payload(payload)` | Valida se é dict, serializável, <=10MB, <=20 níveis |
| `validate_priority(priority)` | Só aceita 0, 1, 2 |
| `get_env_int(name, default)` | Lê env var como int com fallback |
| `backoff_delay(tentativa, base=60, max=3600)` | Delay exponencial com jitter |
| `determine_shard(pagina_id, num_shards)` | Roteia job para um shard |
| `is_retryable_error(error)` | Decide se erro é retryável |

### 13.2 Algoritmo de Backoff

```python
def backoff_delay(tentativa: int, base_delay: int = 60, max_delay: int = 3600) -> float:
    delay = base_delay * (2 ** (tentativa - 1))
    # tentativa 1: 60s
    # tentativa 2: 120s
    # tentativa 3: 240s
    # tentativa 4: 480s
    # tentativa 5: 960s
    # tentativa 6: 1920s
    # tentativa 7+: capped em 3600s (1 hora)
    jitter = delay * 0.2
    delay = delay + random.uniform(-jitter, +jitter)  # +-20%
    return min(delay, max_delay)
```

**Por que jitter?** Sem jitter, jobs que falham juntos tentam de novo
juntos — outro choque de rebanho.

### 13.3 is_retryable_error

```python
def is_retryable_error(error: Exception) -> bool:
    # 4xx (exceto 429) → permanente
    # 429, 5xx, timeout, connection → retryável
    # Qualquer outra coisa → retryável (default otimista)
```

---

## 14. Módulo Contrib: Events (bubus)

**Arquivo**: `src/queue_max/contrib/events.py` (313 linhas)

### 14.1 O Que É

Um sistema de **eventos tipados** que conecta o Queue a um barramento
de eventos via [bubus](https://pypi.org/project/bubus/).

### 14.2 Eventos Definidos

```python
class JobEnqueued(BaseEvent):     job_id, shard_id
class JobCompleted(BaseEvent):    job_id, shard_id
class JobFailed(BaseEvent):       job_id, shard_id, error
class JobRetried(BaseEvent):      job_id, shard_id, error
class Alert(BaseEvent):           alert_type, pending, threshold
```

### 14.3 QueueEventBus

```python
events = QueueEventBus(queue)

# Handler tipado
@events.on(JobCompleted)
def handle(event: JobCompleted):
    print(f"Job {event.job_id} completed!")

# Pattern matching (wildcard)
@events.on("job_*")
def handle_any(event):
    print(f"Event: {type(event).__name__}")

# Aguardar evento (timeout)
result = events.expect(JobEnqueued, timeout=30)
```

### 14.4 Como Funciona

```
Queue._emit("job_completed", ...)
        │
        ▼
QueueEventBus._attach()
        │
        ▼
QueueEventBus._dispatch_async(event)
        │
        ▼  (via asyncio.run_coroutine_threadsafe)
Background event loop
        │
        ▼
bubus.EventBus.dispatch(event)
        │
        ├──► handler 1 (JobCompleted)
        ├──► handler 2 ("job_*")
        └──► handler 3 (expect)
```

Um thread + event loop dedicados (`daemon=True`) processam os eventos
de forma assíncrona sem bloquear o main thread.

---

## 15. Módulo Contrib: Integrações

### 15.1 Django

```python
# tasks.py
from queue_max.contrib.django import task

@task
def send_welcome(user_id):
    User = get_user_model()
    user = User.objects.get(id=user_id)
    send_email(user.email, "Welcome!")
```

**Management commands**:
```
python manage.py queue_worker
python manage.py queue_stats
python manage.py queue_purge
```

### 15.2 FastAPI

```python
from queue_max.contrib.fastapi import BackgroundQueue, QueueMiddleware

app = FastAPI()
app.add_middleware(QueueMiddleware, max_workers=4)

@app.post("/webhook")
async def webhook(payload: dict, background: BackgroundQueue):
    background.enqueue("process_webhook", payload=payload)
    return {"status": "accepted"}
```

### 15.3 Flask

```python
from queue_max.contrib.flask import QueueExtension

app = Flask(__name__)
queue = QueueExtension(app)

@queue.task
def send_email(user_id):
    ...

@queue.route("/notify/<int:user_id>")
def notify(user_id):
    send_email.delay(user_id=user_id)
    return "OK"
```

---

## 16. CLI

**Arquivo**: `src/queue_max/cli.py` (372 linhas)

### 16.1 Comandos

```
queue-max stats          — Métricas da fila
queue-max worker         — Inicia worker
queue-max enqueue        — Enfileira job (de args ou arquivo)
queue-max list           — Lista jobs (por status)
queue-max retry          — Re-enfileira jobs falhos
queue-max purge          — Limpa jobs
```

### 16.2 Exemplos

```bash
# Ver estatísticas
queue-max stats

# Iniciar worker
queue-max worker mymodule:process_function --workers 4

# Enfileirar job
queue-max enqueue --payload '{"task": "send_email", "to": "user@example.com"}' \
                  --priority 2

# Enfileirar lote de arquivo
queue-max enqueue --file jobs.jsonl

# Listar jobs pendentes
queue-max list --status pending

# Re-enfileirar jobs falhos
queue-max retry

# Limpar jobs antigos
queue-max purge --status completed
```

---

## 17. Exceções

**Arquivo**: `src/queue_max/exceptions.py` (25 linhas)

```
QueueError                      → Base de todas
├── RateLimitError              → Rate limiter estourou timeout
├── CircuitBreakerOpenError     → Circuit breaker aberto
├── JobFailedError              → Job falhou permanentemente
├── ShardError                  → Erro em shard específico
└── ConfigurationError          → Configuração inválida
```

---

## 18. Padrões de Projeto Aplicados

### 18.1 Strategy Pattern — Router

O roteamento de shard é um Strategy. Atualmente é uma função
(`determine_shard`), mas no roadmap vira uma classe `Router`
com `ModuloRouter`, `RandomRouter`, `ConsistentHashRouter`.

### 18.2 State Pattern — Circuit Breaker

O circuit breaker tem 3 estados (CLOSED, OPEN, HALF_OPEN) com
transições bem definidas. Cada estado determina o comportamento
de `is_allowed()`.

### 18.3 Template Method — Worker

`Worker` define o esqueleto do loop (`_run_loop`). `AsyncWorker`
sobrescreve o loop para usar asyncio, mantendo o processamento
de job igual.

### 18.4 Observer — Event System

`Queue.on(event, callback)` registra observers. `Queue._emit(event, data)`
notifica todos. O batch() context manager desativa temporariamente.

### 18.5 Proxy — ShardManager

`Queue` delega operações de banco para `ShardManager`, que gerencia
conexões, transações e concorrência. A Queue não sabe de SQLite.

### 18.6 Context Manager Pattern

Vários objetos suportam `with`:

```python
with Queue() as queue:       # Fecha conexões no exit
    queue.enqueue(payload)

with queue.batch():          # Silencia eventos
    queue.enqueue_batch(...)

with WorkerPool(workers):    # Para todos no exit
    pool.start_all()
```

### 18.7 Thread-Local Storage (TLS)

```python
self._local = threading.local()
```

Cada thread tem seu próprio cache de conexões SQLite. Evita
compartilhar objetos não thread-safe entre threads.

### 18.8 Token Bucket

Rate limiter implementa Token Bucket, que permite bursts controlados
e distribui a taxa uniformemente no tempo.

---

## 19. Concorrência e Thread Safety

### 19.1 Mapa de Locks

```
Queue
├── _shard_locks[shard_id]       → threading.Lock  → protege pop_job por shard
├── _events_lock                 → threading.Lock  → protege callbacks de eventos
│
ShardManager
├── _vacuum_lock                 → threading.Lock  → protege VACUUM (1 por vez)
├── _connections_lock            → threading.Lock  → protege set _all_connections
│
RateLimiter
├── _mutex                       → threading.Lock  → protege token bucket
│
CircuitBreaker
├── _mutex                       → threading.Lock  → protege estado + contagem
│
Worker
├── _job_mutex                   → threading.Lock  → protege _current_job
├── _stop_event                  → threading.Event → sinaliza parada
│
QueueEventBus
├── _metrics_lock                → threading.Lock  → protege métricas
├── _thread_started              → threading.Event → espera thread do event loop
```

### 19.2 SQLite Concurrency (Banco de Dados)

- **WAL mode**: Leitores não bloqueiam escritores
- **BEGIN IMMEDIATE**: Lock de escrito na primeira operação (evita deadlock)
- **busy_timeout=30000**: SQLite espera 30s antes de desistir
- **Thread-local connections**: Cada thread tem seus próprios objetos Connection

### 19.3 Race Conditions Prevenidas

| Cenário | Como é Prevenido |
|---------|------------------|
| Dois workers pegam o mesmo job | `BEGIN IMMEDIATE` + `SELECT ... LIMIT 1` + `UPDATE ... WHERE status='pending'` = atomic |
| Um worker vê dado inconsistente | Thread-local connections |
| VACUUM durante operação | `_vacuum_lock` |
| Dois eventos simultâneos | `_events_lock` |
| Close durante conexão ativa | `_connections_lock` + `_all_connections` |

### 19.4 O Que NÃO é Thread-Safe (E Por Que)

- **`close_all()`**: Fecha conexões de todas as threads. Se uma thread
  estiver usando a conexão no momento, pode crashar. Solução: chamar
  `close_all()` só depois que todos os workers pararam.
- **`batch()`**: O context manager atual (antes do fix) trocava `_emit`
  sem lock, e duas threads podiam corromper. Agora usa contador com lock.

---

## 20. Diagrama de Fluxo Completo

### Enfileirar e Processar um Job

```
Usuário                                    Worker
   │                                         │
   │  queue.enqueue(payload)                 │
   │  ├── validate_payload()                 │
   │  ├── validate_priority()                │
   │  ├── router.route(payload, pagina_id)   │
   │  ├── shard_manager.insert_job()         │
   │  │    └── INSERT INTO fila              │
   │  ├── _emit("job_enqueued")              │
   │  └── return {id, shard_id}              │
   │                                         │
   │                                         │  _run_loop()
   │                                         │  ├── rate_limiter.acquire()
   │                                         │  ├── circuit_breaker.is_allowed()
   │                                         │  ├── shard_manager.pop_job()
   │                                         │  │    ├── BEGIN IMMEDIATE
   │                                         │  │    ├── SELECT + UPDATE
   │                                         │  │    ├── COMMIT
   │                                         │  │    └── return Job
   │                                         │  ├── _process_job(job)
   │                                         │  │    ├── on_job_start() callback
   │                                         │  │    ├── process_function(payload)
   │                                         │  │    ├── complete_job() ou fail_job()
   │                                         │  │    ├── _emit("job_completed"/"job_failed")
   │                                         │  │    └── circuit_breaker.record_*()
   │                                         │  └── _send_heartbeat()
   │                                         │
```

### Estado do Job Através do Sistema

```
[PENDING] ──pop_job()──► [PROCESSING] ──complete_job()──► [DELETADO]
                              │
                              └──fail_job()────► [FAILED] ──retry()──► [PENDING]
                                      │              │
                                   (DLQ)         (se max_tentativas
                                    se              esgotou)
                                  permanente
```

---

## 21. Glossário

| Termo | Definição |
|-------|-----------|
| **Backoff** | Técnica de aumentar o tempo de espera entre tentativas de retry |
| **Circuit Breaker** | Padrão que abre o circuito quando falhas consecutivas excedem um limiar |
| **DLQ (Dead Letter Queue)** | Jobs que falharam permanentemente, guardados para inspeção |
| **Jitter** | Variação aleatória aplicada ao backoff para evitar choque de rebanho |
| **Job** | Unidade de trabalho na fila |
| **Orphan Recovery** | Jobs em "processing" cujo worker morreu — são re-agendados |
| **Rate Limiting** | Controle de taxa para não sobrecarregar serviços externos |
| **Shard** | Partição física dos dados (um arquivo .db por shard) |
| **ShardGroup** | Agrupamento de shards para otimizar scan do pop_job |
| **Token Bucket** | Algoritmo de rate limiting com balde de tokens que enchem a taxa constante |
| **WAL Mode** | Write-Ahead Log — permite leituras simultâneas com escritas no SQLite |
| **Worker** | Consumidor que processa jobs em loop |
| **WorkerPool** | Conjunto de workers com auto-scaling |

---

## Apêndice: Evolução do Projeto

### v0.1.0 — Renome e Fundação
- Renome de Robusta Queue para Queue Max
- SIGALRM → ThreadPoolExecutor
- Sharding físico com SQLite WAL
- CLI, tests, CI/CD

### v0.1.1 — Bug Fixes + Modernização
- Event loop leak no AsyncWorker (fechado)
- Thread safety no batch()
- f-strings de logging → lazy % formatting
- Dict/List → dict/list (type hints modernos)
- bubus dependency com python_version marker

### Fase 1 (Roadmap)
- Router Pattern (sharding plugável)
- Enhanced Batch (pop N jobs + buffer no decorator)
- Backpressure (max_pending + QueueFullError)
- File Locking (fcntl para multi-processo)

---

> **Queue Max — Feito para 90% dos casos. Sem Redis. Sem RabbitMQ. Sem desculpa.**
