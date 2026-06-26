# Aprenda Queue Max — Programação de Filas em Python

> Um guia didático, passo a passo, para entender **filas de tarefas**,
> **concorrência**, **SQLite**, **padrões de projeto** e **design de sistemas**
> através do código do Queue Max.

---

## Como usar este guia

Cada seção tem:

1. **O conceito** explicado em português claro
2. **O código** do Queue Max que implementa o conceito
3. **Por que** foi feito assim (e não de outra forma)
4. **Exercício mental** pra fixar

Para aprender de verdade: leia o código, entenda cada linha, pergunte
"por que isso e não aquilo?".

---

## Sumário

1. [Fila de Tarefas — Primeiro Passo](#1-fila-de-tarefas)
2. [SQLite como Banco de Filas](#2-sqlite-como-banco-de-filas)
3. [Sharding — Dividir para Conquistar](#3-sharding)
4. [Concorrência com Threads](#4-concorrência)
5. [Transações e Lock no SQLite](#5-transações-e-lock)
6. [Rate Limiting — Token Bucket](#6-rate-limiting)
7. [Circuit Breaker — Proteger o que Está Fora](#7-circuit-breaker)
8. [Retry com Backoff — Tentar de Novo com Inteligência](#8-retry)
9. [Dead Letter Queue — O que Fazer com Quem Falhou](#9-dlq)
10. [Worker — O Loop de Processamento](#10-worker)
11. [Eventos e Observers](#11-eventos)
12. [Decorator @task — API Elegante](#12-decorator)
13. [CLI com Argparse](#13-cli)
14. [Thread Safety — Mapa de Locks](#14-thread-safety)
15. [Padrões de Projeto no Queue Max](#15-padrões)

---

## 1. Fila de Tarefas

### O Problema

Sua aplicação web precisa enviar um email. Mas enviar email demora
500ms. Se o usuário esperar, a página fica travada.

**Solução ingênua**: fazer o envio na própria requisição:

```python
@app.post("/send-email")
def send_email(to: str):
    smtp.send(to, "Bem-vindo!")  # ← usuario espera 500ms
    return {"status": "sent"}
```

Problema: o usuário fica olhando pra tela enquanto o email não sai.

**Solução correta**: enfileirar e processar depois:

```python
@app.post("/send-email")
def send_email(to: str):
    queue.enqueue({"task": "send_welcome", "to": to})  # ← 2ms, instantâneo
    return {"status": "accepted"}
```

A requisição volta na hora. O worker processa em background.

### Como Queue Max Implementa

```python
# producer (sua view)
job = queue.enqueue({"to": user.email, "template": "welcome"})
# job = {"id": 42, "shard_id": 3}

# consumer (worker em outro terminal)
def process(payload):
    send_email(payload["to"], payload["template"])

worker = Worker("w1", process, queue)
worker.start()
```

O `enqueue` insere no SQLite. O `worker` faz `pop_job` num loop
infinito, processa, e marca como completo.

### Exercício

Se você tivesse que implementar uma fila do zero, o que usaria pra
guardar os jobs? Memória? Arquivo? Banco? Redis? Quais as vantagens
e desvantagens de cada um?

---

## 2. SQLite como Banco de Filas

### Por que SQLite?

Redis seria mais rápido. Mas Redis:

1. Precisa instalar e configurar (`apt install redis-server`...)
2. Precisa gerenciar conexão, pool, reinicialização
3. É um ponto de falha a mais na arquitetura

SQLite já vem instalado em todo Python. Zero dependências.

### Mas SQLite é banco de verdade?

Sim. SQLite não é "SQL de brinquedo". É usado em:

- Todos os iPhones e Androids (contatos, SMS, configurações)
- Browsers (Chrome, Firefox guardam histórico em SQLite)
- Aplicações com milhões de usuários

O que SQLite NÃO é: servidor cliente-servidor (como PostgreSQL).
Mas pra fila local, é perfeito.

### A Tabela

```sql
CREATE TABLE fila (
    id INTEGER PRIMARY KEY AUTOINCREMENT,   -- ID automático
    pagina_id INTEGER NULL,                 -- pra sharding consistente
    payload TEXT NOT NULL,                   -- os dados do job (JSON)
    status TEXT DEFAULT 'pending',           -- pending | processing | failed
    priority INTEGER DEFAULT 0,             -- 0=baixa, 1=média, 2=alta
    tentativas INTEGER DEFAULT 0,           -- quantas vezes tentou
    max_tentativas INTEGER DEFAULT 3,       -- máximo de tentativas
    last_error TEXT NULL,                    -- última mensagem de erro
    error_type TEXT NULL,                    -- classe do erro (ex: ValueError)
    worker_id TEXT NULL,                     -- quem está processando
    created_at TEXT DEFAULT (datetime('now')), -- quando foi criado
    next_retry_at TEXT NULL,                 -- quando tentar de novo
    ...
);
```

Cada linha = um job. Cada job tem status, prioridade, tentativas.

### As PRAGMAs — O Segredo do SQLite

Quando o Queue Max abre uma conexão SQLite, ele roda:

```python
PRAGMA journal_mode = WAL;           # Modo Write-Ahead Log
PRAGMA synchronous = NORMAL;         # Performance vs segurança
PRAGMA busy_timeout = 30000;         # Espera 30s em vez de dar erro
PRAGMA cache_size = 10000;           # 10MB de cache
PRAGMA mmap_size = 268435456;        # 256MB memória mapeada
PRAGMA temp_store = MEMORY;          # Temporárias na RAM
```

**WAL Mode**: sem WAL, leitura e escrita não podem acontecer ao mesmo
tempo. Com WAL, a escrita vai pra um log separado e a leitura acontece
no banco principal. As alterações são consolidadas depois.

```
Sem WAL: [LEITURA═══════][ESCRITA═══════][LEITURA════] → tudo serial
Com WAL: [LEITURA═════════════════════════════════════] → paralelo
         [ESCRITA═══][ESCRITA═══][ESCRITA═══]          → sem bloquear
```

Lembra do problema do GIL em Python? O WAL mode é um "GIL do SQLite"
— resolve o gargalo de leitura/escrita concorrente.

**busy_timeout = 30000**: Quando dois processos/threads tentam escrever
no mesmo `.db` ao mesmo tempo, um deles ganha e o outro espera. Sem
esse timeout, o perdedor recebe `sqlite3.OperationalError: database is
locked` na hora. Com 30s de timeout, ele espera educadamente.

---

## 3. Sharding

### O Problema

Se você tem 1 arquivo SQLite e 6 workers tentando escrever ao mesmo
tempo, eles competem pelo mesmo lock:

```
Worker 1 ──► INSERT em queue.db ──► LOCK ═══╗
Worker 2 ──► INSERT em queue.db ──► LOCK ═══╣ ESPERA
Worker 3 ──► INSERT em queue.db ──► LOCK ═══╣ ESPERA
Worker 4 ──► INSERT em queue.db ──► LOCK ═══╣ ESPERA
Worker 5 ──► INSERT em queue.db ──► LOCK ═══╣ ESPERA
Worker 6 ──► INSERT em queue.db ──► LOCK ═══╣ ESPERA
```

Só 1 worker escreve por vez. Os outros 5 ficam olhando.

### A Solução: Múltiplos Arquivos

Queue Max cria N arquivos .db (shards). Cada worker escreve no seu shard:

```
data/
├── shard_0.db   ← Worker 1 escreve aqui
├── shard_1.db   ← Worker 2 escreve aqui
├── shard_2.db   ← Worker 3 escreve aqui
├── shard_3.db   ← Worker 4 escreve aqui
├── shard_4.db   ← Worker 5 escreve aqui
└── shard_5.db   ← Worker 6 escreve aqui
```

Agora 6 workers escrevem simultaneamente sem competir.

### Como o Job Escolhe o Shard?

```python
def determine_shard(pagina_id, num_shards):
    if pagina_id is not None:
        return pagina_id % num_shards  # Roteamento consistente
    return random.randint(0, num_shards - 1)  # Aleatório
```

- Se você passa `pagina_id`, o mesmo ID vai sempre pro mesmo shard
  → consistência, ordenação por entidade
- Se não passa, fica aleatório → distribuição uniforme

### Por que ShardGroup Existe?

Quando um worker faz `pop_job()`, ele não sabe em qual shard tem job
disponível. Ele precisa procurar.

Procurar em 32 shards lineamente é ineficiente. O `ShardGroup`
organiza em grupos pra minimizar a busca:

```python
# 6 shards  → 1 grupo de 6   → procura em até 6
# 8 shards  → 4 grupos de 2  → procura em até 2 por grupo
# 16 shards → 4 grupos de 4  → procura em até 4 por grupo
# 32 shards → 8 grupos de 4  → procura em até 4 por grupo
```

O worker sorteia um grupo, procura lá. Se achar, retorna. Se não,
passa pro próximo grupo.

**Por que grupos e não só aleatoriedade pura?** Porque grupos reduzem
o número máximo de shards que um worker precisa verificar por
iteração. Com 32 shards puramente aleatórios, um worker pode precisar
verificar todos os 32 antes de achar um job. Com grupos de 4, ele
verifica no máximo 4.

---

## 4. Concorrência

### Thread vs Processo vs Async

Queue Max usa **threads**. Por quê?

| Abordagem | Prós | Contras |
|-----------|------|---------|
| **Threads** | Compartilham memória, leves | GIL (CPU-bound sofre) |
| **Processos** | Isolados, sem GIL | Comunicação complexa (pipe/socket) |
| **Async (asyncio)** | Levíssimo, milhares de conexões | Não pode ter I/O bloqueante |

Queue Max lida com I/O de rede (chamadas HTTP, email, API) e I/O de
disco (SQLite). Ambos liberam o GIL durante a operação, então threads
funcionam bem.

Se o processamento for CPU-bound (processar imagem, ML), threads não
ajudam — aí precisa de processos. Mas pra 90% dos casos de fila
(chamadas de API, envio de email, processamento de dados), threads
são suficientes.

### O Loop do Worker

```python
class Worker:
    def _run_loop(self):
        while not self._stop_event.is_set():  # ← pergunta "devo parar?"
            try:
                job = self.queue.pop_job(self.worker_id)
            except Exception:
                time.sleep(self.poll_interval)  # ← espera 1s
                continue

            if job is None:
                time.sleep(self.poll_interval)  # ← fila vazia, espera
                continue

            self._process_job(job)
            self._send_heartbeat()
```

Cada worker roda em sua própria thread:

```python
thread = threading.Thread(target=self._run_loop, daemon=True)
thread.start()
```

`daemon=True` significa: se o programa principal morrer, o worker
morre junto (não trava o shutdown).

### Como Parar um Worker

```python
def stop(self, timeout=10.0):
    self._stop_event.set()            # ← Sinaliza "pare"
    self._thread.join(timeout=timeout) # ← Espera a thread terminar
```

O `_stop_event` é um `threading.Event` — uma variável que uma thread
seta e outra thread observa. É a forma mais segura de comunicação
entre threads em Python. Sem eventos, você teria que usar variáveis
compartilhadas com lock, que é mais propenso a erro.

---

## 5. Transações e Lock no SQLite

### O Problema Clássico

Dois workers tentam pegar o mesmo job ao mesmo tempo:

```
Worker A: SELECT * FROM fila WHERE status='pending' LIMIT 1
Worker B: SELECT * FROM fila WHERE status='pending' LIMIT 1
           ↓
Worker A: → job 42
Worker B: → job 42  ← MESMO JOB!
           ↓
Worker A: UPDATE fila SET status='processing' WHERE id=42
Worker B: UPDATE fila SET status='processing' WHERE id=42
           ↓
JOB 42 EXECUTADO DUAS VEZES! ❌
```

### A Solução: Transação Atômica

Queue Max resolve com `BEGIN IMMEDIATE`:

```python
def pop_job(self, shard_id, worker_id):
    conn = self._get_connection(shard_id)
    try:
        conn.execute("BEGIN IMMEDIATE")  # ← PEGA O LOCK AGORA
        row = conn.execute("""
            SELECT * FROM fila
            WHERE status='pending' AND (next_retry_at IS NULL OR next_retry_at<=?)
            ORDER BY priority DESC, id ASC LIMIT 1
        """, (now,)).fetchone()
        if row is None:
            conn.commit()
            return None
        conn.execute("""
            UPDATE fila SET status='processing', worker_id=?
            WHERE id=? AND status='pending'
        """, (worker_id, row["id"]))
        conn.commit()                     # ← LIBERA O LOCK
        return Job.from_row(dict(row), shard_id=shard_id)
    except sqlite3.OperationalError:
        conn.rollback()
        return None
```

Com `BEGIN IMMEDIATE`, Worker A pega o lock. Worker B espera o
`busy_timeout` (30s). Quando Worker A faz `COMMIT`, Worker B
consegue o lock, mas aí o job já está `processing` e o SELECT
do Worker B não acha mais nada (porque filtra `status='pending'`).

```
Worker A: BEGIN IMMEDIATE → LOCK ═══╗
Worker B: BEGIN IMMEDIATE → LOCK ═══╣ ESPERA (busy_timeout)
                                     ║
Worker A: SELECT → acha job 42       ║
Worker A: UPDATE → status=processing ║
Worker A: COMMIT → libera lock ──────╝
                                     ╔══ LOCK LIBERADO ═══╗
Worker B: SELECT → NADA (job 42 já processing) ← CORRETO!
Worker B: COMMIT
```

**Por que `BEGIN IMMEDIATE` e não `BEGIN DEFERRED`?**

`DEFERRED` (padrão) só pega lock quando precisa escrever. Se Worker A
faz SELECT com `DEFERRED`, Worker B também pode fazer SELECT ao mesmo
tempo. Aí quando Worker A tenta escrever, o lock não está disponível.
Com `IMMEDIATE`, a intenção de escrever é declarada na hora.

---

## 6. Rate Limiting

### O Problema

Seu processamento de jobs chama uma API externa. API externa tem
limite: 100 requisições por minuto. Seu worker processa 200 jobs por
minuto — depois de 100 chamadas, a API começa a rejeitar (429).

### Token Bucket

Queue Max usa o algoritmo **Token Bucket**:

```
  ╔══════════════════════╗
  ║   🪙🪙🪙🪙🪙🪙        ║  ← tokens (permissões)
  ║  bucket = capacidade ║
  ╚══════════════════════╝
         ↑        ↓
   enche a      worker
   taxa fixa    tira 1 token
   (160/min)    por job
```

- O bucket começa cheio (ex: 160 tokens)
- Cada job precisa de 1 token
- Tokens são recarregados a taxa constante (160/60 = 2.67 tokens/s)
- Se o bucket acaba, o worker espera até ter token

### No Código

```python
def acquire(self, timeout=30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if self._try_acquire():
            return True
        time.sleep(0.1)  # ← espera um pouco e tenta de novo
    raise RateLimitError(...)

def _try_acquire(self):
    with self._mutex:            # ← thread-safe
        self._refill()           # ← calcula quantos tokens foram gerados desde a última vez
        if self._tokens >= 1.0:
            self._tokens -= 1.0  # ← consome 1 token
            return True
        return False
```

### Por que Jitter?

Sem jitter, N workers podem acordar no mesmo instante e todos tentar
adquirir token ao mesmo tempo — **thundering herd** (choque de rebanho).

```python
# Sem jitter: tokens adicionados exatamente a cada 0.375s
# 2 workers dormem 0.375s, acordam juntos, 1 ganha, 1 perde

# Com jitter: cada worker adiciona +-5% aleatório
# 2 workers dormem valores ligeiramente diferentes
# Acordam em momentos diferentes → sem choque
```

---

## 7. Circuit Breaker

### O Problema

Seu worker chama uma API externa que está fora do ar. O worker tenta,
falha, tenta de novo, falha de novo... Cada tentativa demora 30s pra
timeout. O worker fica preso, a fila cresce, e o sistema inteiro sofre.

### A Solução

Se um serviço está fora, **pare de tentar**. Espere um pouco, depois
tente de novo pra ver se voltou.

### Três Estados

```
   CLOSED (normal)
      │
      │  5 falhas consecutivas
      ▼
   OPEN (rejeitando)
      │
      │  60 segundos depois
      ▼
   HALF_OPEN (testando)
      │
      ├── sucesso → CLOSED (voltou!)
      └── falha   → OPEN (ainda quebrado)
```

### No Código do Queue Max

```python
def is_allowed(self):
    with self._mutex:
        if self.state == CLOSED:
            return True                    # ✅ normal
        elif self.state == OPEN:
            if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                self._set_state(HALF_OPEN)
                return True                # ⚠️ vamos ver se melhorou
            return False                   # ❌ não tenta
        elif self.state == HALF_OPEN:
            return True                    # ⚠️ uma tentativa de recuperação

def record_success(self):
    with self._mutex:
        self._failure_count = 0
        if self.state == HALF_OPEN:
            self._set_state(CLOSED)         # ✅ recuperou!

def record_failure(self):
    with self._mutex:
        self._failure_count += 1
        if self.state == HALF_OPEN:
            self._set_state(OPEN)           # ❌ ainda quebrado
        elif self._failure_count >= self.failure_threshold:
            self._set_state(OPEN)           # ❌ abriu!
```

### Onde é Chamado na Queue

```python
def pop_job(self, worker_id):
    if not self.circuit_breaker.is_allowed():
        return None          # ← Nem tenta processar
    ...

def complete_job(self, ...):
    self.circuit_breaker.record_success()  # ← Tudo ok

def fail_job(self, ...):
    self.circuit_breaker.record_failure()  # ← Acumula falha
```

### Por que Circuit Breaker é Melhor que Só Timeout?

Timeout trata o sintoma (cada requisição demora). Circuit breaker
trata a causa (serviço fora). Com circuit breaker:

- Workers não gastam tempo esperando timeout
- Serviço externo tem chance de se recuperar (menos carga)
- Sistema degrada graciosamente em vez de travar

---

## 8. Retry

### O Problema

Job falhou. O que fazer? Tentar de novo? Quantas vezes? Quando?

### Tipos de Erro

Queue Max classifica erros em duas categorias:

```python
def is_retryable_error(error):
    # 4xx (exceto 429) → permanente (ex: 400 Bad Request, 404 Not Found)
    # 429, 5xx, timeout, connection → retryável
    # Padrão: retryável (otimista)
```

**Erro permanente**: não adianta tentar de novo. O payload está errado,
o recurso não existe. Vai pra Dead Letter Queue.

**Erro retryável**: o servidor caiu, a rede falhou, deu rate limit.
Pode funcionar da próxima vez.

### Backoff Exponencial

Tentar de novo imediatamente não adianta (servidor ainda tá caído).
Tentar de novo daqui 1 hora pode ser tarde demais.

Backoff exponencial resolve:

```python
tentativa 1: 60s   (1 minuto)
tentativa 2: 120s  (2 minutos)
tentativa 3: 240s  (4 minutos)
tentativa 4: 480s  (8 minutos)
tentativa 5: 960s  (16 minutos)
tentativa 6+: 3600s (1 hora, cap)
```

### Por que Jitter no Backoff?

Sem jitter:

```
Job A falha às 10:00 → next_retry_at = 10:01
Job B falha às 10:00 → next_retry_at = 10:01
                        ↓
          10:01 → A e B tentam JUNTOS de novo
```

Com jitter:

```
Job A falha às 10:00 → next_retry_at = 10:01 + aleatório
Job B falha às 10:00 → next_retry_at = 10:01 + aleatório DIFERENTE
                        ↓
          10:01 → A tenta, B tenta em 10:02
```

### No Código

```python
def backoff_delay(tentativa, base_delay=60, max_delay=3600):
    delay = base_delay * (2 ** (tentativa - 1))   # 60, 120, 240...
    jitter = delay * 0.2                           # +-20%
    delay += random.uniform(-jitter, jitter)
    return min(delay, max_delay)
```

---

## 9. DLQ

### O Problema

Job tentou 3 vezes e falhou em todas. O que fazer com ele?

- Deixar na fila? → Vai ocupar espaço pra sempre
- Deletar? → Perde a informação do erro
- Tentar de novo? → Já tentou, não adianta

### A Solução: Dead Letter Queue

Jobs que esgotaram as tentativas vão pra DLQ — uma tabela separada
que guarda o payload, o erro, e o tipo do erro para inspeção manual.

```sql
CREATE TABLE dead_letter_queue (
    original_job_id INTEGER,  -- ID original do job
    payload TEXT NOT NULL,     -- O que ia processar
    error TEXT NOT NULL,       -- O que deu errado
    error_type TEXT NOT NULL,  -- Tipo do erro
    shard_id INTEGER           -- Qual shard
);
```

### Fluxo Completo de Falha

```python
def fail_job(self, shard_id, job_id, error, permanent):
    if permanent:
        # Vai direto pra DLQ
        conn.execute("INSERT INTO dead_letter_queue ...")
        conn.execute("UPDATE fila SET status='failed' ...")
    else:
        row = conn.execute("SELECT tentativas, max_tentativas ...")
        t = row["tentativas"] + 1
        if t > row["max_tentativas"]:
            # Esgotou → DLQ
            conn.execute("INSERT INTO dead_letter_queue ...")
            conn.execute("UPDATE fila SET status='failed' ...")
        else:
            # Ainda tem tentativa → agenda retry
            next_retry = backoff_delay(t)
            conn.execute("UPDATE fila SET next_retry_at=?, status='pending' ...")
```

---

## 10. Worker

### O Loop Infinito

Um worker é simplesmente um loop infinito que pergunta:

1. Tem job pra mim?
2. Se sim: processa
3. Se não: dorme e tenta de novo

```python
def _run_loop(self):
    while not self._stop_event.is_set():  # ← "devo parar?"
        try:
            job = self.queue.pop_job(self.worker_id)
        except Exception:
            self._idle_wait()
            continue

        if job is None:
            self._idle_wait()  # ← fila vazia, não queima CPU
            continue

        self._process_job(job)
        self._send_heartbeat()
```

### Por que `_stop_event.is_set()` e não `while True`?

Se for `while True`, a única forma de parar é matar o processo.
Com `_stop_event`, podemos pedir graciosamente:

```python
worker.stop(timeout=10)  # ← "termine seu job atual e pare"
```

O worker termina o job que está processando (ou espera até 10s) e
sai do loop.

### State Machine

```python
INITIALIZED → STARTING → RUNNING → STOPPING → STOPPED
                                  ↘           ↗
                                    ERROR
```

Cada transição é explícita:

```python
def start(self):
    if self._state in (RUNNING, STARTING):
        return  # ← já está rodando, não faz nada
    self._state = STARTING
    ...
    self._thread.start()
    self._state = RUNNING

def stop(self, timeout=10.0):
    self._state = STOPPING
    self._stop_event.set()
    self._thread.join(timeout=timeout)
    self._state = STOPPED if not self._thread.is_alive() else ERROR
```

**Por que state machine?** Impede bugs como:
- `start()` duas vezes → cria duas threads
- `stop()` enquanto está parado → crash
- Ver estado atual → dá pra monitorar

### AsyncWorker

Algumas funções de processamento são async (usam `await`). O
`AsyncWorker` usa `asyncio` em vez de thread pura:

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
            self._loop.close()     # ← sempre limpa o event loop

    async def _process_async(self, job):
        if asyncio.iscoroutinefunction(self.process_function):
            result = await self.process_function(job.payload)
        else:
            result = self.process_function(job.payload)  # sync funciona tbm
        self.queue.complete_job(job.id, job.shard_id)
```

**Por que não usar só AsyncWorker pra tudo?** Porque async não
funciona bem com funções que fazem blocking I/O (que a maioria das
funções de processamento faz). Se a função usa `requests.get()` (que
é bloqueante), o event loop inteiro trava.

### WorkerPool

Múltiplos workers:

```python
pool = WorkerPool([
    Worker("w1", process, queue),
    Worker("w2", process, queue),
    Worker("w3", process, queue),
])
pool.start_all()
pool.wait_for_idle()
pool.stop_all()
```

Com auto-scaling:

```python
pool = WorkerPool(workers=[Worker("w1", process, queue)],
                  auto_scale=True,
                  min_workers=1,
                  max_workers=10)
pool.start_all()
# Se pending > 100: sobe mais workers
# Se pending < 10: desce workers
```

---

## 11. Eventos

### O Problema

Você quer saber quando um job é completado, falha, ou é enfileirado.
Como? Ficar perguntando ("polling") é ineficiente.

### Observer Pattern

O Queue tem um sistema de eventos embutido:

```python
# Quem emite: Queue
def _emit(self, event, **data):
    for callback in self._events[event]:
        try:
            callback(**data)
        except Exception:
            logger.exception("Erro no handler de %s", event)

# Quem escuta: você
def meu_handler(job_id, shard_id):
    print(f"Job {job_id} completado no shard {shard_id}")

queue.on("job_completed", meu_handler)
```

### Eventos Disponíveis

| Evento | Disparado Quando | Payload |
|--------|-----------------|---------|
| `job_enqueued` | Job entra na fila | job_id, shard_id |
| `job_completed` | Job processado com sucesso | job_id, shard_id |
| `job_failed` | Job falha permanentemente | job_id, shard_id, error |
| `job_retried` | Job falha mas vai retentar | job_id, shard_id, error |
| `alert` | Número de jobs ultrapassa threshold | type, pending, threshold |

### Eventos Tipados com bubus (Extra)

O módulo `contrib/events.py` leva os eventos a outro nível:

```python
events = QueueEventBus(queue)

@events.on(JobCompleted)  # ← handler tipado!
def handle(event: JobCompleted):
    print(f"Job {event.job_id} feito!")

# Pattern matching — "job_*" pega qualquer evento de job
@events.on("job_*")
def handle_any(event):
    print(f"Evento: {type(event).__name__}")

# Esperar evento acontecer (timeout)
result = events.expect(JobEnqueued, timeout=30)
```

Isso usa bubus como barramento de eventos assíncrono, com thread
dedicada e event loop próprio.

---

## 12. Decorator

### O Problema

Toda vez que você quer enfileirar uma função, precisa fazer:

```python
def send_email(to, subject):
    # ... lógica ...

# Enfileirar
queue.enqueue({
    "task": "send_email",
    "args": ("user@example.com", "Oi"),
    "kwargs": {}
})
```

Muito repetitivo e propenso a erro.

### @task — API Elegante

```python
@task(priority=2, max_retries=3)
def send_email(to: str, subject: str):
    # ... lógica ...
    return smtp.send(to, subject)

# Uso direto (síncrono)
send_email("user@example.com", "Oi")

# Uso enfileirado (assíncrono)
send_email.delay("user@example.com", "Oi")

# Agendado
send_email.schedule_in(300, "user@example.com", "Oi")  # 5 minutos

# Múltiplos
send_email.map(["a@b.com", "c@d.com"], "Oi")
```

### Como Funciona

O decorator faz três coisas:

1. **Guarda a função original** para execução síncrona
2. **Cria `.delay()`** que valida argumentos, monta payload e chama `queue.enqueue()`
3. **Anexa metadados** (nome, versão, prioridade) para inspeção

```python
def task(queue=None, priority=0, max_retries=None, timeout=None):
    def decorator(func):
        _queue = queue or Queue()
        task_name = f"{func.__module__}.{func.__name__}"
        sig = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """Execução síncrona (chamada direta)"""
            sig.bind(*args, **kwargs)
            if timeout:
                with ThreadPoolExecutor() as e:
                    future = e.submit(func, *args, **kwargs)
                    return future.result(timeout=timeout)
            return func(*args, **kwargs)

        def delay(*args, **kwargs):
            """Execução assíncrona (enfileira)"""
            sig.bind(*args, **kwargs)
            payload = {
                "task": task_name,
                "args": args,
                "kwargs": kwargs,
                "timeout": timeout,
            }
            return _queue.enqueue(payload, priority=priority, max_retries=max_retries)

        wrapper.delay = delay
        wrapper.schedule_in = schedule_in
        wrapper.map = map
        wrapper.task_name = task_name
        wrapper.queue = _queue
        return wrapper
    return decorator
```

### Por que `functools.wraps`?

Sem `@functools.wraps`, a função decorada perde seu nome original,
docstring, e assinatura:

```python
@task()
def send_email(to):
    """Envia email"""
    pass

print(send_email.__name__)   # Sem wraps: "wrapper"
print(send_email.__name__)   # Com wraps: "send_email" ✅
print(send_email.__doc__)    # Com wraps: "Envia email" ✅
```

### @periodic_task — Tarefas Repetitivas

```python
@periodic_task(interval=3600)  # A cada hora
def cleanup():
    """Limpa jobs antigos"""
    queue.cleanup_old_jobs(days=7)

# Inicia o scheduler (roda em daemon thread)
cleanup.start_scheduler()
```

### @retryable_task — Retry Síncrono

Diferente do retry da fila (que espera e re-enfileira), este retry
tenta de novo imediatamente:

```python
@retryable_task(max_retries=5, retry_on=[TimeoutError])
def fetch_from_api(url):
    return requests.get(url, timeout=10).json()
    # Se der TimeoutError, tenta até 5x com backoff
```

---

## 13. CLI

### Por que CLI?

Nem todo mundo quer escrever código Python pra operar a fila.

```bash
# Ver estado
queue-max stats

# Iniciar worker
queue-max worker meu_modulo:minha_funcao --workers 4

# Enfileirar job
queue-max enqueue --payload '{"task": "send_email", "to": "x@y.com"}'

# Ver jobs pendentes
queue-max list --status pending --limit 20

# Ver jobs falhos com erro
queue-max list --status failed --show-error

# Re-enfileirar jobs falhos
queue-max retry

# Limpar jobs antigos
queue-max purge --status completed
```

### Como é Implementado

Usa `argparse` (padrão do Python, sem dependências):

```python
def build_parser():
    parser = argparse.ArgumentParser(description="Queue Max CLI")
    sub = parser.add_subparsers(dest="command")

    # subcomando "stats"
    p = sub.add_parser("stats", help="Exibe estatísticas")
    p.add_argument("--json", action="store_true")

    # subcomando "worker"
    p = sub.add_parser("worker", help="Inicia worker")
    p.add_argument("function", help="MODULO:FUNCAO")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--poll-interval", type=float, default=1.0)
    ...
```

---

## 14. Thread Safety

### O Básico

Duas threads modificando a mesma variável ao mesmo tempo = **race
condition**.

```python
# Código PROBLEMÁTICO
self._tokens -= 1.0
# Thread A e Thread B executam isso ao mesmo tempo
# self._tokens começa em 5.0
# Thread A lê 5.0, Thread B lê 5.0
# Thread A escreve 4.0, Thread B escreve 4.0
# Total: 2 tokens gastos, mas contagem foi de 5 → 4 (deveria ser 3)
```

### Solução: Mutex (Lock)

```python
self._mutex = threading.Lock()

def _try_acquire(self):
    with self._mutex:  # ← só 1 thread por vez aqui dentro
        self._tokens -= 1.0  # ← seguro
```

### Mapa de Todos os Locks no Queue Max

| Lock | O que Protege | Por que Precisa |
|------|--------------|-----------------|
| `_shard_locks[i]` | pop_job no shard i | Dois workers não podem pegar o mesmo job |
| `_events_lock` | Lista de callbacks de eventos | Adicionar/remover listener durante emissão |
| `_vacuum_lock` | VACUUM no SQLite | VACUUM precisa acesso exclusivo |
| `_connections_lock` | Lista de todas as conexões | close_all fecha conexões de forma segura |
| `_mutex` (rate_limiter) | Contagem de tokens | Race condition no token bucket |
| `_mutex` (circuit_breaker) | Estado + contagem de falhas | Race condition na abertura do circuito |
| `_job_mutex` (worker) | Job atual do worker | get_current_job() ler enquanto processa |

### Lock-Free: Thread-Local Connections

Cada thread tem seu próprio conjunto de conexões SQLite:

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

Thread A → conexão A para shard 0
Thread B → conexão B para shard 0

As conexões são objetos diferentes, então não precisam de lock
pra serem usadas. O SQLite gerencia a concorrência no arquivo.

---

## 15. Padrões de Projeto

Cada padrão de projeto é uma solução testada pra um problema
recorrente. Queue Max usa vários:

### 1. Strategy — Router (em planejamento)

**Problema**: Como decidir qual shard cada job vai?

**Solução**: Uma interface `Router` que qualquer um pode implementar.

```python
class ModuloRouter:
    def route(self, payload, pagina_id, num_shards):
        return pagina_id % num_shards if pagina_id else randint(0, num_shards-1)

class ConsistentHashRouter:
    def route(self, payload, pagina_id, num_shards):
        key = str(pagina_id or payload)
        return hashlib.md5(key.encode()).digest() % num_shards
```

O `Queue` aceita qualquer `Router` — o algoritmo de roteamento é
trocável sem modificar a Queue.

### 2. State — Circuit Breaker

**Problema**: O comportamento do circuit breaker muda dependendo do
estado (CLOSED, OPEN, HALF_OPEN).

**Solução**: Cada estado tem regras diferentes pra `is_allowed()`.

```python
def is_allowed(self):
    if state == CLOSED: return True
    if state == OPEN:
        if timeout_expirou():
            state = HALF_OPEN
            return True
        return False
    if state == HALF_OPEN: return True
```

### 3. Template Method — Worker/AsyncWorker

**Problema**: Worker normal e AsyncWorker compartilham 90% do código,
mas diferem no loop principal.

**Solução**: `Worker` define o esqueleto, `AsyncWorker` sobrescreve
só o que muda.

```python
class Worker:
    def start(self): ...  # igual pra ambos
    def stop(self): ...   # igual pra ambos
    def _process_job(self, job): ...  # igual pra ambos
    def _run_loop(self): ...          # DIFERENTE → AsyncWorker sobrescreve
```

### 4. Observer — Event System

**Problema**: Vários componentes precisam saber quando um job é
completado, sem a Queue precisar conhecer cada um.

**Solução**: `on()` registra interessados, `_emit()` notifica todos.

### 5. Proxy — ShardManager

**Problema**: `Queue` não deveria lidar com SQLite, conexões, transações.

**Solução**: `ShardManager` é um proxy que abstrai todo o SQLite.
A Queue chama `shard_manager.insert_job()` e não sabe se é SQLite,
PostgreSQL, ou arquivo texto.

### 6. Context Manager — with

**Problema**: Recursos (conexões, workers) precisam ser limpos.

**Solução**: `__enter__`/`__exit__` para uso com `with`:

```python
with Queue() as q:
    q.enqueue(payload)
# conexões fechadas automaticamente

with WorkerPool(workers) as pool:
    pool.start_all()
# workers parados automaticamente
```

### 7. Token Bucket — Rate Limiter

**Problema**: Controlar taxa de requisições sem perder a capacidade
de lidar com bursts.

**Solução**: Tokens acumulam até o burst máximo.

---

## 16. Construa Sua Própria Fila em 5 Passos

Pra entender profundamente, nada melhor que construir. Vamos
implementar uma fila mínima passo a passo.

### Passo 1: O Banco

```python
import sqlite3, json, threading, time

conn = sqlite3.connect("fila.db")
conn.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT (datetime('now'))
    )
""")
conn.commit()
```

### Passo 2: Enfileirar

```python
def enqueue(payload):
    conn.execute(
        "INSERT INTO jobs (payload) VALUES (?)",
        (json.dumps(payload),)
    )
    conn.commit()
    print(f"Job enfileirado")
```

### Passo 3: Processar (com Race Condition)

```python
def pop_job_INGENUO():
    row = conn.execute(
        "SELECT * FROM jobs WHERE status='pending' ORDER BY id LIMIT 1"
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE jobs SET status='processing' WHERE id=?",
            (row["id"],)
        )
        conn.commit()
        return json.loads(row["payload"])
    return None
```

**Problema**: Se 2 threads rodam isso ao mesmo tempo, ambas pegam
o mesmo job. É a race condition clássica.

### Passo 4: Processar (Correto — com Transação)

```python
def pop_job_CORRETO():
    conn.execute("BEGIN IMMEDIATE")              # ← lock AGORA
    row = conn.execute(
        "SELECT * FROM jobs WHERE status='pending' ORDER BY id LIMIT 1"
    ).fetchone()
    if not row:
        conn.commit()
        return None
    conn.execute(
        "UPDATE jobs SET status='processing' WHERE id=? AND status='pending'",
        (row["id"],)
    )
    conn.commit()                                 # ← libera lock
    return json.loads(row["payload"])
```

A diferença: `BEGIN IMMEDIATE` + `WHERE status='pending'` no UPDATE.
Se outra thread já pegou o job, o UPDATE não afeta nenhuma linha.

### Passo 5: Worker Completo

```python
class Worker:
    def __init__(self, process_function):
        self.process_function = process_function
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self):
        while not self._stop.is_set():
            payload = pop_job_CORRETO()
            if payload:
                try:
                    self.process_function(payload)
                    print("Job OK")
                except Exception as e:
                    print(f"Job falhou: {e}")
            else:
                time.sleep(1)  # fila vazia, espera
```

**Pronto!** Você tem uma fila funcional em 50 linhas.

### O Que Queue Max Adiciona a Isso

| Sua fila de 50 linhas | Queue Max |
|---|---|
| 1 arquivo SQLite | N shards (N arquivos, sem lock competition) |
| Sem índice | Índices em status, prioridade, heartbeat |
| Sem prioridade | Prioridade 0, 1, 2 |
| Sem retry | Retry com backoff exponencial + jitter |
| Sem DLQ | Dead Letter Queue para jobs que esgotaram |
| Sem rate limit | Token bucket (160 req/min default) |
| Sem circuit breaker | 3 estados com recovery automático |
| Sem heartbeat | Workers reportam atividade |
| Orphan: job preso pra sempre | Recover orphans (jobs em processing sem heartbeat) |
| 1 thread | WorkerPool com auto-scaling |
| Sleep fixo | Poll interval configurável |
| Sem CLI | `queue-max stats`, `worker`, `list`, `retry`, `purge` |

Cada feature do Queue Max resolve um problema real que sua fila
simples teria em produção.

---

## 17. Debugging na Prática

### Problema 1: "Meus jobs não estão sendo processados"

**Diagnóstico**:

```bash
# 1. Verifique se tem jobs na fila
queue-max stats

# Saída:
# Pending: 150    ← tem jobs!
# Processing: 0   ← ninguém processando
# Failed: 3

# 2. Verifique se o worker está rodando
ps aux | grep queue-max
# Se não tiver: worker não foi iniciado

# 3. Verifique o circuit breaker
queue-max stats
# circuit_state: open  ← AHÁ! Circuito aberto!
```

**Circuito aberto?** O serviço externo falhou muito. Espere
60s (recovery_timeout) ou reinicie:

```python
queue.circuit_breaker.reset()
# ou via CLI: (implementar reset command)
```

### Problema 2: "Job falhou e não está retentando"

**Diagnóstico**:

```bash
# Ver jobs falhos
queue-max list --status failed --show-error

# Saída:
# Job 42: "Connection refused" (ConnectionError)
# Job 43: "Division by zero"   (ZeroDivisionError)
```

`ConnectionError` é retryável (deveria ter retentado).
`ZeroDivisionError` é permanente (não adianta retentar — é bug no código).

O `is_retryable_error()` classifica:

```python
def is_retryable_error(error):
    error_str = str(error).lower()
    # 400, 404 → permanente (False)
    # 429, 500, timeout, connection → retryável (True)
    # Padrão → True (otimista)
```

ZeroDivisionError → padrão → True → **retryável**. Isso pode não ser
o desejado. Se o erro é bug, você quer que vá pra DLQ pra investigar,
não que fique retentando pra sempre.

### Problema 3: "Rate limit está muito baixo/alto"

```python
# Por ambiente (sem modificar código)
export RATE_LIMIT_MAX=500
queue = Queue()  # ← vai ler RATE_LIMIT_MAX=500

# Por parâmetro
queue = Queue(rate_limit=500)

# Por task específica
@task(rate_limit=50)  # cria Queue própria com rate limit diferente
```

### Problema 4: "Too many open files"

```bash
# Sintoma: erro "Too many open files" no log
# Causa: muitas conexões SQLite abertas (cada thread tem N conexões)

# Solução 1: Aumentar ulimit
ulimit -n 65535

# Solução 2: Fechar conexões não usadas
queue.close()  # Fecha todas
```

### Problema 5: "database is locked"

```bash
# Sintoma: log repetindo "database is locked"
# Causa: muita contenção no shard

# Solução 1: Mais shards (espalhar carga)
Queue(shards=16)  # Antes: 6

# Solução 2: Menos workers por shard
# Se você tem 6 shards e 12 workers, cada shard tem em média 2 workers
# competindo. Com 6 workers e 6 shards, cada um tem seu próprio shard.

# Solução 3: Aumentar busy_timeout (mais paciência)
export DB_BUSY_TIMEOUT=60000  # 60 segundos de espera
```

### Problema 6: Orphan Jobs

```python
# Sintoma: jobs em "processing" pra sempre
# Causa: worker morreu (crash, kill -9) sem marcar complete ou fail
# Jobs ficam "processing" mas ninguém está processando

# Solução: recuperar órfãos
total = queue.recover_orphans()
print(f"{total} jobs recuperados")
# Re-agenda como 'pending' com next_retry_at = agora
```

---

## 18. Erros Comuns e Como Evitar

### Erro 1: Compartilhar conexão SQLite entre threads

```python
# ❌ ERRADO
conn = sqlite3.connect("shard_0.db")

def worker():
    conn.execute(...)  # mesma conexão em múltiplas threads!

# ✅ CORRETO (Queue Max usa thread-local)
# Cada thread tem sua própria conexão
def worker():
    local_conn = sqlite3.connect("shard_0.db")  # conexão exclusiva
    local_conn.execute(...)
```

### Erro 2: Não tratar exceções no processamento

```python
# ❌ ERRADO
def process(payload):
    resultado = 1 / 0  # ZeroDivisionError → job some sem trace

# ✅ CORRETO (Queue Max captura)
# Se der erro, fail_job() é chamado, job vai pra retry ou DLQ com stack trace
```

### Erro 3: Payload não serializável

```python
# ❌ ERRADO
queue.enqueue({"data": datetime.now()})  # datetime não é JSON!

# ✅ CORRETO
queue.enqueue({"data": datetime.now().isoformat()})  # string ISO
```

Queue Max valida na hora:
```python
def validate_payload(payload):
    if not isinstance(payload, dict):
        raise ValueError("Payload precisa ser dict")
    json.dumps(payload)  # testa serialização → já levanta erro aqui
    return payload
```

### Erro 4: Confundir erro permanente com retryável

```python
# ❌ ERRADO (assumir que tudo é retryável)
# Job com payload inválido vai ficar tentando pra sempre

# ✅ CORRETO (Queue Max classifica)
# 400 Bad Request → permanente
# 429 Too Many Requests → retryável
# ConnectionError → retryável
# ValueError → retryável (padrão otimista)
```

### Erro 5: Criar Queue para cada job

```python
# ❌ ERRADO
for item in items:
    q = Queue()  # nova Queue = nova inicialização de shards!
    q.enqueue(item)

# ✅ CORRETO
q = Queue()
for item in items:
    q.enqueue(item)
# ou melhor ainda:
q.enqueue_batch(items)
```

### Erro 6: Ignorar o rate limiter

```python
# ❌ ERRADO
queue = Queue(rate_limit=10)  # 10 req/min
# Seu worker vai processar 10 jobs por minuto e os outros 90 vão
# ficar esperando. Não é bug, é feature — mas surpreende.

# ✅ CORRETO
queue = Queue(rate_limit=160)  # 160 req/min (default)
# Ajuste conforme o limite da API externa que você chama.
```

### Erro 7: Não fechar a Queue

```python
# ❌ ERRADO
queue = Queue()
queue.enqueue(...)
# queue.close()  ← esqueceu!

# ✅ CORRETO
with Queue() as queue:
    queue.enqueue(...)
# close() automático no __exit__
```

---

## 19. Testando uma Fila

### Teste Unitário vs Teste de Integração

Fila é um sistema com estado (banco de dados, threads). Testar
isoladamente é limitado — você precisa de testes de integração.

### Testando o Queue

```python
def test_enqueue_e_pop():
    queue = Queue(shards=1)  # 1 shard pra simplificar

    # Enfileira
    result = queue.enqueue({"task": "test"})
    assert "id" in result
    assert result["shard_id"] == 0

    # Pop
    job = queue.pop_job("worker-1")
    assert job is not None
    assert job.payload["task"] == "test"
    assert job.status == JobStatus.PROCESSING

    # Complete
    queue.complete_job(job.id, job.shard_id)
    assert queue.is_empty()

    queue.close()
```

### Testando Concorrência

```python
def test_concurrent_enqueue():
    queue = Queue(shards=4)

    def enqueue_many(start, count):
        for i in range(start, start + count):
            queue.enqueue({"i": i})

    threads = [
        threading.Thread(target=enqueue_many, args=(0, 50)),
        threading.Thread(target=enqueue_many, args=(50, 50)),
    ]
    for t in threads: t.start()
    for t in threads: t.join()

    stats = queue.get_stats()
    assert stats["pending"] == 100  # nenhum job perdido!

    queue.close()
```

### Testando o Circuit Breaker

```python
def test_circuit_breaker_abre():
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=0.5)

    assert cb.is_allowed()  # CLOSED → permite

    cb.record_failure()     # 1 falha
    cb.record_failure()     # 2 falhas
    cb.record_failure()     # 3 falhas → abre!

    assert not cb.is_allowed()  # OPEN → rejeita

    time.sleep(0.6)         # espera recovery_timeout

    assert cb.is_allowed()  # HALF_OPEN → permite tentativa
```

### Testando o Worker

```python
def test_worker_processa():
    queue = Queue(shards=1)
    queue.enqueue({"task": "test"})

    results = []

    def process(payload):
        results.append(payload["task"])

    worker = Worker("w1", process, queue, poll_interval=0.1)
    worker.start()
    time.sleep(0.5)  # espera processar
    worker.stop()

    assert results == ["test"]
    assert queue.is_empty()
    queue.close()
```

### Por que Testes de Fila São Diferentes

| Aspecto | Teste Normal | Teste de Fila |
|---------|-------------|---------------|
| Estado | Função pura, sem estado | Banco de dados, threads |
| Tempo | Milissegundos | Precisa de `time.sleep()` pra esperar worker |
| Concorrência | Single thread | Múltiplas threads |
| Limpeza | Automática | Precisa fechar conexões, parar workers |
| Reprodutibilidade | Determinístico | Race conditions podem não reproduzir |

Dica: use `time.sleep()` com moderação. Prefira `wait_until_empty()`
e `wait_for_jobs()` que são mais precisos.

---

## 20. Performance: O Que Esperar

### Números de Referência

Com configuração default (6 shards, rate_limit=160):

| Cenário | Jobs/s | Observação |
|---------|--------|------------|
| Enqueue simples | ~500/s | INSERT com 1 shard |
| Enqueue batch (100) | ~5000/s | executemany com 1 shard |
| Pop + Complete | ~50/s | Limitado pelo rate limiter (160/min) |
| Pop + Complete (sinlimite) | ~200/s | SQLite contention |
| 6 workers, 6 shards | ~200/s | Paralelismo real |
| 12 workers, 6 shards | ~250/s | Shards viram gargalo |

### Gargalos Comuns

1. **Rate limiter** (default 160/min) → Aumente se não estiver chamando
   API externa
2. **SQLite contention** → Aumente shards
3. **Process function lenta** → A culpa é da sua função, não da fila
4. **WAL mode desabilitado** → Sem WAL, leitura e escrita competem

### Onde o Queue Max Não é Adequado

- **Altíssimo throughput** (> 10.000 jobs/s) → Redis/Kafka
- **Múltiplos servidores** → Falta coordenação distribuída
- **Jobs CPU-bound** → Precisa de multiprocessing (não threads)
- **Filas globais** → Sharding é local a uma máquina

---

## 21. SQLite vs Redis para Filas

### Quando SQLite Ganha

| Cenário | SQLite | Redis |
|---------|--------|-------|
| Projeto pequeno/médio | ✅ **0 setup** | Precisa instalar/configurar |
| Single-server | ✅ **Arquivo local** | Processo extra rodando |
| Persistência | ✅ **Em disco** | Depende de configuração RDB/AOF |
| Dados relacionais | ✅ **SQL** | Precisa gerenciar manualmente |
| Consultas complexas | ✅ **SELECT, JOIN, WHERE** | Chave-valor limitado |
| Timeout/retry | ✅ **next_retry_at + WHERE** | Precisa de sorted sets |
| Dead letter queue | ✅ **Tabela separada** | Lista ou set separado |

### Quando Redis Ganha

| Cenário | Redis | SQLite |
|---------|-------|--------|
| Multi-server | ✅ Nativo | ❌ Precisa de locking extra |
| Altíssimo throughput | ✅ ~100K ops/s | ❌ ~1K ops/s |
| Pub/sub em tempo real | ✅ Nativo | ❌ Precisa polling |
| Cache | ✅ Na memória | ❌ Em disco |
| Estruturas complexas | ✅ Lists, Sets, Sorted Sets | ✅ SQL |

### A Verdade

Redis ganha em **throughput** e **distribuição**. SQLite ganha
em **simplicidade** e **zero dependências**.

A pergunta certa é: "meu projeto precisa de 10.000 ops/s ou
100 ops/s?" Se for 100 ops/s (e é pra maioria), SQLite resolve.

Queue Max existe porque tem muito projeto que usa Redis "porque
sim" quando SQLite já resolvia.

---

## 22. Como Queue Max Lida com Falhas

### Matriz de Falhas

| O Que Falha | O Queue Max Faz | Você Precisa Fazer |
|-------------|----------------|--------------------|
| Worker morre (crash) | Job fica "processing". `recover_orphans()` re-agenda | Chamar recover periódico ou agendar no cron |
| Disco cheio | SQLite levanta `OperationalError` (logado) | Liberar espaço |
| API externa fora | Circuit breaker abre → workers param | Esperar recovery ou reset manual |
| Rate limit externo | `is_retryable_error` → True → job retenta | Nada (backoff automático) |
| Erro no código | fail_job() com stack trace na DLQ | Corrigir o bug, re-enfileirar da DLQ |
| Thread morre | Worker preso em join(timeout) → log | Verificar código do process_function |

### Garantias

- **At-least-once**: Jobs não são perdidos (se o worker morre, orphan
  recovery re-agenda). Mas podem ser processados mais de uma vez.
- **No duplicação**: Dentro do mesmo processo, `BEGIN IMMEDIATE` +
  `WHERE status='pending'` impede double-claim. Entre processos,
  depende de file locking.
- **Ordenação**: Jobs da mesma prioridade são FIFO. Jobs de prioridade
  maior sempre antes. Entre shards, ordem não é garantida.

---

## 23. FAQ Didático

### "Por que usar fila e não thread direto?"

Thread direto:

```python
Thread(target=send_email, args=(to,)).start()  # simples, né?
```

Problemas:
- Se o servidor reinicia, o email é perdido (está na memória)
- Se 1000 usuários chamam ao mesmo tempo, 1000 threads são criadas
- Sem retry, sem monitoramento, sem visibilidade

Fila resolve:
- Jobs persistem no disco (sobrevivem a restart)
- Workers controlados (pool com limite)
- Retry automático
- Métricas e debugging

### "Por que SQLite e não arquivo JSON?"

Arquivo JSON:

```python
with open("fila.json", "r+") as f:
    fila = json.load(f)
    fila.append(job)
    json.dump(fila, f)
```

Problemas:
- Ler e escrever o arquivo inteiro a cada operação
- Dois processos corrompem o arquivo
- Sem índices, sem consultas, sem transações
- Sem concorrência segura

SQLite resolve tudo isso com 50 anos de engenharia de banco de dados.

### "6 shards é sempre o ideal?"

Não. A fórmula:
- **Poucos workers** (1-2) → 1-2 shards (menos arquivos pra gerenciar)
- **Médio** (3-10 workers) → 6 shards (default)
- **Muitos workers** (10+) → 16-32 shards

Regra: shards = workers * 1.5 aproximadamente. Cada worker precisa
conseguir um shard sem competir muito.

```python
# 4 workers
Queue(shards=4)   # Cada worker tem "seu" shard

# 20 workers
Queue(shards=12)  # Alguma competição, mas saudável
```

### "Worker com timeout vs sem timeout — qual usar?"

**Sem timeout**: A função pode travar pra sempre (ex: request sem
timeout). Não recomendado para produção.

**Com timeout**: Um `ThreadPoolExecutor` executa a função em outra
thread. Se exceder o tempo, `TimeoutError` é levantado.

```python
# Recomendado
Worker("w1", process, queue, job_timeout=30)
```

O custo: um `ThreadPoolExecutor` dedicado por worker. Impacto mínimo.

### "AsyncWorker vs Worker — qual escolher?"

Use `AsyncWorker` se sua `process_function` é async:

```python
async def process(payload):
    async with httpx.AsyncClient() as client:
        await client.post(url, json=payload)

worker = AsyncWorker("w1", process, queue)
```

Use `Worker` se sua função é síncrona:

```python
def process(payload):
    requests.post(url, json=payload)

worker = Worker("w1", process, queue)
```

AsyncWorker também funciona com funções síncronas, mas perde a
vantagem do async (não tem concorrência real).

### "Quantos workers devo rodar?"

Depende do gargalo:

| Gargalo | Workers | Estratégia |
|---------|---------|------------|
| CPU (sua função processa pesado) | 1-2 | Mais workers não ajudam (GIL) |
| I/O (chamadas HTTP, banco) | 10-20 | Workers paralelizam I/O |
| API externa (rate limited) | Rate limit / taxa | Workers controlados pelo rate limiter |
| Disco (SQLite) | shards * 2 | Workers competem pelos shards |

Na dúvida, comece com `WorkerPool` e auto-scaling:

```python
pool = WorkerPool(
    workers=[Worker("w1", process, queue)],
    auto_scale=True,
    min_workers=1,
    max_workers=10,
)
```

---

## 24. Exercícios com Resolução

### Exercício 1: Implemente um Router Customizado

**Problema**: Jobs do tipo "email" devem ir pros shards 0-2. Jobs
do tipo "relatório" devem ir pros shards 3-5.

**Tente antes de olhar a resposta**.

```python
# RESPOSTA
class TipoRouter:
    """Router que separa tipos de job em shards diferentes."""
    def route(self, payload, pagina_id, num_shards):
        tipo = payload.get("tipo", "email")
        if tipo == "email":
            return random.randint(0, 2)       # shards 0-2
        elif tipo == "relatorio":
            return random.randint(3, 5)       # shards 3-5
        return pagina_id % num_shards if pagina_id is not None else random.randint(0, num_shards - 1)

# Uso no Queue Max (quando Router Pattern for implementado)
queue = Queue(shards=6, router=TipoRouter())
queue.enqueue({"tipo": "email", "to": "user@example.com"})   # → shard 0-2
queue.enqueue({"tipo": "relatorio", "data": "..."})          # → shard 3-5
```

### Exercício 2: Rate Limiter Adaptativo

**Problema**: Quando a fila está cheia (>80%), reduzir o rate limit
pra dar tempo dos workers processarem. Quando vazia (<20%), aumentar.

```python
# RESPOSTA (simplificada)
class AdaptiveRateLimiter:
    def __init__(self, base_rate=160):
        self.base_rate = base_rate
        self.current_rate = base_rate
        self._last_adjust = 0

    def adjust(self, pending_pct):
        now = time.time()
        if now - self._last_adjust < 5:  # só ajusta a cada 5s
            return
        if pending_pct > 0.8:
            self.current_rate = max(10, int(self.base_rate * (1 - pending_pct)))
        elif pending_pct < 0.2:
            self.current_rate = self.base_rate
        self._last_adjust = now
```

### Exercício 3: Encontre e Corrija o Bug

```python
# O que há de errado com este código?
queue = Queue(shards=6)

def process(payload):
    user = User.objects.get(id=payload["user_id"])
    send_email(user.email, "Oi")

worker = Worker("w1", process, queue)
worker.start()
```

**Resposta**: Se `payload["user_id"]` não existir, dá `KeyError`.
O worker captura a exceção e chama `fail_job()`. O job vai pra DLQ
com o stack trace. O erro é permanente (é bug no código, não falha
de rede). **Correção**: trate o erro ou garanta que o payload tem
o campo.

---

## 25. Glossário Visual

```
┌─────────────────────────────────────────────────────────────────┐
│                        QUEUE MAX GLOSSÁRIO                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  FILA       = Lista ordenada de tarefas esperando processamento │
│  JOB        = Uma unidade de trabalho (payload + metadados)     │
│  SHARD      = Arquivo SQLite contendo um subconjunto dos jobs   │
│  WORKER     = Thread que processa jobs em loop                  │
│  POLL       = Perguntar "tem job?" repetidamente                │
│  BACKOFF    = Aumentar tempo de espera entre tentativas         │
│  JITTER     = Variação aleatória pra evitar choque de rebanho  │
│  DLQ        = Dead Letter Queue (jobs que falharam pra sempre)  │
│  ORPHAN     = Job travado em "processing" porque o worker morreu│
│  CB         = Circuit Breaker (para de tentar se serviço caiu)  │
│  TOKEN      = Permissão pra executar 1 job (rate limiter)       │
│  BUCKET     = Acumulador de tokens (pra permitir bursts)        │
│  WAL        = Write-Ahead Log (leitura não bloqueia escrita)    │
│  HEARTBEAT  = Sinal de vida do worker (pra detectar órfãos)     │
│  IMMEDIATE  = Lock de escrita obtido na hora (evita deadlock)   │
│  TENTATIVA  = Quantas vezes o job foi executado                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

"O código é a fonte da verdade. O manual é só um mapa.
Se o mapa e o código discordam, o mapa está errado."

