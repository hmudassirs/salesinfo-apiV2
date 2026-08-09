I reviewed both load-test JSON reports and perf.md. The comparison is
quite clear: **moving from 1 Unicorn worker to 4 workers improves
concurrency handling, but it does not solve the underlying latency
problem.**

**1. Direct comparison**

  -------------------------------------------------
  **Metric**           **1         **4   **Change**
                  worker**   workers** 
  ------------- ---------- ----------- ------------
  Requests             500         500          ---

  Concurrency          500         500          ---

  Success          500/500     500/500          ---

  Failures               0           0          ---

  Wall time         7.31 s  **6.73 s**       **7.8%
                                           better**

  Throughput    68.4 req/s      **74.2       **8.5%
                               req/s**     better**

  Min latency       350 ms      367 ms   \~5% worse

  Avg latency      3646 ms **2732 ms**      **25.1%
                                           better**

  p50              3825 ms **2361 ms**      **38.3%
                                           better**

  p95              6578 ms **6213 ms**       **5.5%
                                           better**

  p99              7029 ms **6498 ms**       **7.5%
                                           better**

  Max              7110 ms **6646 ms**       **6.5%
                                           better**
  -------------------------------------------------

The reports confirm that both tests were identical: 500 concurrent
requests executing SELECT 1 AS ok, with all 500 returning HTTP 200.

The actual commands were also explicitly 1 worker versus 4 workers.

**2. The most important finding: PostgreSQL is NOT the bottleneck**

This is the key point.

Your test query is:

SELECT 1 AS ok

and the server-side instrumentation shows:

**1 worker**

- sql_execute mean ≈ **1.89 ms**

- pool_acquire mean ≈ **0.08 ms**

- pool_release mean ≈ **0.024 ms**

- sql_fetch_one ≈ **3.13 ms**

**4 workers**

- sql_execute mean ≈ **2.11 ms**

- pool_acquire mean ≈ **0.084 ms**

- pool_release mean ≈ **0.023 ms**

- sql_fetch_one ≈ **3.25 ms**

These are tiny compared with the **2.7--3.6 second application-level
latency**.

So I would **not increase the PostgreSQL pool aggressively** as the next
optimization.

Your pool has 16 idle connections at the snapshot, and pool acquisition
is only around 0.08 ms.

That is strong evidence that:

**The request is spending most of its time somewhere before/around
application dispatch, not waiting for PostgreSQL.**

**3. Four workers clearly reduce application contention**

This is where the 4-worker result is meaningful.

With 1 worker:

dispatch p50 = **28.6 ms**\
dispatch p95 = **223.3 ms**\
dispatch p99 = **229.0 ms**

With 4 workers:

dispatch p50 = **18.0 ms**\
dispatch p95 = **55.0 ms**\
dispatch p99 = **71.8 ms**

That\'s a substantial improvement in the server\'s dispatch behavior.

In particular:

**dispatch p95**

223 ms → 55 ms

That\'s about a **75% reduction**.

So the 4 workers are doing what we would expect: they allow more
requests to be processed simultaneously rather than making one worker
absorb the entire 500-request burst.

**4. But there is still a huge gap between dispatch and client latency**

This is the most interesting part of the results.

For 4 workers:

**Client p50**

≈ **2361 ms**

while:

**Server dispatch p50**

≈ **18 ms**

And:

**Client p95**

≈ **6213 ms**

while:

**Server dispatch p95**

≈ **55 ms**

That discrepancy is enormous.

It tells us that the 2--6 second latency isn\'t coming from:

- PostgreSQL execution

- connection acquisition

- connection release

- the actual SELECT 1

- normal application dispatch execution

at least not according to the instrumentation currently recorded.

The server instrumentation shows very small SQL and pool timings while
the external load tester sees multi-second request times.

Therefore, **the next investigation should be the request path outside
those measured sections.**

**5. The latency distribution is especially revealing**

I calculated the raw-request distribution from both JSON files.

**1 worker**

Out of 500:

- **183 requests \> 5 seconds**

- **91 requests \> 6 seconds**

- **242 requests \> 4 seconds**

**4 workers**

- **84 requests \> 5 seconds**

- **36 requests \> 6 seconds**

- **121 requests \> 4 seconds**

So 4 workers nearly cut the number of \>5-second requests in half.

That is good.

But **84/500 requests still taking over 5 seconds is far too high** for:

SELECT 1

This is not a database-query-performance problem.

It\'s a **high-concurrency request scheduling/queuing problem**.

**6. The 1-worker test has a particularly bad tail**

The 1-worker latency distribution is roughly:

p50 3.83 s

p95 6.58 s

p99 7.03 s

max 7.11 s

That means the majority of requests are already slow, and virtually the
entire tail approaches 7 seconds.

The raw report confirms the maximum request was about 7.11 seconds.

With 4 workers:

p50 2.36 s

p95 6.21 s

p99 6.50 s

max 6.65 s

So the **median improves dramatically**, but the tail barely moves.

This distinction is important.

**Four workers fixed:**

**general worker contention**

**Four workers did NOT fix:**

**the mechanism producing the multi-second tail**

**7. CPU utilization does not look saturated**

The machine reports:

CPU logical count = 8

For the 1-worker process, the snapshot reports about:

user CPU 4.5 s

system CPU 0.94 s

For the 4-worker snapshot:

user CPU 5.77 s

system CPU 1.38 s

This doesn\'t look like a CPU-bound SELECT 1 workload.

Also, memory is small:

- \~104 MB in the 1-worker process

- \~112 MB in the 4-worker snapshot

So I don\'t see evidence here that memory pressure or CPU exhaustion
explains the latency.

**8. The pool configuration is actually healthy**

Both snapshots end with:

pool_active_connections = 0

pool_idle_connections = 16

and the health endpoint reports a pool configured around:

min_connections = 4

max_connections = 16

current_connections = 16

This is important because your earlier concern was whether the
connection pool was responsible.

Based on **these measurements**, it isn\'t.

Increasing:

16 → 32 → 64

is unlikely to magically turn 2.7-second latency into 20-ms latency.

**9. There is also an instrumentation limitation**

Notice something unusual:

**1-worker report**

500 client requests

server request_count = 569

**4-worker report**

500 client requests

server request_count = 387

The performance snapshot therefore isn\'t capturing exactly the same set
of requests as the load-test measurement.

That\'s not necessarily a bug---the smoke tests and other requests are
also involved---but it means we **shouldn\'t try to mathematically
reconcile every server histogram directly with the 500 external
requests**.

More importantly, the instrumentation is currently insufficient to
explain the entire request latency.

**10. What I think is happening**

Based strictly on these results, the architecture is behaving
approximately like this:

500 concurrent clients

│

▼

Load generator

│

▼

HTTP connection /

request scheduling

│

│ ← large amount of unexplained latency

▼

Unicorn worker(s)

│

▼

application dispatch

│

├── authentication/cache

│

├── pool acquire \~0.08 ms

│

├── PostgreSQL \~2 ms

│

├── fetch \~3 ms

│

└── response

The measured database path is extremely fast.

The measured application dispatch is tens of milliseconds.

But the client sees:

2--7 seconds

Therefore, **the missing instrumentation is where we should concentrate
next.**

**11. Don\'t interpret this as \"4 workers solved performance\"**

I would rate the results like this:

**Reliability**

**Excellent for this test**

500/500 successful in both runs.\
0 failures.\
All smoke tests passed.

**Scalability**

**Improved**

4 workers:

**68.4 → 74.2 req/s**

But only \~8.5% improvement.

**Median latency**

**Significantly improved**

3.83s → 2.36s.

**Tail latency**

**Still poor**

p95:

6.58s → 6.21s

p99:

7.03s → 6.50s

**Database**

**Not the apparent bottleneck**

\~2 ms SQL execution.

**Connection pool**

**Not the apparent bottleneck**

\~0.08 ms acquisition.

**Overall API performance**

**Still not production-grade for this workload.**

**12. What I would do next**

I would **not make another random optimization**.

The next step should be instrumentation of the complete request
lifecycle.

For every /api/query request, record:

request_received

↓

auth_start

↓

auth_end

↓

cache_lookup_start

↓

cache_lookup_end

↓

pool_acquire_start

↓

pool_acquire_end

↓

sql_start

↓

sql_end

↓

fetch_start

↓

fetch_end

↓

serialization_start

↓

serialization_end

↓

response_start

↓

request_complete

Then calculate:

HTTP total latency

=

queue/wait

\+ authentication

\+ cache

\+ pool acquisition

\+ SQL

\+ fetch

\+ serialization

\+ response

Right now you are measuring some of these pieces, but **not enough of
them to explain the 2--7 second external latency**.

**My conclusion**

The results are actually useful because they narrow the problem
considerably.

**The good news:**

- PostgreSQL is responding quickly.

- The pool isn\'t waiting.

- No requests fail.

- Four workers improve median latency substantially.

- Memory isn\'t exploding.

- The application survives 500 concurrent requests.

**The bad news:**

- 500 concurrent requests still produce **2.36 s median latency with 4
  workers**.

- **95% of requests take up to \~6.2 s**.

- **99% take up to \~6.5 s**.

- Only \~74 req/s is achieved.

- The server-side SQL/pool timings cannot explain the client-observed
  latency.

So I would **stop optimizing PostgreSQL and the connection pool for the
moment**.

The next target should be the **HTTP → Unicorn → async/thread
execution/request scheduling path**, with instrumentation that
identifies exactly where those missing seconds are being spent.

And the 4-worker result is a strong clue: **worker-level concurrency
matters, but worker count alone is not the fundamental solution.**
