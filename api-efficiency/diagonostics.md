I traced the uploaded salesinfo-apiV2-main.zip against both
500-concurrency reports. The important conclusion is now more precise:

**The bottleneck is not PostgreSQL --- and I found a concrete code-level
reason to investigate**

The test itself is healthy: 500/500 requests return 200, while 4 workers
improve throughput from 68.4 to 74.2 req/s and p50 from 3.83 s to 2.36
s.

The server measurements are dramatically smaller:

  --------------------------------------
  **Stage**              **1         **4
                    worker**   workers**
  --------------- ---------- -----------
  Server dispatch    28.6 ms     18.0 ms
  p50                        

  Server dispatch   223.3 ms     55.0 ms
  p95                        

  PostgreSQL         1.89 ms     2.11 ms
  execute                    

  Pool acquire      0.080 ms    0.084 ms

  Pool release      0.024 ms    0.023 ms

  Client p50          **3825 **2361 ms**
                        ms** 

  Client p95          **6578 **6213 ms**
                        ms** 
  --------------------------------------

Those numbers are present in the reports and prior analysis.

**1. There is a very important architectural clue in the code**

Your /api/query route is actually quite thin:

/api/query

↓

CurrentUser dependency

↓

QueryService.run()

↓

cache

↓

query concurrency semaphore

↓

DB pool

↓

PostgreSQL

The route creates QueryService and calls service.run().

The database execution itself is correctly moved off the event loop into
a **dedicated DB executor**. AsyncSessionWrapper.fetch_all() ultimately
calls run_in_db_executor(), rather than blocking the event loop. That
part of the architecture is sound.

**2. But authentication has a concurrency-amplification problem**

This is the first code issue I would fix.

Every protected request goes through:

api_key_service = APIKeyService(service_manager)

validated_key = await api_key_service.validate_api_key(raw_api_key)

APIKeyService has a 30-second per-process validation cache.

But the cache is **not single-flight protected**.

So when 500 requests arrive simultaneously with the same API key:

request 1 ── cache miss ── PostgreSQL

request 2 ── cache miss ── PostgreSQL

request 3 ── cache miss ── PostgreSQL

\...

request 500 ─ cache miss ─ PostgreSQL

because all 500 can observe the cache as empty before the first lookup
completes.

The code explicitly says the validation cache is a simple dictionary and
does not synchronize concurrent misses.

That means your optimization handles:

repeated sequential requests

but not:

**a burst of simultaneous requests using the same key.**

That distinction matters enormously for this exact benchmark.

**3. The same problem exists for the user lookup**

After API-key validation, the middleware does:

user = \_get_cached_user(owner_id)

if user is None:

user = await run_in_service_executor(

service_manager.users.get_by_id, owner_id

)

\...

The user cache is also populated only after the DB call completes.

So a simultaneous burst can produce another \"thundering herd\":

500 requests

│

├── API-key cache miss

│ └── many service DB validations

│

└── user cache miss

└── many service DB user lookups

This is particularly relevant because your architecture uses a separate
bounded **service executor** specifically for
authentication/service-database work.

The executor code confirms that service DB work is deliberately isolated
into its own thread pool.

**4. Why 4 workers help**

This now makes the worker comparison more meaningful.

With one worker, one process has to absorb the entire burst.

With four workers:

500 requests

│

├── Worker 1

├── Worker 2

├── Worker 3

└── Worker 4

each process has its own:

- API-key cache

- user cache

- service executor

- DB pool

- event loop

So adding workers distributes the burst and reduces contention.

That matches your actual result:

**68.4 → 74.2 req/s**

and:

**3.83 s → 2.36 s p50**

while the database timings remain essentially unchanged.

**But there is an even more important finding**

I don\'t think the authentication cache stampede alone explains the
entire 6-second tail.

The evidence against that is the server-side profiler.

For the 4-worker run, server dispatch p95 is only about **55 ms**, while
the client p95 is **6.21 seconds**.

And the server snapshot contains completed /api/query requests around
only tens of milliseconds.

So there are **two separate problems**:

**Problem A --- application concurrency amplification**

This is visible in the code:

**API-key validation + user lookup are not single-flight.**

We should fix this.

**Problem B --- the load-test latency includes time that your
application profiler doesn\'t see**

This is the larger mystery.

The current load tester starts its stopwatch **before client.post()**:

start = time.perf_counter()

resp = await client.post(\...)

elapsed_ms = (time.perf_counter() - start) \* 1000

So its number includes:

client coroutine scheduling

\+

HTTP connection acquisition

\+

TCP/socket handling

\+

request transmission

\+

server accept/read

\+

application

\+

response transmission

\+

client response handling

Your server dispatch timer only measures the application-side
call_next() section.

Therefore:

**\"client latency = server application latency\" is not true for this
benchmark.**

This is exactly why the existing report correctly says the missing
latency is somewhere outside the currently measured application stages.

**I also found something important in load_test.py**

The load tester is already configured with:

max_connections = concurrency + 50

max_keepalive_connections = concurrency

for a concurrency of 500.

So the obvious httpx connection-pool limit of 100 is **not** the
explanation.

The test deliberately raises it to 550 connections.

That eliminates one common false diagnosis.

**The next instrumentation should therefore be different**

Your existing instrumentation is good **inside the application**, but it
starts too late to answer the most important question.

I would add these measurements to the load test:

TASK_CREATED

↓

CLIENT_REQUEST_START

↓

HTTP_CONNECTION_ACQUIRE

↓

REQUEST_HEADERS_SENT

↓

SERVER_RECEIVED

↓

SERVER_APP_START

↓

SERVER_APP_END

↓

RESPONSE_RECEIVED

↓

CLIENT_REQUEST_END

Then calculate:

client_total

client_connection_wait

client_network_wait

server_queue

server_application

response_transfer

The critical equation becomes:

client_total

=

connection_wait

\+ network/request wait

\+ server queue

\+ application

\+ response wait

Right now you only have:

client_total

and approximately:

server_application

That\'s why we can\'t account for the missing seconds yet.

**I would make these changes in this order**

**Phase 1 --- Fix authentication thundering herd**

Change:

validation cache

from:

dict

to:

cache + in-flight Future

Conceptually:

first request

↓

cache miss

↓

becomes leader

↓

one PostgreSQL lookup

↓

populate cache

↓

release all followers

Instead of:

500 requests

↓

500 PostgreSQL validations

you get:

500 requests

↓

1 PostgreSQL validation

↓

499 await same Future

Do the same for the user cache.

This is the highest-value code change I see in the authentication path.

**Phase 2 --- Instrument the service executor**

Your executor already has:

active

approx_queue_depth

max_workers

but the current load report doesn\'t capture the executor state **during
the burst**.

We need:

service_executor_queue_wait

service_executor_execution_time

service_executor_active

service_executor_queue_depth

especially for:

api_keys.validate

users.get_by_id

That will prove or disprove the auth-stampede hypothesis quantitatively.

**Phase 3 --- Instrument HTTP boundary**

Add server-side timestamps immediately at the ASGI boundary, before
middleware:

ASGI receive

↓

auth middleware

↓

performance middleware

↓

FastAPI route

The objective is to distinguish:

waiting before ASGI

from:

waiting inside application

because your current dispatch measurement starts too far downstream for
this particular problem.

**Phase 4 --- Re-run controlled tests**

Don\'t immediately run 500 again.

Run:

10 concurrent

25 concurrent

50 concurrent

100 concurrent

250 concurrent

500 concurrent

with:

1 worker

2 workers

4 workers

and record:

  -----------------------------------------------------------------------------------------------------
    **Concurrency**   **Workers**   **p50**   **p95**   **p99**   **throughput**   **service     **ASGI
                                                                                     queue**    queue**
  ----------------- ------------- --------- --------- --------- ---------------- ----------- ----------
                 10             1                                                            

                 25             1                                                            

                 50             1                                                            

                100             1                                                            

                250             1                                                            

                500             1                                                            

                500             2                                                            

                500             4                                                            
  -----------------------------------------------------------------------------------------------------

That curve will tell us exactly where saturation begins.

**What I would NOT change yet**

Based on the code and measurements, I would **not** currently:

- increase PostgreSQL from 16 → 32/64 connections;

- replace PostgreSQL;

- redesign QueryService;

- remove the async bridge;

- add more DB threads blindly;

- introduce Redis just for this benchmark;

- optimize SELECT 1;

- blame PostgreSQL.

Your current PostgreSQL path is already around **2 ms execution**, with
approximately **0.08 ms pool acquisition**, while the client sees
seconds.

The existing pool snapshot also shows 16 idle connections, so there is
no evidence of pool exhaustion in these runs.

**Bottom line**

I would now revise the earlier diagnosis from simply:

\"HTTP → Unicorn scheduling is slow\"

to:

**There are two layers of contention: a code-level burst amplification
in authentication/service-database lookups, plus a large amount of
latency occurring outside the currently measured application dispatch
interval.**

The **first one is directly visible in the code and should be fixed
now**. The **second one requires ASGI/HTTP-boundary instrumentation
before we can honestly identify its exact source**.

And your existing results.md reaches the same fundamental conclusion:
don\'t keep tuning PostgreSQL/pool; instrument the complete HTTP →
Unicorn → async/thread lifecycle next.
