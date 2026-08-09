unicorn run_api:app --workers 1

(salesdata-api) hmudassi@hmudassi:\~/py/salesdata-api\$ python
load_test.py --dashboard-api-key
QtDiHnBSX3vgzHkMylyHEUl_4A9j0M92cX6TR3pr4gI Registering/logging
in/minting API key... Using API key:
ps8y-\_jIBMO6jdU32OKrc5KabO7vCQ7nJjzcp4jlj_0

============================================================ Phase 1:
functional smoke tests
============================================================ \[PASS\]
GET /api/health \[PASS\] POST /api/query (SELECT 1) \[PASS\] POST
/api/query (repeat -\> cached) \[PASS\] GET /api/tables \[PASS\] GET
/api/auth/keys/{owner_id} \[PASS\] 5x rapid requests, same key
(validation cache) \[PASS\] POST /api/query (bad key -\> 401) \[PASS\]
POST /api/query (no key -\> 401)
------------------------------------------------------------ 8/8 checks
passed ============================================================

Firing 500 concurrent requests: 'SELECT 1 AS ok'

============================================================ Phase 2:
load test --- 500 requests, concurrency=500
============================================================ Wall time:
7.31s Throughput: 68.4 req/s Succeeded (200): 500 Failed: 0
------------------------------------------------------------ Latency
min/avg/max: 350.4 / 3646.1 / 7110.1 ms Latency p50/p95/p99: 3825.1 /
6577.7 / 7029.0 ms
============================================================

Fetching server-side /debug/performance snapshot... Saved full report
to:
/home/hmudassi/py/salesdata-api/load_test_report_20260809T134215Z.json

unicorn run_api:app --workers 4

(salesdata-api) hmudassi@hmudassi:\~/py/salesdata-api\$ python
load_test.py --dashboard-api-key
QtDiHnBSX3vgzHkMylyHEUl_4A9j0M92cX6TR3pr4gI Registering/logging
in/minting API key... Using API key:
KWEsUmbdsWJuEyltQ8AM_3YKc4S9-CYkbEt60Ez9LEs

============================================================ Phase 1:
functional smoke tests
============================================================ \[PASS\]
GET /api/health \[PASS\] POST /api/query (SELECT 1) \[PASS\] POST
/api/query (repeat -\> cached) \[PASS\] GET /api/tables \[PASS\] GET
/api/auth/keys/{owner_id} \[PASS\] 5x rapid requests, same key
(validation cache) \[PASS\] POST /api/query (bad key -\> 401) \[PASS\]
POST /api/query (no key -\> 401)
------------------------------------------------------------ 8/8 checks
passed ============================================================

Firing 500 concurrent requests: 'SELECT 1 AS ok'

============================================================ Phase 2:
load test --- 500 requests, concurrency=500
============================================================ Wall time:
6.73s Throughput: 74.2 req/s Succeeded (200): 500 Failed: 0
------------------------------------------------------------ Latency
min/avg/max: 366.6 / 2732.3 / 6645.9 ms Latency p50/p95/p99: 2360.7 /
6212.7 / 6498.4 ms
============================================================

Fetching server-side /debug/performance snapshot... Saved full report
to:
/home/hmudassi/py/salesdata-api/load_test_report_20260809T134257Z.json
