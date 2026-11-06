#!/usr/bin/env python3
"""Real HTTP/PG/NATS experiment. Only controls clearinghouse-owned processes/containers."""
import concurrent.futures as cf
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import signal
import socket
import statistics
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / '.runtime/clearinghouse'
TOKEN = 'local-demo-alpha-token'
PROCESSES = []
LOGS = []
ENV = os.environ.copy()
ENV.setdefault('DATABASE_URL', 'postgres://postgres:localdemo@127.0.0.1:55432/clearinghouse?sslmode=disable')
ENV.setdefault('NATS_URL', 'nats://127.0.0.1:54222')
RUN = f'demo_{time.time_ns()}'
ENV['PGSCHEMA'] = RUN
ENV['NATS_STREAM'] = RUN
ENV['API_TOKENS'] = json.dumps({TOKEN: 'demo', 'local-demo-beta-token': 'other'})

def command(*args):
    return subprocess.run([str(BINARY), *args], env=ENV, check=True, capture_output=True, text=True).stdout

def start(mode, port=None):
    log = open(ROOT / 'evidence' / f'{RUN}-{mode}-{len(PROCESSES)}.log', 'w')
    LOGS.append(log)
    args = [str(BINARY), '-mode', mode]
    if port: args += ['-addr', f'127.0.0.1:{port}']
    p = subprocess.Popen(args, env=ENV, stdout=log, stderr=log)
    PROCESSES.append(p)
    return p

def freeport():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

PORTS = [freeport(), freeport()]

def request(path, body=None, key=None, token=TOKEN, port=None):
    headers = {'Authorization': f'Bearer {token}'}
    if key: headers['Idempotency-Key'] = key
    data = None if body is None else json.dumps(body).encode()
    if data: headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(f'http://127.0.0.1:{port or PORTS[0]}{path}', data=data, headers=headers)
    begin = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=12) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read()
    return status, json.loads(raw), (time.perf_counter()-begin)*1000

def until(check, seconds=20):
    deadline = time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:
            value = check()
            if value: return value
        except (OSError, urllib.error.URLError): pass
        time.sleep(.1)
    raise AssertionError('condition not reached before deadline')

def transfer(i, prefix, amount=1, spread=False):
    source = i % 16 if spread else 0
    dest = source+16 if spread else 1
    begin = time.perf_counter()
    for attempt in range(5):
        result = request('/v1/transfers', {'from': f'a{source}', 'to': f'a{dest}', 'amount': amount}, f'{prefix}-{i}', port=PORTS[i%2])
        if result[0] != 503:
            return (*result[:2], (time.perf_counter()-begin)*1000, attempt)
        time.sleep(.03*(attempt+1) + random.random()*.03)
    return (*result[:2], (time.perf_counter()-begin)*1000, 4)

def load(prefix, n=600, spread=False):
    begin = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=24) as pool:
        results = list(pool.map(lambda i: transfer(i, prefix, spread=spread), range(n)))
    elapsed = time.perf_counter()-begin
    assert all(r[0] == 201 for r in results), [r for r in results if r[0] != 201][:5]
    durations = sorted(r[2] for r in results)
    return {'requests': n, 'concurrency': 24, 'api_processes': 2, 'seconds': elapsed,
            'requests_per_second': n/elapsed, 'p50_ms': statistics.median(durations),
            'p95_ms': durations[int(.95*(n-1))], 'p99_ms': durations[int(.99*(n-1))],
            'final_errors': 0, 'transient_503_retries': sum(r[3] for r in results),
            'latency_includes_retries': True}

def main():
    command('-mode', 'init')
    command('-mode', 'seed', '-tenant', 'demo')
    command('-mode', 'seed', '-tenant', 'other')
    api = [start('api', port) for port in PORTS]
    for port in PORTS: until(lambda: request('/healthz', port=port)[0] == 200)
    publisher = start('publisher')
    consumer = start('consumer')
    assert request('/v1/accounts/a0', token='wrong')[0] == 401
    with cf.ThreadPoolExecutor(max_workers=24) as pool:
        repeated = list(pool.map(lambda i: request('/v1/transfers', {'from':'a0','to':'a1','amount':7}, 'duplicate', port=PORTS[i%2]), range(48)))
    assert all(r[0] in (200,201) for r in repeated)
    assert len({r[1]['id'] for r in repeated}) == 1
    assert sum(r[0] == 201 for r in repeated) == 1
    assert request('/v1/transfers', {'from':'a0','to':'a1','amount':8}, 'duplicate')[0] == 409
    assert request('/v1/accounts/a0', token='local-demo-beta-token')[1]['balance'] == 1000000
    hot = load('hot')
    spread = load('spread', spread=True)
    # Immutable command survives an abruptly killed API process; retry returns same committed ID.
    before = request('/v1/transfers', {'from':'a0','to':'a1','amount':11}, 'crash-retry')
    api[0].kill(); api[0].wait(timeout=5)
    api[0] = start('api', PORTS[0])
    until(lambda: request('/healthz')[0] == 200)
    after = request('/v1/transfers', {'from':'a0','to':'a1','amount':11}, 'crash-retry')
    assert after[0] == 200 and before[1]['id'] == after[1]['id']
    # Consumer is a separately killed process. Writes and publishing must continue.
    consumer.kill(); consumer.wait(timeout=5)
    for i in range(50): assert transfer(i, 'consumer-outage')[0] == 201
    pending = request('/v1/summary')[1]['pending']
    assert pending >= 50
    consumer = start('consumer')
    until(lambda: request('/v1/summary')[1]['pending'] == 0)
    # Optional broker outage affects only these named task-owned containers.
    broker_test = {'executed': False}
    if os.environ.get('DEMO_BROKER_OUTAGE') == '1':
        subprocess.run(['docker','stop','-t','1','clearinghouse-nats'], check=True, capture_output=True)
        try:
            for i in range(50): assert transfer(i,'broker-outage')[0] == 201
            broker_pending = request('/v1/summary')[1]['pending']
            assert broker_pending >= 50
        finally:
            subprocess.run(['docker','start','clearinghouse-nats'], check=True, capture_output=True)
        until(lambda: request('/v1/summary')[1]['pending'] == 0, seconds=30)
        broker_test = {'executed': True, 'accepted_while_broker_down': 50, 'pending_during_outage':broker_pending, 'pending_after_recovery':0}
    # A high-contention burst overdraws neither account and rejects excess spends.
    with cf.ThreadPoolExecutor(max_workers=24) as pool:
        spends = list(pool.map(lambda i: request('/v1/transfers', {'from':'a31','to':'a30','amount':100000},f'spend-{i}',port=PORTS[i%2]),range(40)))
    assert sum(r[0] == 201 for r in spends) == 10
    assert sum(r[0] == 409 for r in spends) == 30
    until(lambda: request('/v1/summary')[1]['pending'] == 0)
    reconcile = request('/v1/reconcile')[1]
    assert reconcile['mismatches'] == 0 and reconcile['journal_net'] == 0 and reconcile['balance_total'] == 32000000
    summary = request('/v1/summary')[1]
    assert summary['transfers'] == reconcile['transfers']
    result = {'run_id':RUN,'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),'platform':platform.platform(),
              'binary_sha256': hashlib.sha256(BINARY.read_bytes()).hexdigest(),
              'go_version': subprocess.check_output(['go','version'],text=True).strip(),
              'source_sha256': {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted([*ROOT.glob('*.go'),*ROOT.glob('cmd/**/*.go'),ROOT/'schema.sql',ROOT/'go.mod',ROOT/'go.sum',Path(__file__).resolve()])},
              'cpu_count':os.cpu_count(), 'database':'PostgreSQL 18 (local Docker VM)', 'broker':'NATS 2.12 JetStream (local Docker VM)',
              'hot_accounts':hot,'spread_accounts':spread,'duplicate_requests':48,'duplicate_effects':1,
              'consumer_outage_accepted':50,'consumer_outage_pending':pending,'broker_outage':broker_test,
              'crash_retry_same_id':True,'overspend_success':10,'overspend_rejected':30,'reconciliation':reconcile,'summary':summary,
              'limits':'Local host end-to-end HTTP measurements include Python client and VM overhead; not cloud capacity or an SLA.'}
    (ROOT/'evidence/demo.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))

if __name__ == '__main__':
    try: main()
    finally:
        for p in PROCESSES:
            if p.poll() is None: p.terminate()
        for p in PROCESSES:
            try: p.wait(timeout=8)
            except subprocess.TimeoutExpired: p.kill(); p.wait()
        for log in LOGS: log.close()
