#!/usr/bin/env python3
"""Execute the built image's API and workers against the local task-owned services."""
import json
from pathlib import Path
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
NETWORK = 'clearinghouse-local'
RUN = f'container_{time.time_ns()}'
CONTAINERS = []

def docker(*args, check=True):
    result = subprocess.run(['docker', *args], text=True, capture_output=True, check=check)
    return result.stdout.strip()

def main():
    docker('network', 'create', NETWORK, check=False)
    for service in ('clearinghouse-pg', 'clearinghouse-nats'):
        networks = json.loads(docker('inspect', '--format', '{{json .NetworkSettings.Networks}}', service))
        if NETWORK not in networks: docker('network', 'connect', NETWORK, service)
    settings = ['--network', NETWORK, '-e', 'DATABASE_URL=postgres://postgres:localdemo@clearinghouse-pg:5432/clearinghouse?sslmode=disable',
                '-e', f'PGSCHEMA={RUN}', '-e', f'NATS_STREAM={RUN}', '-e', 'NATS_URL=nats://clearinghouse-nats:4222',
                '-e', 'API_TOKENS={"local-demo-alpha-token":"demo"}', '--read-only', '--cap-drop', 'ALL',
                '--security-opt', 'no-new-privileges', '--memory', '128m', '--cpus', '1']
    for mode in ('init','seed'): docker('run', '--rm', *settings, 'clearinghouse-local', '-mode', mode)
    for mode in ('api','publisher','consumer'):
        name=f'{RUN}-{mode}'
        args=['run','-d','--name',name,*settings]
        if mode=='api':args+=['-p','127.0.0.1::8089']
        docker(*args,'clearinghouse-local','-mode',mode,'-addr','0.0.0.0:8089');CONTAINERS.append(name)
    base='http://'+docker('port',CONTAINERS[0],'8089/tcp')
    headers={'Authorization':'Bearer local-demo-alpha-token','Content-Type':'application/json','Idempotency-Key':'container-proof'}
    for _ in range(50):
        try:
            with urllib.request.urlopen(base+'/healthz',timeout=1) as response: assert response.status==200;break
        except OSError:time.sleep(.1)
    else:raise AssertionError('container API not ready')
    req=urllib.request.Request(base+'/v1/transfers',data=b'{"from":"a0","to":"a1","amount":11}',headers=headers)
    with urllib.request.urlopen(req,timeout=5) as response:transfer=json.load(response);assert response.status==201
    for _ in range(100):
        with urllib.request.urlopen(urllib.request.Request(base+'/v1/summary',headers=headers),timeout=2) as response: summary=json.load(response)
        if summary['pending']==0 and summary['transfers']==1:break
        time.sleep(.1)
    else:raise AssertionError('container workers did not materialize')
    assert summary['volume']==11 and summary['currency']=='USD'
    config=json.loads(docker('inspect',CONTAINERS[0]))[0]
    result={'run_id':RUN,'image_id':config['Image'],'runtime_user':config['Config']['User'],
            'read_only_root':config['HostConfig']['ReadonlyRootfs'],'memory_limit_bytes':config['HostConfig']['Memory'],
            'api_status':201,'transfer':transfer,'materialized':summary,'services':'3 Linux containers + PostgreSQL + NATS',
            'scope':'local Colima Docker VM, not a cloud deployment'}
    (ROOT/'evidence/docker-smoke.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))

if __name__=='__main__':
    try: main()
    finally:
        for name in CONTAINERS:docker('rm','-f',name,check=False)
