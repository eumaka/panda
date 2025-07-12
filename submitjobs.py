#!/usr/bin/env python3

import os
import ssl
import json
import time
import uuid
import stomp
from pandaclient import PrunScript, panda_api

# === Configuration ===
host = 'pandaserver02.sdcc.bnl.gov'
port = 61612
cafile = '/eic/u/ejikutu/.globus/full-chain.pem'

panda_auth_vo = os.getenv('PANDA_AUTH_VO')
if not panda_auth_vo:
    print("Error: PANDA_AUTH_VO environment variable is not set.")
    print("Please set it, e.g., 'export PANDA_AUTH_VO=wlcg'")
    exit(1)

mq_user = os.getenv('MQ_USER')
mq_passwd = os.getenv('MQ_PASSWD')

# === Job Submission Function ===
def submit_job(job):
    print(f"[INFO] Submitting job: {job}")
    generated_uuid = str(uuid.uuid4())
    out_ds_name = f"user.eumaka.{generated_uuid}"

    prun_args = [
        "--exec", job.get("exec", "./my_script.sh"),
        "--outDS", out_ds_name,
        "--nJobs", str(job.get("nJobs", 1)),
        "--vo", job.get("vo", panda_auth_vo),
        "--site", job.get("site", "BNL_PanDA_1"),
        "--prodSourceLabel", "test",
        "--workingGroup", panda_auth_vo,
        "--noBuild",
        "--outputs", job.get("outputs", "myout.txt")
    ]

    try:
        params = PrunScript.main(True, prun_args)
        c = panda_api.get_api()
        status, result = c.submit_task(params)

        if status == 0:
            task_id = result[2]
            panda_monitor_url = os.getenv('PANDAMON_URL')
            print(f"[✔] Submitted PanDA task {task_id}")
            print(f"Monitor: {panda_monitor_url}/task/{task_id}/")
        else:
            print(f"[✖] Submission failed: {result}")
    except Exception as e:
        print(f"[ERROR] Exception during submission: {e}")

# === STOMP Listener ===
class JobListener(stomp.ConnectionListener):
    def on_message(self, frame):
        print(f"[INFO] Received message: {frame.body}")
        try:
            job = json.loads(frame.body)
            submit_job(job)
        except Exception as e:
            print(f"[ERROR] Failed to parse/submit job: {e}")

# === Connect to ActiveMQ ===
conn = stomp.Connection(
    host_and_ports=[(host, port)],
    vhost=host,
    try_loopback_connect=False,
    heartbeats=(10000, 10000)
)

conn.transport.set_ssl(
    for_hosts=[(host, port)],
    ca_certs=cafile,
    ssl_version=ssl.PROTOCOL_TLS_CLIENT
)

conn.set_listener('', JobListener())

client_id = 'test-client-new'
subscription_name = 'test-durable-sub-new'

conn.connect(
    login=mq_user,
    passcode=mq_passwd,
    wait=True,
    version='1.2',
    headers={'client-id': client_id}
)

conn.subscribe(
    destination='epictopic',
    id='sub-test-001-new',
    ack='auto',
    headers={'activemq.subscriptionName': subscription_name}
)

print(f"[INFO] Listening on '/topic/epictopic' as {client_id}.{subscription_name}...")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Exiting...")
    conn.disconnect()
