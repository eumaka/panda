#!/usr/bin/env python3

import os
import sys
import json
import time
import uuid
import ssl
import stomp
from pandaclient import PrunScript, panda_api

# === ActiveMQ settings ===
MQ_HOST = os.getenv('MQ_HOST', 'pandaserver02.sdcc.bnl.gov')
MQ_PORT = int(os.getenv('MQ_PORT', 61612))
MQ_USER = os.getenv('MQ_USER')
MQ_PASSWD = os.getenv('MQ_PASSWD')
MQ_CAFILE = os.getenv('MQ_CAFILE', os.path.expanduser('~/.globus/full-chain.pem'))

DESTINATION = 'epictopic'

if not MQ_USER or not MQ_PASSWD:
    print("Error: MQ_USER and MQ_PASSWD must be set in the environment.")
    sys.exit(1)

if not os.path.exists(MQ_CAFILE):
    print(f"Error: MQ_CAFILE {MQ_CAFILE} does not exist.")
    sys.exit(1)

# === PanDA VO ===
try:
    PANDA_AUTH_VO = os.environ['PANDA_AUTH_VO']
except KeyError:
    print("Error: PANDA_AUTH_VO is not set. Please set it (e.g., export PANDA_AUTH_VO=wlcg).")
    sys.exit(1)


def submit_panda_job(stf_json):
    """
    Submit one PanDA job, passing the STF JSON as argument to my_script_new.sh
    """
    unique_id = str(uuid.uuid4())
    filename = stf_json.get("filename", "unknown")
    out_ds_name = f"user.eumaka.{filename}.{unique_id}"

    exec_str = f"./my_script_new.sh '{json.dumps(stf_json)}'"

    prun_args = [
        "--exec", exec_str,
        "--outDS", out_ds_name,
        "--nJobs", "1",
        "--vo", "wlcg",
        "--site", "BNL_PanDA_1",
        "--prodSourceLabel", "test",
        "--workingGroup", PANDA_AUTH_VO,
        "--noBuild",
        "--outputs", "myout.txt"
    ]

    params = PrunScript.main(True, prun_args)

    # Use filename + uuid as taskName
    params['taskName'] = f"stf_task_{filename}_{unique_id}"

    c = panda_api.get_api()

    print(f"[INFO] Submitting PanDA job for STF {filename} …")
    status, result_tuple = c.submit_task(params)

    if status == 0:
        jedi_task_id = result_tuple[2]
        monitor_url = os.getenv('PANDAMON_URL', 'https://pandamon01.sdcc.bnl.gov')
        print(f"✅ Submitted! JediTaskID: {jedi_task_id}")
        print(f"🔗 {monitor_url}/task/{jedi_task_id}/")
    else:
        print(f"❌ Task submission failed. Status: {status}, Message: {result_tuple}")


class STFListener(stomp.ConnectionListener):
    def on_error(self, frame):
        print(f"[ERROR] {frame.body}")

    def on_message(self, frame):
        body = frame.body.strip()
        print(f"\n📨 Received message:\n{body}\n")

        try:
            stf_json = json.loads(body)
            submit_panda_job(stf_json)
        except Exception as e:
            print(f"[ERROR] Failed to process STF: {e}")


def main():
    conn = stomp.Connection(
        host_and_ports=[(MQ_HOST, MQ_PORT)],
        vhost=MQ_HOST,
        try_loopback_connect=False,
        heartbeats=(10000, 10000)
    )

    conn.transport.set_ssl(
        for_hosts=[(MQ_HOST, MQ_PORT)],
        ca_certs=MQ_CAFILE,
        ssl_version=ssl.PROTOCOL_TLS_CLIENT
    )

    listener = STFListener()
    conn.set_listener('', listener)

    client_id = 'test-client'
    subscription_name = 'test-durable-sub'

    conn.connect(
        login=MQ_USER,
        passcode=MQ_PASSWD,
        wait=True,
        version='1.2',
        headers={'client-id': client_id}
    )

    conn.subscribe(
        destination=DESTINATION,
        id='sub-test-001',
        ack='auto',
        headers={'activemq.subscriptionName': subscription_name}
    )

    print(f"[INFO] Listening on '{DESTINATION}' as {client_id}.{subscription_name} …")
    print("[CTRL+C to exit]")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nExiting…")
        conn.disconnect()


if __name__ == "__main__":
    main()
