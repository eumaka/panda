#!/usr/bin/env python3

import json
import ssl
import stomp
import os

# === Configuration ===
host = 'pandaserver02.sdcc.bnl.gov'
port = 61612
cafile = '/eic/u/ejikutu/.globus/full-chain.pem'

username = os.getenv('MQ_USER')
password = os.getenv('MQ_PASSWD')

destination = 'epictopic'

# === Job Message ===
job_message = {
    "exec": "./my_script.sh",
    "outputs": "myout.txt",
    "nJobs": 1,
    "vo": "wlcg",
    "site": "BNL_PanDA_1"
}

# === Send Message ===
conn = stomp.Connection(
    host_and_ports=[(host, port)],
    vhost=host,
    try_loopback_connect=False
)

conn.transport.set_ssl(
    for_hosts=[(host, port)],
    ca_certs=cafile,
    ssl_version=ssl.PROTOCOL_TLS_CLIENT
)

conn.connect(login=username, passcode=password, wait=True)
conn.send(destination=destination, body=json.dumps(job_message))
print(f"[✔] Sent job message to {destination}:\n{json.dumps(job_message, indent=2)}")
conn.disconnect()
