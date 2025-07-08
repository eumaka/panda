#!/usr/bin/env python3

import os, sys, json, uuid, argparse
import datetime
from sys import exit

# --- Argument parsing ---
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument("-v", "--verbose",      action='store_true', help="Verbose mode")
parser.add_argument("-m", "--mq",           action='store_true', help="Enable MQ send/receive", default=False)
parser.add_argument("-s", "--schedule",     type=str,            help="Path to the schedule (YAML)", default='')
parser.add_argument("-f", "--factor",       type=float,          help="Time factor", default=1.0)
parser.add_argument("-u", "--until",        type=float,          help="Time limit", default=None)
parser.add_argument("-c", "--clock",        type=float,          help="Scheduler clock freq (sec)", default=1.0)
parser.add_argument("-d", "--dest",         type=str,            help="Output destination folder", default='')
parser.add_argument("-L", "--low",          type=float,          help="Low STF production time limit", default=1.0)
parser.add_argument("-H", "--high",         type=float,          help="High STF production time limit", default=2.0)

parser.add_argument("--submit-panda",       action='store_true', help="Submit STF jobs to PanDA when receiving MQ messages", default=False)

args        = parser.parse_args()
verbose     = args.verbose
mq          = args.mq
submit_panda= args.submit_panda
schedule    = args.schedule
dest        = args.dest
factor      = args.factor
until       = args.until
clock       = args.clock
low         = args.low
high        = args.high

if verbose:
    print(f"*** Verbose mode: {verbose} ***")
    print(f"*** MQ enabled: {mq} ***")
    print(f"*** PanDA submission: {submit_panda} ***")

# --- Counters ---
panda_success_count = 0
panda_fail_count = 0

# --- Set DAQSIM_PATH ---
daqsim_path = ''
try:
    daqsim_path = os.environ['DAQSIM_PATH']
    if verbose: print(f"*** DAQSIM_PATH is set: {daqsim_path} ***")
    sys.path.append(daqsim_path)
except:
    if verbose: print("*** DAQSIM_PATH is undefined, using ../ ***")
    daqsim_path = '../'
    sys.path.append(daqsim_path)

if schedule == '':
    schedule = os.path.join(daqsim_path, "config", "schedule-rt.yml")

if verbose:
    print(f"*** sys.path: {sys.path} ***")
    print(f"*** Schedule file: {schedule} ***")
    print(f"*** Output destination: {dest if dest else 'None'} ***")
    print(f"*** Time factor: {factor} ***")

# --- Import DAQ ---
try:
    from daq import *
    if verbose: print("*** Loaded DAQ package ***")
except:
    print("*** Failed to import DAQ package, exiting ***")
    exit(-1)

# --- PanDA submission helper ---
def submit_panda_job(stf_json):
    global panda_success_count, panda_fail_count
    from pandaclient import PrunScript, panda_api

    unique_id = str(uuid.uuid4())
    filename = stf_json.get("filename", "unknown")

    out_ds_name = f"user.eumaka.{filename}.{unique_id}"
    unique_taskname = f"stf_task_{filename}_{unique_id}"

    exec_str = f"./my_script_new.sh '{json.dumps(stf_json)}'"

    PANDA_AUTH_VO = os.getenv('PANDA_AUTH_VO')
    if not PANDA_AUTH_VO:
        print("Error: PANDA_AUTH_VO is not set.")
        panda_fail_count += 1
        return

    prun_args = [
        "--exec", exec_str,
        "--outDS", out_ds_name,
        "--nJobs", "1",
        "--vo", "wlcg",
        "--site", "BNL_PanDA_1",
        "--prodSourceLabel", "managed",
        "--workingGroup", PANDA_AUTH_VO,
        "--noBuild",
        "--outputs", "myout.txt"
    ]

    if verbose:
        print(f"[DEBUG] Using exec: {exec_str}")
        print(f"[DEBUG] Running prun from current directory: {os.getcwd()}")

    print(f"[INFO] Starting PanDA submission for STF: {filename}")
    try:
        params = PrunScript.main(True, prun_args)
        params['taskName'] = unique_taskname

        if verbose:
            print(f"[DEBUG] Submission params:\n{json.dumps(params, indent=2)}")

        c = panda_api.get_api()
        status, result_tuple = c.submit_task(params)

        if status == 0:
            jedi_task_id = result_tuple[2]
            monitor_url = os.getenv('PANDAMON_URL', 'https://pandamon01.sdcc.bnl.gov')
            print(f"✅ Submitted! JediTaskID: {jedi_task_id}")
            print(f"🔗 {monitor_url}/task/{jedi_task_id}/")
            panda_success_count += 1
        else:
            print(f"❌ Submission failed. Status: {status}, Message: {result_tuple}")
            panda_fail_count += 1

    except Exception as e:
        print(f"[ERROR] Exception during PanDA submission: {e}")
        panda_fail_count += 1


# --- DAQ ---
sndr = None
rcvr = None

if mq:
    try:
        from comms import Sender, Receiver
        if verbose: print("*** Imported Sender and Receiver ***")
    except:
        print("*** Failed to import Sender/Receiver, exiting ***")
        exit(-1)

    try:
        sndr = Sender(verbose=verbose)
        sndr.connect()
        if verbose: print("*** Sender connected ***")
    except:
        print("*** Failed to instantiate Sender, exiting ***")
        exit(-1)

    def process_received_message(msg):
        print(f"[MQ] Received message:\n{msg}")
        if submit_panda:
            try:
                stf_json = json.loads(msg)
                submit_panda_job(stf_json)
            except Exception as e:
                print(f"[ERROR] Failed to submit PanDA job: {e}")

    try:
        rcvr = Receiver(verbose=verbose, processor=process_received_message)
        rcvr.connect()
        if verbose: print("*** Receiver connected ***")
    except:
        print("*** Failed to instantiate Receiver, exiting ***")
        exit(-1)

daq = DAQ(
    schedule_f  = schedule,
    destination = dest,
    until       = until,
    clock       = clock,
    factor      = factor,
    low         = low,
    high        = high,
    verbose     = verbose,
    sender      = sndr
)

daq.simulate()

print("---")
if verbose:
    print(f"*** Completed at {daq.get_time()} | STFs generated: {daq.Nstf} ***")
    if submit_panda:
        print(f"*** PanDA jobs submitted successfully: {panda_success_count} ***")
        print(f"*** PanDA jobs failed: {panda_fail_count} ***")
    print("*** Disconnecting MQ ***")

if mq:
    if sndr: sndr.disconnect()
    if rcvr: rcvr.disconnect()
print("---")

