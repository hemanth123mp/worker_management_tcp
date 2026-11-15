# tcp worker: register, heartbeat every 1s, run one task at a time, report done

import argparse
import json
import socket
import threading
import time

PORT = 5000


def send_json(conn, msg, slock):
    with slock:                # heartbeat thread and main thread both write here
        conn.sendall((json.dumps(msg) + "\n").encode())


def session(args, sim_fail):
    # one tcp session with the server. returns True if it ended in a simulated crash
    with socket.create_connection((args.server_ip, PORT), timeout=2) as conn:
        conn.settimeout(None)            # the 2s was only for connecting
        reader = conn.makefile("r")      # keep ONE reader, a second one would lose buffered lines
        slock = threading.Lock()
        stop, crashed = threading.Event(), threading.Event()
        got = 0                          # tasks received so far

        def crash():
            print(f"{args.worker_name}: simulated crash", flush=True)
            crashed.set()
            try:
                conn.shutdown(socket.SHUT_RDWR)   # shutdown not close, so the server sees the FIN now
            except OSError:
                pass

        send_json(conn, {"type": "register", "worker": args.worker_name, "speed": args.speed}, slock)
        print(json.loads(reader.readline()), flush=True)
        t0 = time.time()

        def beat():
            while not stop.is_set():
                if sim_fail and args.fail_after and time.time() - t0 >= args.fail_after:
                    crash()
                    return
                try:
                    send_json(conn, {"type": "heartbeat", "worker": args.worker_name}, slock)
                except OSError:
                    return
                stop.wait(1)

        threading.Thread(target=beat, daemon=True).start()
        try:
            for line in reader:
                msg = json.loads(line)
                if msg.get("type") != "task":
                    continue
                t = msg["task"]
                work = t["delay"] / args.speed * args.slowdown
                print(f"{args.worker_name}: running {t['task_id']} for {work:.1f}s", flush=True)
                got += 1
                if sim_fail and args.fail_on_task == got:
                    crashed.wait(work / 2)       # die half way through this task
                    crash()
                    break
                if crashed.wait(work):           # a sleep that a crash can interrupt
                    break
                send_json(conn, {"type": "done", "worker": args.worker_name,
                                 "task_id": t["task_id"], "attempt": t["attempt"]}, slock)
                print(f"{args.worker_name}: done {t['task_id']}", flush=True)
        except (OSError, ValueError):
            pass
        finally:
            stop.set()
        return crashed.is_set()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("server_ip")
    parser.add_argument("worker_name")
    parser.add_argument("--speed", type=float, default=1.0, help="task time = delay / speed")
    parser.add_argument("--fail-after", type=float, help="simulate a crash after N seconds")
    parser.add_argument("--fail-on-task", type=int, help="simulate a crash half way through the Nth task")
    parser.add_argument("--restart-after", type=float, help="after the crash, reconnect N seconds later")
    parser.add_argument("--slowdown", type=float, default=1.0, help="test only: really work N times slower than the speed we report")
    args = parser.parse_args()

    crashed_once = False
    while True:
        try:
            crashed = session(args, sim_fail=not crashed_once)
        except OSError as err:
            if not crashed_once:
                print(f"{args.worker_name}: cannot connect: {err}", flush=True)
                return
            time.sleep(1)       # restarting: keep trying, the sdn quarantine may still block us
            continue
        if not crashed or args.restart_after is None:
            return
        crashed_once = True
        print(f"{args.worker_name}: restarting in {args.restart_after}s", flush=True)
        time.sleep(args.restart_after)


if __name__ == "__main__":
    main()
