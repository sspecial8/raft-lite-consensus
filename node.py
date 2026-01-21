import argparse
import threading
import time
import random
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# ---------- Node identity ----------
node_id = None
port = None
peers = []

# ---------- Raft state ----------
state = "Follower"          # Follower | Candidate | Leader
current_term = 0
voted_for = None
log = []
commit_index = -1

last_heartbeat = time.time()
votes_received = 0

ELECTION_TIMEOUT = (3, 5)
HEARTBEAT_INTERVAL = 1


# ---------- Helpers ----------
def majority():
    return (len(peers) + 1) // 2 + 1


# ---------- Election ----------
def election_timeout_loop():
    global state, current_term, voted_for, votes_received

    while True:
        time.sleep(0.2)

        if state != "Leader" and time.time() - last_heartbeat > random.uniform(*ELECTION_TIMEOUT):
            state = "Candidate"
            current_term += 1
            voted_for = node_id
            votes_received = 1

            print(f"[Node {node_id}] Timeout → Candidate (term {current_term})")

            for peer in peers:
                threading.Thread(
                    target=request_vote,
                    args=(peer,),
                    daemon=True
                ).start()


def request_vote(peer):
    global votes_received, state

    try:
        r = requests.post(
            f"http://{peer}/request_vote",
            json={"term": current_term, "candidateId": node_id},
            timeout=1
        )
        data = r.json()

        if data["voteGranted"]:
            votes_received += 1
            if votes_received >= majority() and state == "Candidate":
                state = "Leader"
                print(f"[Node {node_id}] Became Leader (term {current_term})")
                threading.Thread(target=heartbeat_loop, daemon=True).start()
    except:
        pass


@app.route("/request_vote", methods=["POST"])
def handle_request_vote():
    global current_term, voted_for, state

    data = request.json
    term = data["term"]
    candidate = data["candidateId"]

    if term > current_term:
        current_term = term
        voted_for = None
        state = "Follower"

    vote_granted = False
    if voted_for is None and term == current_term:
        voted_for = candidate
        vote_granted = True

    return jsonify({"term": current_term, "voteGranted": vote_granted})


# ---------- Heartbeats ----------
def heartbeat_loop():
    while state == "Leader":
        for peer in peers:
            try:
                requests.post(
                    f"http://{peer}/append_entries",
                    json={
                        "term": current_term,
                        "leaderId": node_id,
                        "entries": []
                    },
                    timeout=1
                )
            except:
                pass

        print(f"[Leader {node_id}] Heartbeat (term {current_term})")
        time.sleep(HEARTBEAT_INTERVAL)


@app.route("/append_entries", methods=["POST"])
def handle_append_entries():
    global last_heartbeat, state, current_term

    data = request.json
    term = data["term"]

    if term >= current_term:
        current_term = term
        state = "Follower"
        last_heartbeat = time.time()

    return jsonify({"success": True})


# ---------- Client command ----------
@app.route("/client_command", methods=["POST"])
def client_command():
    global log, commit_index

    if state != "Leader":
        return jsonify({"error": "Not leader"}), 400

    cmd = request.json["cmd"]
    entry = {"term": current_term, "cmd": cmd}
    log.append(entry)

    print(f"[Leader {node_id}] Append log entry (term={current_term}, cmd={cmd})")

    acks = 1
    for peer in peers:
        try:
            r = requests.post(
                f"http://{peer}/replicate",
                json={"entry": entry},
                timeout=1
            )
            if r.json()["success"]:
                acks += 1
        except:
            pass

    if acks >= majority():
        commit_index += 1
        print(f"[Leader {node_id}] Entry committed (index={commit_index})")

    return jsonify({"success": True})


@app.route("/replicate", methods=["POST"])
def replicate_entry():
    global log
    entry = request.json["entry"]
    log.append(entry)
    print(f"[Node {node_id}] Append success")
    return jsonify({"success": True})


# ---------- Main ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--peers", required=True)

    args = parser.parse_args()
    node_id = args.id
    port = int(args.port)
    peers = [f"127.0.0.1:{p}" for p in args.peers.split(",")]

    print(f"[Node {node_id}] Started on port {port}")

    threading.Thread(target=election_timeout_loop, daemon=True).start()
    app.run(port=port)
