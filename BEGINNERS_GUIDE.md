# Beginner's Guide — Distributed Consensus Engine

This is the only document you need. It explains what the system does, how to run it, where to look at each step, and what every log line means. No prior knowledge of distributed systems is needed.

---

## Part 1 — What is this system? (read this first)

### The plain-English version

Imagine 5 people sitting in a room. They share a notebook (the **ledger**). Whenever someone wants to write an entry (a **transaction**), the group must agree — majority rules. One person is chosen as the **leader**; they propose entries and the others vote. If the leader leaves the room (crashes), the group picks a new leader and continues.

This project is a software version of that scenario running inside Docker containers.

### The 3 things the system demonstrates

| # | Protocol | What it proves | When it runs |
|---|----------|----------------|--------------|
| 1 | **Bully Election** | Nodes can automatically elect a leader | At startup and every time the leader crashes |
| 2 | **Paxos** | Honest nodes can agree on transactions even if some crash | Default mode — runs during normal client submissions |
| 3 | **PBFT** | Nodes can agree even when one node is *actively lying* | When you switch `MODE=pbft` in docker-compose.yml |

### The 7 containers and what each one does

| Container | Role | What it does |
|-----------|------|--------------|
| `node1` | Honest node (lowest priority) | Participates in voting; becomes leader only if nodes 2–5 are all dead |
| `node2` | Honest node | Participates in voting |
| `node3` | Honest node | Participates in voting |
| `node4` | Honest node (2nd highest priority) | Becomes leader if node5 crashes |
| `node5` | Honest node (**default leader**) | Has highest ID so wins the first election automatically |
| `adversary` | Malicious node | Sends fake/conflicting messages to test if honest nodes can detect lies |
| `client` | Transaction submitter | Sends 10 transactions to the leader and reports which ones were committed |
| `toxiproxy` | Network chaos tool | Can simulate slow/broken network between nodes (used in chaos test) |

---

## Part 2 — One-time setup

> Do this once. You never need to do it again unless you change the source code.

Open a terminal, navigate to the project folder, and run:

```bash
cd /path/to/distributed-consensus-engine
docker-compose build
```

**How long it takes:** 1–3 minutes (downloads Python, installs dependencies).

**How to know it worked:** The last few lines of output will look like:
```
Successfully built abc123def456
Successfully tagged distributed-consensus-engine_node1:latest
```

If you see `error` in red, check that Docker Desktop is running (whale icon in your menu bar).

---

## Part 3 — Starting the cluster

```bash
docker-compose up
```

**What happens:** All 7 containers start at once. Their logs all print to the same terminal — this is normal and expected. It will look chaotic at first.

### Taming the log chaos

Open a **second terminal** in the same folder and run this to see only the important events:

```bash
docker-compose logs -f | grep -E "NEW LEADER|Leader is now|COMMITTED|FAILED|PAXOS|PBFT"
```

This filters out the noise and shows only leader elections, consensus rounds, and transaction results.

---

## Part 4 — Viewing logs in Docker Desktop (easier for beginners)

Docker Desktop has a GUI that lets you read one container's logs at a time — much easier than a scrolling terminal.

**Step-by-step:**

1. Open **Docker Desktop** (click the whale icon in your menu bar or taskbar).
2. Click **"Containers"** in the left sidebar.
3. You will see a group called `distributed-consensus-engine` with an arrow. Click the arrow to expand it — you'll see all 7 containers listed.
4. To see **one node's logs**: click the container name (e.g. `node5`) → click the **"Logs"** tab at the top.
5. To **search inside logs**: type a keyword in the search box at the top right of the Logs panel (e.g. type `LEADER` to find election events, `COMMITTED` to find transaction commits).
6. To see **all containers' logs together**: click the group row `distributed-consensus-engine` → click **"Logs"**.

> **Tip:** When testing a specific stage, open Docker Desktop → click the relevant node → watch its Logs tab. Much cleaner than the terminal.

---

## Part 5 — Stage-by-stage walkthrough

Work through these stages in order. Each one shows you what to run, where to look, what log lines to expect, and how to confirm it worked.

---

### Stage 1 — Startup (happens automatically, seconds 0–5)

**What runs:** Each node boots up, generates its RSA security keys, starts its TCP server, and shares its public key with all peers.

**Where to look:**
- Docker Desktop → click `node1` → Logs tab
- Or terminal: `docker-compose logs node1`

**Log lines to look for:**
```
[Node 1] Starting in PAXOS mode
[Node 1] Server listening on port 5000
[Node 1] Got public key from Node 2
[Node 1] Got public key from Node 3
```

**What this means:**
- `Server listening` → the node is alive and ready to receive messages from peers
- `Got public key from Node X` → the nodes have completed a crypto handshake; they can now verify each other's signatures

**✅ Stage 1 complete when:** All 5 nodes (node1–node5) show `Server listening on port 5000` in their logs.

---

### Stage 2 — Leader Election / Bully Algorithm (seconds 5–8)

**What runs:** Node5 has the highest ID (5 > 4 > 3 > 2 > 1). The Bully Algorithm says the highest-ID alive node always wins. Node5 checks if anyone ranks higher — nobody does — so it declares itself leader and tells everyone.

**Where to look:**
- Docker Desktop → click `node5` → Logs tab (to see the winner)
- Docker Desktop → click `node3` → Logs tab (to see a follower acknowledge)
- Or filtered terminal: `docker-compose logs -f | grep -E "NEW LEADER|Leader is now"`

**Log lines to look for:**
```
node5  | [Node 5] *** I AM THE NEW LEADER ***
node4  | [Node 4] Leader is now Node 5
node3  | [Node 3] Leader is now Node 5
node2  | [Node 2] Leader is now Node 5
node1  | [Node 1] Leader is now Node 5
```

**What this means:** The Bully Algorithm completed successfully. Node5 will now send a heartbeat ping to all followers every 1 second. Followers reset their 3-second timeout each time they receive one.

**✅ Stage 2 complete when:** All 4 follower nodes print `Leader is now Node 5`.

---

### Stage 3 — Paxos: Normal Transaction Processing (seconds 10–25)

**What runs:** The client container wakes up after a 10-second delay, discovers node5 is the leader, then submits 10 transactions one by one. For each transaction, the leader runs a 2-phase Paxos round: ask for votes (Prepare), collect votes (Promise), send value (Accept), collect confirmations (Accepted), then commit.

**Where to look (two windows):**

Window A — client progress:
```bash
docker logs client
```

Window B — what the leader is doing:
```bash
docker logs node5
```

Or in Docker Desktop: open `client` and `node5` in separate tabs.

**Log lines to look for — client (`docker logs client`):**
```
[CLIENT] Leader found at node5:5000

[CLIENT] [1/10] Submitting: TX001: Aarav sends $100 to Priya
  -> COMMITTED
[CLIENT] [2/10] Submitting: TX002: Priya sends $50 to Rahul
  -> COMMITTED
...
[CLIENT] [10/10] Submitting: TX010: Meera sends $15 to Aarav
  -> COMMITTED

==================================================
  DONE: 10 committed, 0 failed
  Total: 10 transactions
==================================================
```

**Log lines to look for — node5 (`docker logs node5`):**
```
[Node 5] Client request: 'TX001: Aarav sends $100 to Priya'
[Node 5] PAXOS Phase 1: PREPARE(n=11)
[Node 5] PAXOS Phase 2: ACCEPT(n=11, v='TX001: Aarav sends $100 to Priya')
[Node 5] PAXOS CONSENSUS REACHED: 'TX001: Aarav sends $100 to Priya'
[Node 5] Ledger: 1 entries
```

**What this means, line by line:**
- `PREPARE(n=11)` → Leader asking all nodes: "I want to propose with ticket number 11. Can I?"
- `ACCEPT(n=11, v='...')` → Leader got 3+ yes votes, now sending the actual value: "Please accept this transaction"
- `CONSENSUS REACHED` → 3+ nodes replied "Accepted" → transaction is now committed
- `Ledger: 1 entries` → The transaction was written to disk permanently

**✅ Stage 3 complete when:** Client prints `DONE: 10 committed, 0 failed`.

---

### Stage 4 — Leader Crash & Automatic Re-election (manual test)

**What runs:** You manually kill node5. Followers stop receiving heartbeats. After 3 seconds of silence, node4 (the next highest ID) calls a new election and wins.

**Step 1 — Kill the leader.** Open a new terminal and run:
```bash
docker-compose stop node5
```

**Step 2 — Watch the re-election.** In Docker Desktop, open `node4` → Logs. Or in terminal:
```bash
docker-compose logs -f node4
```

**Log lines to look for on node4:**
```
[Node 4] Leader timeout! Starting election...
[Node 4] *** I AM THE NEW LEADER ***
```

**Log lines to look for on followers (node1, node2, node3):**
```
[Node 3] Leader is now Node 4
[Node 1] Leader is now Node 4
```

**How long it takes:** About 3–5 seconds after you run `docker-compose stop node5`.

**What this means:**
- `Leader timeout!` → 3 seconds passed without a heartbeat → node4 assumed node5 crashed
- `I AM THE NEW LEADER` → Bully algorithm ran again; node4 won (highest ID still alive)
- Followers now send their heartbeat timer to node4

**Step 3 — Restore node5:**
```bash
docker-compose start node5
```

**What you will see after node5 restarts:**
```
[Node 5] *** I AM THE NEW LEADER ***
[Node 4] Leader is now Node 5
```

Node5 takes leadership back from node4. **This is correct and expected.** The Bully Algorithm's rule is absolute: the highest-ID alive node is always the leader. When node5 comes back, it sees no recent heartbeat, triggers a new election, and wins because ID=5 beats ID=4. There is no concept of "respect the existing leader" in this algorithm.

This demonstrates a key property of Bully: a recovered node always reclaims the top role if it has the highest ID. The cluster stays consistent — node4 gracefully steps down.

**✅ Stage 4 complete when:** node4 prints `I AM THE NEW LEADER` after the crash, then node5 prints `I AM THE NEW LEADER` after the restore.

---

### Stage 5 — PBFT with Byzantine Adversary

**What runs:** The adversary node (ID=6, the highest ID!) tries to confuse honest nodes by sending different fake messages to each peer. PBFT's digest-matching logic catches the inconsistency and ignores the lies.

> Note: The adversary has NODE_ID=6 — higher than all honest nodes. In Paxos mode it would win the election and then withhold heartbeats (causing chaos). In PBFT mode it attacks the three-phase commit instead.

**Step 1 — Switch to PBFT mode.** Stop the cluster:
```bash
docker-compose down
```

Open `docker-compose.yml` in a text editor. Find every line that says `MODE=paxos` under node1, node2, node3, node4, node5 and change them all to `MODE=pbft`. Leave the adversary's `MODE=pbft` line unchanged (it already says pbft).

**Step 2 — Restart:**
```bash
docker-compose up --build
```

**Where to look — adversary attacking:**
Docker Desktop → click `adversary` → Logs
```
[ADVERSARY] *** MALICIOUS NODE ACTIVATED ***
[ADVERSARY] *** Attack mode: equivocate ***
[ADVERSARY] *** EQUIVOCATING on seq=1 ***
[ADVERSARY] *** Sent fake digest to node 1 ***
[ADVERSARY] *** Sent fake digest to node 2 ***
[ADVERSARY] *** Sent fake digest to node 3 ***
```

**Where to look — honest nodes catching the lies:**
Docker Desktop → click `node2` → Logs
```
[Node 2] PBFT: Digest mismatch! Ignoring.
[Node 2] PBFT: Prepare quorum for seq=1
[Node 2] PBFT COMMITTED: seq=1, tx='TX001: Aarav sends $100 to Priya'
```

**Where to look — client result:**
```bash
docker logs client
```
Expected: still `DONE: 10 committed, 0 failed` — honest nodes ignored the lies.

**What this means:**
- The adversary sent node1 fake_digest_A and node2 fake_digest_B (different!)
- Honest nodes compare the digest against their own copy of the Pre-Prepare
- Mismatches are detected and dropped
- The remaining 3+ honest nodes still have matching digests → quorum reached → committed

**✅ Stage 5 complete when:** Client shows `DONE: 10 committed, 0 failed` AND adversary logs show equivocation attempts.

**Step 3 — Reset back to Paxos:**
```bash
docker-compose down
# Change MODE=pbft back to MODE=paxos in docker-compose.yml for node1–5
```

---

### Stage 6 — Automated Chaos Test

**What runs:** A shell script runs 5 automated tests: normal operation, leader crash, network partition via Toxiproxy, PBFT with adversary, and high network latency.

**Run it:**
```bash
bash tests/chaos_test.sh
```

**Where to look:** The script prints its own coloured output directly to your terminal. You don't need to watch Docker logs — the script queries the cluster and reports pass/fail for each test.

**Expected output (abridged):**
```
[TEST 0] Checking cluster health... PASS
[TEST 1] Normal operation... PASS
[TEST 2] Leader crash & recovery... PASS
[TEST 3] Network partition... PASS
[TEST 4] PBFT Byzantine adversary... PASS
[TEST 5] High latency injection... PASS

All tests passed!
```

**✅ Stage 6 complete when:** All tests show `PASS`.

---

## Part 6 — Verifying the ledger (proof that consensus worked)

After the client finishes submitting transactions, every node should have saved the same 10 transactions in the same order. Run these to compare:

```bash
docker exec node1 cat /app/data/ledger_node_1.json
docker exec node5 cat /app/data/ledger_node_5.json
```

Expected output (same on both nodes):
```json
[
  "TX001: Aarav sends $100 to Priya",
  "TX002: Priya sends $50 to Rahul",
  "TX003: Rahul sends $25 to Ananya",
  "TX004: Ananya sends $75 to Vikram",
  "TX005: Vikram sends $30 to Deepika",
  "TX006: Deepika sends $60 to Arjun",
  "TX007: Arjun sends $40 to Kavya",
  "TX008: Kavya sends $20 to Rohit",
  "TX009: Rohit sends $90 to Meera",
  "TX010: Meera sends $15 to Aarav"
]
```

**What this proves:** node1 and node5 reached the same conclusion independently, without ever sharing their ledger files directly. This is what consensus means.

---

## Part 7 — What every log line means

| Log line | Plain English meaning |
|----------|-----------------------|
| `Starting in PAXOS mode` | Node booted, will use Paxos for consensus |
| `Server listening on port 5000` | Node is online and ready for connections |
| `Got public key from Node X` | Crypto handshake complete with peer X |
| `Leader timeout! Starting election...` | No heartbeat for 3 s → assumed leader crashed |
| `I AM THE NEW LEADER` | This node won the Bully election |
| `Leader is now Node X` | This follower now recognises Node X as leader |
| `Client request: 'TX...'` | Leader received a transaction from the client |
| `PAXOS Phase 1: PREPARE(n=N)` | Leader asking peers: "may I propose with ticket N?" |
| `PAXOS Phase 2: ACCEPT(n=N, v='...')` | Leader got majority yes → sending the value |
| `PAXOS CONSENSUS REACHED` | Majority accepted → transaction committed |
| `PBFT Pre-Prepare: seq=N` | Leader broadcasting transaction N to all nodes |
| `PBFT Prepare: seq=N` | Follower echoing "I received seq=N, digest matches" |
| `PBFT: Prepare quorum for seq=N` | 3+ nodes matched on seq=N → advancing to Commit |
| `PBFT COMMITTED: seq=N` | 3+ Commits received → transaction locked in |
| `MALICIOUS NODE ACTIVATED` | Adversary container started |
| `EQUIVOCATING on seq=N` | Adversary sending different fake digests to each peer |
| `Sent fake digest to node X` | Adversary sent its unique lie to node X |
| `Digest mismatch! Ignoring.` | Honest node caught the adversary lying → dropped |
| `Ledger: N entries` | N transactions durably saved to disk on this node |
| `-> COMMITTED` | Client: this transaction was successfully committed |
| `-> FAILED (no response)` | Node unreachable — check if its container is running |
| `-> Error: Not leader` | Client sent to wrong node — it auto-finds the real leader |

---

## Part 8 — Useful commands cheat sheet

```bash
# Start everything
docker-compose up

# Start in background (no log flood)
docker-compose up -d

# Watch filtered key events only
docker-compose logs -f | grep -E "NEW LEADER|Leader is now|COMMITTED|FAILED|PAXOS|PBFT"

# See one container's logs
docker logs node5
docker logs client
docker logs adversary

# Follow one container's logs live
docker-compose logs -f node5

# Kill one node (simulate crash)
docker-compose stop node5

# Bring it back
docker-compose start node5

# Check who is the current leader (from inside the cluster)
docker exec node1 python -c "
import socket, json
s = socket.socket(); s.settimeout(5)
s.connect(('node1', 5000))
s.sendall((json.dumps({'type': 'who_is_leader'}) + '\n').encode())
print(json.loads(s.recv(4096).decode()))
s.close()
"

# Read a node's ledger
docker exec node1 cat /app/data/ledger_node_1.json

# Stop everything (keep ledger data)
docker-compose down

# Stop everything AND delete all saved ledger data
docker-compose down -v

# Rebuild images after code changes
docker-compose build
```

---

## Part 9 — Troubleshooting

**"Connection refused" in client logs**
The client started before the nodes were ready. Wait 10–15 seconds. If it persists, check `docker-compose logs node5` — node5 may have crashed. Run `docker-compose ps` to see which containers are up.

**"DONE: 10 committed, 0 failed" but ledger is empty**
The ledger file path is `/app/data/ledger_node_X.json`. If the volume doesn't exist yet, the directory may not have been created. Run `docker-compose down -v` then `docker-compose up` to start fresh.

**Adversary has NODE_ID=6 and wins the election in Paxos mode**
This is by design. If the adversary wins election and withholds heartbeats, node4 will eventually detect the timeout and re-elect. Switch to PBFT mode to see the adversary's Byzantine attacks instead.

**Logs scroll too fast to read**
Use Docker Desktop → click a specific container → Logs tab. Much calmer than the terminal. Use the search box to jump to specific keywords.

**`docker-compose: command not found`**
Try `docker compose` (with a space, no hyphen) — newer versions of Docker use this syntax.

**Container keeps restarting**
Run `docker logs <container-name>` to see the error before the crash. Usually a missing dependency or port conflict.
