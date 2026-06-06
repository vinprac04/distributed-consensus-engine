"""
Client - Submits transactions to the distributed consensus cluster.

Flow:
  1. Wait for the cluster to boot and elect a Raft leader.
  2. Discover which node is currently the leader by querying every node.
  3. Send each transaction to the leader one at a time.
  4. If the leader rejects a request (e.g. it was just deposed), re-discover
     the new leader and retry that single transaction once more.
  5. Print a final tally of committed vs. failed transactions.

Why talk only to the leader?
  In Raft, only the leader can accept new log entries and drive them to a
  majority commit.  Follower nodes don't accept writes; they redirect or
  reject them.  So we must route every client_request to whoever holds the
  leadership role right now.
"""

import socket
import json
import os
import time

# ---------------------------------------------------------------------------
# Configuration — pulled from environment so Docker Compose can override them
# without rebuilding the image.
#
#   LEADER_HOST / LEADER_PORT : fallback address if no node responds to the
#                               "who_is_leader" query (e.g. during cold start).
#   ALL_NODES                 : comma-separated "host:port" list of every node
#                               in the cluster — used to probe for the leader.
# ---------------------------------------------------------------------------
LEADER_HOST = os.environ.get('LEADER_HOST', 'node5')
LEADER_PORT = int(os.environ.get('LEADER_PORT', '5000'))
ALL_NODES = os.environ.get('ALL_NODES', 'node1:5000,node2:5000,node3:5000,node4:5000,node5:5000')


def send_to_node(host, port, message):
    """
    Open a short-lived TCP connection to a single cluster node, send one JSON
    message, and read back a single JSON response line.

    Protocol detail:
      - Each message is a JSON object terminated by a newline ('\\n').
      - The node writes back a JSON object on one line; we read up to 4096 bytes.
      - The socket is closed immediately after — we don't reuse connections.
        This keeps the client stateless and avoids stale-socket surprises when
        a node crashes and restarts.

    Returns the parsed response dict, or None if anything goes wrong (timeout,
    refused connection, malformed JSON, etc.).  Callers must treat None as
    "this node is unreachable right now".
    """
    try:
        # AF_INET = IPv4, SOCK_STREAM = TCP (reliable, ordered, connection-based)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # 10-second deadline — if the node is alive but slow we'll give up rather
        # than block the whole submission loop indefinitely.
        s.settimeout(10)

        s.connect((host, port))

        # json.dumps serialises the Python dict → JSON string.
        # The trailing '\n' acts as a message delimiter on the receiving end.
        s.sendall((json.dumps(message) + '\n').encode())

        # Block until the node writes a response (or the timeout fires).
        # 4096 bytes is large enough for any single status reply.
        response = s.recv(4096).decode().strip()
        s.close()

        # An empty response usually means the node closed the connection early
        # (e.g. it crashed mid-reply).  Return None so the caller can handle it.
        if response:
            return json.loads(response)
        return None

    except socket.timeout:
        # Node is up but not answering — could be overloaded or doing an
        # election.  We log and return None; caller will retry or skip.
        print(f"  Timeout connecting to {host}:{port}")
        return None
    except ConnectionRefusedError:
        # Node process isn't listening — it may still be starting up or it
        # crashed.  Common during the first few seconds after `docker compose up`.
        print(f"  Connection refused by {host}:{port}")
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None


def find_leader():
    """
    Broadcast a 'who_is_leader' query to every known node and return the first
    affirmative answer we get.

    Why broadcast instead of asking just one node?
      - Any node (leader or follower) knows who the current leader is because
        Raft nodes track the leaderId in their volatile state.
      - If the node we try first is down, we fall through to the next one.
      - The first node that replies with a valid leader_host wins; we stop there.

    Fallback:
      If every node is unreachable (cluster still booting, network partition,
      etc.) we fall back to the hardcoded LEADER_HOST / LEADER_PORT from the
      environment.  This is a best-effort guess — the submission loop will get
      a proper error and can call find_leader() again.
    """
    nodes = ALL_NODES.split(',')
    for node_str in nodes:
        node_str = node_str.strip()
        if not node_str:
            continue  # skip empty strings from trailing commas in the env var
        host, port = node_str.split(':')
        result = send_to_node(host, int(port), {'type': 'who_is_leader'})
        if result and result.get('leader_host'):
            # Got a definitive answer — stop querying the rest.
            return result['leader_host'], result['leader_port']

    # No node answered; return the env-var fallback so we can at least try.
    return LEADER_HOST, LEADER_PORT


def main():
    print("=" * 50)
    print("  DISTRIBUTED CONSENSUS CLIENT")
    print("=" * 50)
    print()

    # -----------------------------------------------------------------------
    # Step 1 — Give the cluster time to elect its first leader.
    #
    # Raft needs a majority of nodes to exchange votes before one node becomes
    # leader.  With 5 nodes that's 3 votes; the election timeout is randomised
    # (150–300 ms typically) to avoid split votes.  In practice the whole
    # process takes well under a second, but Docker container start-up adds
    # latency.  10 seconds is a conservative safety margin.
    # -----------------------------------------------------------------------
    print("[CLIENT] Waiting 10 seconds for cluster to stabilize...")
    time.sleep(10)

    # -----------------------------------------------------------------------
    # Step 2 — Locate the current leader.
    # -----------------------------------------------------------------------
    leader_host, leader_port = find_leader()
    print(f"[CLIENT] Leader found at {leader_host}:{leader_port}")
    print()

    # -----------------------------------------------------------------------
    # Step 3 — Define the workload: a list of financial transactions.
    #
    # Each string is treated as an opaque command by the consensus layer.
    # The Raft log doesn't interpret the content — it just guarantees that
    # every node will apply these entries in the same order.  A real system
    # would parse and execute them inside the state machine; here they are
    # stored as plain strings for demonstration purposes.
    # -----------------------------------------------------------------------
    transactions = [
        "TX001: Aarav sends $100 to Priya",
        "TX002: Priya sends $50 to Rahul",
        "TX003: Rahul sends $25 to Ananya",
        "TX004: Ananya sends $75 to Vikram",
        "TX005: Vikram sends $30 to Deepika",
        "TX006: Deepika sends $60 to Arjun",
        "TX007: Arjun sends $40 to Kavya",
        "TX008: Kavya sends $20 to Rohit",
        "TX009: Rohit sends $90 to Meera",
        "TX010: Meera sends $15 to Aarav",
    ]

    # -----------------------------------------------------------------------
    # Step 4 — Submit transactions sequentially, one per 1.5 seconds.
    #
    # Why not fire them all at once?
    #   Raft commits one log entry per round-trip (AppendEntries RPC + majority
    #   acknowledgement).  Flooding the leader with concurrent requests can
    #   cause queuing and makes it harder to trace which transaction caused an
    #   issue.  Sequential submission with a small delay keeps the log clean
    #   and lets us see each commit clearly.
    #
    # Error handling — two-tier retry:
    #   Tier 1: The leader replies with status='error'.  This usually means
    #           leadership changed (the old leader lost its majority and stepped
    #           down).  We re-run find_leader() and retry the same transaction
    #           once against the new leader.
    #   Tier 2: No response at all (None).  The leader may have crashed mid-
    #           commit.  We mark the transaction as failed rather than retrying
    #           blindly, because the entry might already be committed on a
    #           majority — double-submission could cause a duplicate.
    # -----------------------------------------------------------------------
    committed = 0
    failed = 0

    for i, tx in enumerate(transactions, 1):
        print(f"[CLIENT] [{i}/{len(transactions)}] Submitting: {tx}")

        # Send a client_request to the leader.  The node will:
        #   1. Append the transaction to its local Raft log.
        #   2. Broadcast AppendEntries RPCs to all followers.
        #   3. Wait for a majority to acknowledge → entry is now "committed".
        #   4. Apply the entry to the state machine and reply 'committed'.
        result = send_to_node(leader_host, leader_port, {
            'type': 'client_request',
            'transaction': tx
        })

        if result and result.get('status') == 'committed':
            # Happy path — the transaction reached a majority of nodes.
            committed += 1
            print(f"  -> COMMITTED")

        elif result and result.get('status') == 'error':
            # The node we thought was leader is no longer in charge.
            # Re-discover the new leader and attempt the transaction once more.
            print(f"  -> Error: {result.get('message')}")
            print("  -> Searching for new leader...")
            leader_host, leader_port = find_leader()

            result = send_to_node(leader_host, leader_port, {
                'type': 'client_request',
                'transaction': tx
            })
            if result and result.get('status') == 'committed':
                committed += 1
                print(f"  -> COMMITTED (retry)")
            else:
                failed += 1
                print(f"  -> FAILED")

        else:
            # No response — node unreachable or crashed before replying.
            failed += 1
            print(f"  -> FAILED (no response)")

        # Brief pause between submissions so the leader has time to finish the
        # AppendEntries round-trip and apply the committed entry before the
        # next one arrives.
        time.sleep(1.5)

    # -----------------------------------------------------------------------
    # Step 5 — Print final summary.
    # -----------------------------------------------------------------------
    print()
    print("=" * 50)
    print(f"  DONE: {committed} committed, {failed} failed")
    print(f"  Total: {len(transactions)} transactions")
    print("=" * 50)


if __name__ == '__main__':
    main()
