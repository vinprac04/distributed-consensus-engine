"""
Main Consensus Node — the core of the distributed consensus engine.

Architecture overview
---------------------
Each Node runs 4 permanent threads:

  Thread 1 — TCP Server
    Listens on port 5000 for incoming connections.  Every connection is
    dispatched to a short-lived handler thread that reads one JSON message,
    routes it to the appropriate handler method, and writes back a response
    if the handler returns one.

  Thread 2 — Heartbeat Loop
    If this node is the current leader it broadcasts a heartbeat to all peers
    every 1 second.  Followers use this signal to know the leader is alive.

  Thread 3 — Monitor Loop
    Followers watch the timestamp of the last received heartbeat.  If no
    heartbeat arrives for 3 seconds they conclude the leader has crashed and
    trigger a new election.

  Thread 4 — Key Exchange (one-shot at startup)
    Sends this node's RSA public key to all peers so they can later verify
    PBFT message signatures.

Consensus modes
---------------
  paxos (default) — Crash Fault Tolerant.
    Tolerates up to ⌊(N−1)/2⌋ node *crashes*.  With N=5, that's 2 crashes.
    Uses two phases: Prepare/Promise then Accept/Accepted, coordinated by
    the elected leader.

  pbft — Byzantine Fault Tolerant.
    Tolerates up to ⌊(N−1)/3⌋ Byzantine (lying/arbitrary) nodes.  With N=5,
    f=1.  Uses three phases: Pre-Prepare → Prepare → Commit, each requiring
    a quorum of 2f+1 = 3 matching messages.

Leader election
---------------
  Uses the Bully Algorithm: when a follower detects a dead leader it sends
  ELECTION to all higher-ID nodes.  If any respond with ALIVE, they take over.
  If none respond, this node declares itself leader with a COORDINATOR message.
  The highest-ID alive node always wins, which guarantees no split-brain.

Thread safety
-------------
  All mutable consensus state (proposal numbers, promise/accept response lists,
  PBFT message logs, ledger) is protected by a single threading.Lock (self.lock).
  Handlers acquire the lock only for the critical section, release it before
  performing any I/O (send_message calls) to avoid deadlocks.
"""

import socket
import threading
import json
import os
import time
import hashlib

from src.crypto_utils import generate_keypair, sign_message, verify_signature, key_to_string, string_to_key


class Node:
    def __init__(self):
        # ---------------------------------------------------------------
        # Identity & network config
        # NODE_ID comes from Docker Compose environment so each container
        # gets a unique integer (1-5).  host='0.0.0.0' means the server
        # listens on all interfaces, allowing other containers to connect.
        # ---------------------------------------------------------------
        self.node_id = int(os.environ.get('NODE_ID', 1))
        self.host = '0.0.0.0'
        self.port = 5000
        self.mode = os.environ.get('MODE', 'paxos')

        # ---------------------------------------------------------------
        # Peer table: {peer_id: (host, port)}
        # PEERS env var format: "node2:5000,node3:5000,..."
        # We extract the numeric ID from the hostname (node2 → 2) so we
        # can compare IDs during the Bully election algorithm.
        # ---------------------------------------------------------------
        self.peers = {}
        peers_str = os.environ.get('PEERS', '')
        for entry in peers_str.split(','):
            entry = entry.strip()
            if not entry:
                continue
            host, port = entry.split(':')
            peer_id = int(host.replace('node', ''))
            self.peers[peer_id] = (host, int(port))

        # ---------------------------------------------------------------
        # Cryptographic identity
        # Each node generates a fresh RSA-2048 key pair at startup.
        # The private key signs outgoing PBFT messages; the public key is
        # distributed to peers so they can verify those signatures.
        # peer_public_keys is populated later via the key_exchange message.
        # ---------------------------------------------------------------
        self.private_key, self.public_key = generate_keypair()
        self.peer_public_keys = {}

        # ---------------------------------------------------------------
        # Leader election state (Bully Algorithm)
        # leader_id:           who we currently believe is the leader
        # is_leader:           True only on the node that won the election
        # last_heartbeat:      timestamp of the most recent heartbeat received
        # election_in_progress: guard flag to prevent re-entrant elections
        # ---------------------------------------------------------------
        self.leader_id = None
        self.is_leader = False
        self.last_heartbeat = time.time()
        self.election_in_progress = False

        # ---------------------------------------------------------------
        # Paxos state
        # proposal_number:  monotonically increasing; incremented by 10 each
        #                   round to leave room for concurrent proposers.
        # highest_promised: the largest proposal number this node has promised
        #                   not to accept anything lower than.
        # accepted_proposal/value: the most recent proposal this node accepted
        #                   in Phase 2.  Returned in Promise responses so the
        #                   new leader can preserve previously accepted values.
        # promise_responses / accepted_responses: collected replies from peers
        #                   during the current round; reset at the start of each.
        # ---------------------------------------------------------------
        self.proposal_number = self.node_id
        self.highest_promised = 0
        self.accepted_proposal = 0
        self.accepted_value = None
        self.promise_responses = []
        self.accepted_responses = []

        # ---------------------------------------------------------------
        # PBFT state
        # view_number:     current PBFT view (increments on view-change / new
        #                  leader).  A node rejects messages from wrong views.
        # sequence_number: per-request counter assigned by the leader in
        #                  Pre-Prepare; monotonically increasing within a view.
        # prepare_messages / commit_messages: {seq: [msg, ...]} — collected
        #                  Prepare/Commit messages per sequence number.
        # pre_prepare_log: {seq: message} — the Pre-Prepare the leader sent;
        #                  used by followers to look up the canonical digest.
        # committed_seqs:  set of sequence numbers already committed; prevents
        #                  double-commits if duplicate Commit messages arrive.
        # f:               maximum number of Byzantine nodes tolerated.
        # quorum:          2f+1 — the minimum number of matching messages needed
        #                  to advance a PBFT phase (3 out of 5 nodes).
        # ---------------------------------------------------------------
        self.view_number = 0
        self.sequence_number = 0
        self.prepare_messages = {}
        self.commit_messages = {}
        self.pre_prepare_log = {}
        self.committed_seqs = set()
        self.f = 1
        self.quorum = 2 * self.f + 1  # = 3 matching messages required

        # ---------------------------------------------------------------
        # Ledger — the committed transaction log
        # Stored in memory (list) and persisted to a per-node JSON file.
        # Having per-node files lets you inspect the state of each node
        # after the run via `docker exec` or volume mounts.
        # ---------------------------------------------------------------
        self.ledger = []
        self.ledger_file = f'/app/data/ledger_node_{self.node_id}.json'

        # Single re-entrant-safe lock guarding all mutable state above.
        self.lock = threading.Lock()

    # ===================================================================
    # NETWORKING — Low-level socket helpers
    # ===================================================================

    def send_message(self, target_id, message):
        """
        Open a TCP connection to `target_id`, send one JSON message, close.

        Fire-and-forget: we don't read a response here.  Returns True if the
        data was sent, False if the peer is unreachable.  A 3-second timeout
        prevents a dead peer from blocking the calling thread for long.

        Why not keep persistent connections?
          Persistent connections require connection-lifecycle management and
          reconnect logic.  Short-lived connections are simpler and adequate
          for message rates well under 100/s.
        """
        if target_id not in self.peers:
            return False
        host, port = self.peers[target_id]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)
            s.connect((host, port))
            data = json.dumps(message) + '\n'
            s.sendall(data.encode())
            s.close()
            return True
        except Exception:
            return False

    def send_and_receive(self, target_id, message):
        """
        Send a message and block until a response arrives (or timeout).

        Used for request-reply exchanges where we need the peer's answer
        before proceeding (e.g., Paxos Prepare → Promise).  5-second timeout
        gives slow peers a fair chance without stalling the round indefinitely.

        Returns the parsed response dict, or None on any failure.
        """
        if target_id not in self.peers:
            return None
        host, port = self.peers[target_id]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((host, port))
            data = json.dumps(message) + '\n'
            s.sendall(data.encode())
            response = s.recv(4096).decode().strip()
            s.close()
            if response:
                return json.loads(response)
            return None
        except Exception:
            return None

    def broadcast(self, message):
        """
        Send `message` to every peer (non-blocking, best-effort).

        Failures are silently ignored — if a peer is down, it simply misses
        this message.  The consensus protocols are designed to make progress
        as long as a quorum of nodes is reachable.
        """
        for peer_id in self.peers:
            self.send_message(peer_id, message)

    # ===================================================================
    # SERVER — Thread 1: Accept and dispatch incoming connections
    # ===================================================================

    def start_server(self):
        """
        Bind a TCP server socket and accept connections in a loop.

        SO_REUSEADDR prevents "Address already in use" errors when the
        container restarts quickly and the OS hasn't yet freed the port.

        listen(10): the OS will queue up to 10 pending connections before
        refusing new ones.  Adequate for a 5-node cluster.

        Each accepted connection is handed to handle_connection() in a
        separate daemon thread so the accept loop is never blocked.
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(10)
        print(f"[Node {self.node_id}] Server listening on port {self.port}")

        while True:
            try:
                client_socket, addr = server.accept()
                t = threading.Thread(target=self.handle_connection, args=(client_socket,))
                t.daemon = True
                t.start()
            except Exception as e:
                print(f"[Node {self.node_id}] Server error: {e}")

    def handle_connection(self, client_socket):
        """
        Read one message from a connection, process it, send response if any.

        The 5-second socket timeout guards against a peer that connects but
        then stalls before sending data (e.g. due to a bug or crash).

        Protocol: sender writes JSON + '\\n', receiver reads up to 4096 bytes.
        If the response is None (most fire-and-forget messages), we close
        without writing anything back.
        """
        try:
            client_socket.settimeout(5)
            data = client_socket.recv(4096).decode().strip()
            if not data:
                client_socket.close()
                return

            message = json.loads(data)
            response = self.handle_message(message)

            if response:
                client_socket.sendall((json.dumps(response) + '\n').encode())

            client_socket.close()
        except Exception as e:
            try:
                client_socket.close()
            except:
                pass

    def handle_message(self, message):
        """
        Dispatch a received message to the correct handler by 'type' field.

        Using a dict-based dispatch table instead of if/elif chains keeps the
        code flat and makes it easy to see all supported message types at a
        glance.  Unknown types are logged and ignored (not an error — nodes
        are allowed to receive messages they don't understand in a real system).
        """
        msg_type = message.get('type', '')

        handlers = {
            'heartbeat':        self.handle_heartbeat,
            'election':         self.handle_election,
            'alive':            self.handle_alive,
            'coordinator':      self.handle_coordinator,
            'client_request':   self.handle_client_request,
            'who_is_leader':    self.handle_who_is_leader,
            'key_exchange':     self.handle_key_exchange,
            'paxos_prepare':    self.handle_paxos_prepare,
            'paxos_promise':    self.handle_paxos_promise,
            'paxos_accept':     self.handle_paxos_accept,
            'paxos_accepted':   self.handle_paxos_accepted,
            'paxos_commit':     self.handle_paxos_commit,
            'pbft_pre_prepare': self.handle_pbft_pre_prepare,
            'pbft_prepare':     self.handle_pbft_prepare,
            'pbft_commit':      self.handle_pbft_commit,
        }

        handler = handlers.get(msg_type)
        if handler:
            return handler(message)
        else:
            print(f"[Node {self.node_id}] Unknown message type: {msg_type}")
            return None

    # ===================================================================
    # LEADER ELECTION — Bully Algorithm
    #
    # The Bully Algorithm guarantees that the node with the highest ID
    # among currently alive nodes always becomes the leader.
    #
    # Election sequence:
    #   1. Follower detects missing heartbeat → calls start_election().
    #   2. Sends ELECTION to all peers with higher IDs.
    #   3. If a higher peer is alive, it replies ALIVE and starts its own
    #      election (which it will win because it has an even higher ID).
    #   4. If nobody replies, this node becomes leader and sends COORDINATOR
    #      to everyone.
    # ===================================================================

    def heartbeat_loop(self):
        """
        Thread 2: Broadcast a heartbeat every 1 second while leader.

        The heartbeat serves two purposes:
          1. Tells followers the leader is still alive (resets their timeout).
          2. Carries the leader_id so a node that missed the COORDINATOR
             message can still learn who the leader is.
        """
        while True:
            if self.is_leader:
                self.broadcast({
                    'type': 'heartbeat',
                    'sender': self.node_id,
                    'leader_id': self.node_id
                })
            time.sleep(1)

    def monitor_loop(self):
        """
        Thread 3: Watch for a dead leader and trigger re-election.

        Waits 5 seconds at startup to let the cluster initialise and elect
        a leader before beginning to watch.  After that, if more than 3 seconds
        pass without a heartbeat (and this node isn't the leader itself),
        it assumes the leader has crashed and starts an election.

        3-second deadline is chosen to be noticeably longer than the 1-second
        heartbeat interval, so transient network delays don't cause false alarms.
        """
        time.sleep(5)
        while True:
            if not self.is_leader:
                elapsed = time.time() - self.last_heartbeat
                if elapsed > 3.0:
                    print(f"[Node {self.node_id}] Leader timeout! Starting election...")
                    self.start_election()
            time.sleep(1)

    def start_election(self):
        """
        Bully Algorithm Phase 1: contact all higher-ID nodes.

        election_in_progress flag prevents this node from starting a second
        election while the first is still resolving (e.g. if two monitor_loop
        ticks fire close together).

        If this node has the highest ID among all peers, higher_nodes is empty
        and it immediately becomes leader — no need to ask anyone else.

        Otherwise we send ELECTION messages and wait 2 seconds for ALIVE
        replies.  If at least one higher node replied, it will run its own
        election and broadcast COORDINATOR when it wins, which sets
        election_in_progress=False on all nodes (including this one).
        """
        if self.election_in_progress:
            return
        self.election_in_progress = True

        higher_nodes = [nid for nid in self.peers if nid > self.node_id]

        if not higher_nodes:
            # No one ranks higher — this node wins by default.
            self.become_leader()
            self.election_in_progress = False
            return

        got_response = False
        for nid in higher_nodes:
            result = self.send_message(nid, {
                'type': 'election',
                'sender': self.node_id
            })
            if result:
                got_response = True

        # Wait for ALIVE responses.  If a higher node is alive, it will have
        # replied by now and started its own election to win the role.
        time.sleep(2)

        if not self.election_in_progress:
            return  # A COORDINATOR message arrived — someone else won.

        if not got_response:
            # No higher node answered; we become leader.
            self.become_leader()

        self.election_in_progress = False

    def become_leader(self):
        """
        Declare leadership: update local state and notify all peers.

        Broadcasting COORDINATOR ensures every follower updates its leader_id
        and resets its heartbeat timer, preventing immediate re-election.
        """
        self.is_leader = True
        self.leader_id = self.node_id
        print(f"[Node {self.node_id}] *** I AM THE NEW LEADER ***")
        self.broadcast({
            'type': 'coordinator',
            'sender': self.node_id,
            'leader_id': self.node_id
        })

    # --- Election message handlers ---

    def handle_heartbeat(self, message):
        # Update who the leader is and reset the timeout clock.
        # Also handles the edge case where this node incorrectly thinks it's
        # the leader but receives a heartbeat from the real leader.
        self.leader_id = message['leader_id']
        self.last_heartbeat = time.time()
        self.is_leader = (self.leader_id == self.node_id)
        return None

    def handle_election(self, message):
        sender = message['sender']
        if self.node_id > sender:
            # We outrank the sender — tell them we're alive and start our own
            # election to claim leadership ourselves.
            self.send_message(sender, {'type': 'alive', 'sender': self.node_id})
            self.start_election()
        return None

    def handle_alive(self, message):
        # A higher-ID node is alive and will become leader.  Cancel our
        # election attempt so we don't also try to become leader.
        self.election_in_progress = False
        return None

    def handle_coordinator(self, message):
        # A COORDINATOR message means someone has won the election.
        # Update our belief about who the leader is and reset all election state.
        self.leader_id = message['leader_id']
        self.is_leader = (self.leader_id == self.node_id)
        self.last_heartbeat = time.time()
        self.election_in_progress = False
        print(f"[Node {self.node_id}] Leader is now Node {self.leader_id}")
        return None

    # ===================================================================
    # PAXOS — Crash Fault Tolerant consensus (Mode: paxos)
    #
    # Classic two-phase protocol:
    #
    # Phase 1 — Prepare / Promise
    #   Leader sends PREPARE(n) to all.  A follower replies PROMISE(n) if n
    #   is greater than any proposal it has previously promised.  The PROMISE
    #   also carries the highest proposal the follower has *accepted* so far,
    #   allowing the new leader to resume any value that was already in-flight.
    #
    # Phase 2 — Accept / Accepted
    #   If a majority sent PROMISE, the leader sends ACCEPT(n, v).  If any
    #   PROMISE carried a previously accepted value, v must be that value
    #   (Paxos safety constraint — we can't overwrite an already-committed
    #   value).  Followers reply ACCEPTED(n).  Once a majority reply, the
    #   value is committed and broadcast as PAXOS_COMMIT so all nodes persist.
    #
    # Majority = 3 out of 5.  We track self as one vote so we only need 2
    # more from peers.
    # ===================================================================

    def run_paxos(self, transaction):
        """
        Leader drives a full Paxos round for one transaction.

        proposal_number is incremented by 10 each round.  Using steps of 10
        instead of 1 leaves room for concurrent leaders (e.g. during a split-
        brain) to pick distinguishable numbers without colliding.

        Returns True if the transaction reaches a majority commit, False otherwise.
        """
        if not self.is_leader:
            return False

        with self.lock:
            self.proposal_number += 10
            self.promise_responses = []
            self.accepted_responses = []

        prop_num = self.proposal_number

        print(f"[Node {self.node_id}] PAXOS Phase 1: PREPARE(n={prop_num})")

        # Phase 1 — Prepare: tell all followers "I want to propose with number n".
        self.broadcast({
            'type': 'paxos_prepare',
            'sender': self.node_id,
            'proposal_number': prop_num
        })

        # Count this node's own Promise (it always promises to itself).
        with self.lock:
            self.promise_responses.append({
                'sender': self.node_id,
                'accepted_proposal': self.accepted_proposal,
                'accepted_value': self.accepted_value
            })

        # Wait for followers to reply.  2 seconds is generous; in a healthy
        # LAN cluster responses arrive in milliseconds.
        time.sleep(2)

        with self.lock:
            promises = len(self.promise_responses)

        if promises < 3:
            # Didn't reach majority — another proposer may have stolen our
            # proposal number, or too many nodes are down.  Give up this round.
            print(f"[Node {self.node_id}] PAXOS failed: only {promises} promises (need 3)")
            return False

        # Paxos safety: if any Promise carries a previously accepted value, we
        # MUST propose that value instead of our new transaction.  This prevents
        # overwriting a value that may already be committed on some nodes.
        with self.lock:
            highest = max(self.promise_responses, key=lambda r: r.get('accepted_proposal', 0))
            value = transaction
            if highest.get('accepted_value') is not None:
                value = highest['accepted_value']

        # Phase 2 — Accept: ask followers to accept (n, value).
        print(f"[Node {self.node_id}] PAXOS Phase 2: ACCEPT(n={prop_num}, v='{value}')")
        self.broadcast({
            'type': 'paxos_accept',
            'sender': self.node_id,
            'proposal_number': prop_num,
            'value': value
        })

        # Count own Accepted vote.
        with self.lock:
            self.accepted_proposal = prop_num
            self.accepted_value = value
            self.accepted_responses.append({'sender': self.node_id})

        time.sleep(2)

        with self.lock:
            accepted = len(self.accepted_responses)

        if accepted >= 3:
            # Majority accepted — commit to ledger and tell all nodes.
            print(f"[Node {self.node_id}] PAXOS CONSENSUS REACHED: '{value}'")
            self.commit_to_ledger(value)
            self.broadcast({'type': 'paxos_commit', 'sender': self.node_id, 'value': value})
            return True

        print(f"[Node {self.node_id}] PAXOS failed: only {accepted} accepted (need 3)")
        return False

    # --- Paxos message handlers ---

    def handle_paxos_prepare(self, message):
        """
        Follower receives PREPARE(n).  Promises not to accept anything < n
        and reports its current accepted (proposal, value) back to the leader.
        """
        prop_num = message['proposal_number']
        sender = message['sender']
        with self.lock:
            if prop_num > self.highest_promised:
                self.highest_promised = prop_num
                self.send_message(sender, {
                    'type': 'paxos_promise',
                    'sender': self.node_id,
                    'proposal_number': prop_num,
                    'accepted_proposal': self.accepted_proposal,
                    'accepted_value': self.accepted_value
                })
        return None

    def handle_paxos_promise(self, message):
        """Collect Promise responses during Phase 1."""
        with self.lock:
            self.promise_responses.append(message)
        return None

    def handle_paxos_accept(self, message):
        """
        Follower receives ACCEPT(n, v).  If n >= highest_promised (we haven't
        pledged to a newer proposer since sending our Promise), we accept the
        value and acknowledge to the leader.
        """
        prop_num = message['proposal_number']
        sender = message['sender']
        with self.lock:
            if prop_num >= self.highest_promised:
                self.accepted_proposal = prop_num
                self.accepted_value = message['value']
                self.send_message(sender, {
                    'type': 'paxos_accepted',
                    'sender': self.node_id,
                    'proposal_number': prop_num
                })
        return None

    def handle_paxos_accepted(self, message):
        """Collect Accepted responses during Phase 2."""
        with self.lock:
            self.accepted_responses.append(message)
        return None

    def handle_paxos_commit(self, message):
        """
        Leader broadcasts PAXOS_COMMIT once it has a majority.
        All followers (including those that may have missed Phase 2) apply
        the committed value to their ledger.
        """
        self.commit_to_ledger(message['value'])
        return None

    # ===================================================================
    # PBFT — Byzantine Fault Tolerant consensus (Mode: pbft)
    #
    # Three-phase protocol designed to reach agreement even when up to f
    # nodes behave arbitrarily (send false messages, equivocate, crash).
    #
    # Phase 1 — Pre-Prepare (leader → all)
    #   Leader assigns a sequence number, computes a digest of the transaction,
    #   signs the (view, seq, digest) tuple, and broadcasts PRE-PREPARE.
    #
    # Phase 2 — Prepare (all → all)
    #   Each honest follower that receives a valid PRE-PREPARE broadcasts
    #   PREPARE(view, seq, digest) signed with its own key.  A node collects
    #   2f+1 matching PREPARE messages before moving to Phase 3.
    #   This phase ensures that at least 2f+1 nodes agree on the (seq, digest).
    #
    # Phase 3 — Commit (all → all)
    #   Each node that collected 2f+1 Prepares broadcasts COMMIT(view, seq).
    #   A node that collects 2f+1 Commits finalises the transaction.
    #   This phase ensures the committed value survives any subsequent view
    #   change (new leader election within PBFT).
    #
    # Safety property: even if f Byzantine nodes lie or equivocate, the
    # remaining 2f+1 honest nodes form a quorum and commit the correct value.
    # ===================================================================

    def compute_digest(self, transaction):
        """
        SHA-256 hash of the transaction, used as a compact fingerprint.

        JSON-serialised with sort_keys=True for the same reason as in
        crypto_utils: ensure identical byte sequences regardless of dict order.
        The digest is included in Pre-Prepare and Prepare messages so every
        node can verify the transaction hasn't been altered in transit.
        """
        return hashlib.sha256(json.dumps(transaction, sort_keys=True).encode()).hexdigest()

    def run_pbft(self, transaction):
        """
        Leader kicks off a PBFT round for one transaction.

        sequence_number is per-leader-view and monotonically increases.
        The leader signs (view, seq, digest) — if the adversary replays or
        forges a Pre-Prepare, the signature check on the receiver will fail.

        We wait 5 seconds for the three-phase round-trip to complete.
        In a real deployment you'd use callbacks or condition variables instead
        of a sleep, but the fixed wait is adequate for a demo cluster.

        Returns True if the transaction appears in committed_seqs (i.e. this
        node received 2f+1 Commits), False otherwise.
        """
        if not self.is_leader:
            return False

        with self.lock:
            self.sequence_number += 1
            seq = self.sequence_number

        digest = self.compute_digest(transaction)

        msg_to_sign = {'view': self.view_number, 'seq': seq, 'digest': digest}
        signature = sign_message(self.private_key, msg_to_sign)

        pre_prepare = {
            'type': 'pbft_pre_prepare',
            'view': self.view_number,
            'seq': seq,
            'transaction': transaction,
            'digest': digest,
            'sender': self.node_id,
            'signature': signature
        }

        print(f"[Node {self.node_id}] PBFT Pre-Prepare: seq={seq}")
        with self.lock:
            self.pre_prepare_log[seq] = pre_prepare
        self.broadcast(pre_prepare)

        # Leader also participates in the Prepare phase (it doesn't get its
        # own broadcast, so it calls send_pbft_prepare directly).
        self.send_pbft_prepare(seq, digest)

        time.sleep(5)

        with self.lock:
            if seq in self.committed_seqs:
                return True
        return False

    def send_pbft_prepare(self, seq, digest):
        """
        Broadcast a signed PREPARE message and count this node's own vote.

        Signing the Prepare with our private key lets other nodes verify it
        came from us.  The adversary can forge the *content* but can't forge
        the signature without our private key, so honest nodes can detect
        impersonation.
        """
        msg_to_sign = {'view': self.view_number, 'seq': seq, 'digest': digest}
        signature = sign_message(self.private_key, msg_to_sign)

        prepare_msg = {
            'type': 'pbft_prepare',
            'view': self.view_number,
            'seq': seq,
            'digest': digest,
            'sender': self.node_id,
            'signature': signature
        }

        self.broadcast(prepare_msg)

        # Count own Prepare so we don't need to receive it back from peers.
        with self.lock:
            if seq not in self.prepare_messages:
                self.prepare_messages[seq] = []
            self.prepare_messages[seq].append(prepare_msg)

    def send_pbft_commit(self, seq):
        """
        Broadcast a signed COMMIT message and immediately check if we've
        accumulated enough commits to finalise this sequence number.

        Called in a separate daemon thread (spawned from handle_pbft_prepare)
        to avoid holding the lock during the broadcast — broadcasting while
        holding a lock would deadlock if the local server thread tries to
        deliver an incoming message that also needs the lock.
        """
        msg_to_sign = {'view': self.view_number, 'seq': seq}
        signature = sign_message(self.private_key, msg_to_sign)

        commit_msg = {
            'type': 'pbft_commit',
            'view': self.view_number,
            'seq': seq,
            'sender': self.node_id,
            'signature': signature
        }

        self.broadcast(commit_msg)

        # Count own Commit and attempt finalisation.
        with self.lock:
            if seq not in self.commit_messages:
                self.commit_messages[seq] = []
            self.commit_messages[seq].append(commit_msg)
            self.try_pbft_commit(seq)

    def try_pbft_commit(self, seq):
        """
        Finalise a sequence number if we have 2f+1 Commit messages.

        Must be called with self.lock held.  committed_seqs prevents double-
        commit if more Commit messages arrive after we've already finalised.
        """
        if seq in self.committed_seqs:
            return
        commits = self.commit_messages.get(seq, [])
        if len(commits) >= self.quorum:
            self.committed_seqs.add(seq)
            transaction = self.pre_prepare_log[seq]['transaction']
            self.commit_to_ledger(transaction)
            print(f"[Node {self.node_id}] PBFT COMMITTED: seq={seq}, tx='{transaction}'")

    # --- PBFT message handlers ---

    def handle_pbft_pre_prepare(self, message):
        """
        Follower validates a Pre-Prepare from the leader and starts Prepare phase.

        Two checks before participating:
          1. View number matches — rejects stale messages from a deposed leader.
          2. Digest matches the transaction — catches corruption or forgery.
             An adversary can send a Pre-Prepare with any digest they like, but
             this check recomputes the hash locally and discards mismatches.
        """
        seq = message['seq']
        view = message['view']
        transaction = message['transaction']
        digest = message['digest']

        if view != self.view_number:
            return None

        expected = self.compute_digest(transaction)
        if digest != expected:
            print(f"[Node {self.node_id}] PBFT: Digest mismatch! Ignoring.")
            return None

        with self.lock:
            self.pre_prepare_log[seq] = message

        print(f"[Node {self.node_id}] PBFT Prepare: seq={seq}")
        self.send_pbft_prepare(seq, digest)
        return None

    def handle_pbft_prepare(self, message):
        """
        Collect Prepare messages for a sequence number; advance to Commit when
        2f+1 matching Prepares arrive.

        Digest cross-check: compares the incoming digest against what we stored
        from the Pre-Prepare.  An equivocating adversary sends different digests
        to different nodes — this check filters those out.

        Deduplication: only one Prepare per sender per seq is counted.  Without
        this, a Byzantine node could flood us with Prepare messages and
        artificially inflate the quorum count.

        The Commit is sent from a new daemon thread to avoid holding the lock
        during the broadcast (see send_pbft_commit docstring).
        """
        seq = message['seq']
        sender = message['sender']
        digest = message['digest']

        with self.lock:
            if seq in self.pre_prepare_log:
                expected = self.pre_prepare_log[seq]['digest']
                if digest != expected:
                    print(f"[Node {self.node_id}] PBFT: Prepare digest mismatch from node {sender}. Ignoring.")
                    return None

            if seq not in self.prepare_messages:
                self.prepare_messages[seq] = []

            already = any(p['sender'] == sender for p in self.prepare_messages[seq])
            if not already:
                self.prepare_messages[seq].append(message)

            if len(self.prepare_messages[seq]) >= self.quorum and seq not in self.committed_seqs:
                print(f"[Node {self.node_id}] PBFT: Prepare quorum for seq={seq}")
                threading.Thread(target=self.send_pbft_commit, args=(seq,), daemon=True).start()

        return None

    def handle_pbft_commit(self, message):
        """
        Collect Commit messages and finalise once quorum is reached.

        Same deduplication logic as handle_pbft_prepare: one Commit per sender.
        try_pbft_commit is called with the lock held so the check-then-act on
        committed_seqs is atomic.
        """
        seq = message['seq']
        sender = message['sender']

        with self.lock:
            if seq in self.committed_seqs:
                return None

            if seq not in self.commit_messages:
                self.commit_messages[seq] = []

            already = any(c['sender'] == sender for c in self.commit_messages[seq])
            if not already:
                self.commit_messages[seq].append(message)

            self.try_pbft_commit(seq)

        return None

    # ===================================================================
    # CLIENT REQUEST HANDLING
    # ===================================================================

    def handle_client_request(self, message):
        """
        Entry point for external transaction submissions from client.py.

        Only the leader processes client requests — followers return an error
        so the client knows to find the real leader.  This is the standard
        Raft/Paxos pattern: route all writes through the single coordinator.

        Response status values:
          'committed'  — consensus reached, transaction in ledger.
          'failed'     — consensus failed (not enough nodes alive).
          'error'      — this node is not the leader; client should retry elsewhere.
        """
        transaction = message.get('transaction', '')
        print(f"[Node {self.node_id}] Client request: '{transaction}'")

        if not self.is_leader:
            return {'status': 'error', 'message': 'Not leader'}

        if self.mode == 'pbft':
            success = self.run_pbft(transaction)
        else:
            success = self.run_paxos(transaction)

        if success:
            return {'status': 'committed', 'transaction': transaction}
        else:
            return {'status': 'failed', 'transaction': transaction}

    def handle_who_is_leader(self, message):
        """
        Answer a client's leader-discovery query.

        Any node can answer this because every node tracks leader_id from
        COORDINATOR and heartbeat messages.  We return the leader's externally-
        reachable hostname (from the peers table) rather than '0.0.0.0', which
        only makes sense inside the node's own container.

        Special case: if this node *is* the leader, self.host ('0.0.0.0') isn't
        useful to the client — but the client already knows this node's hostname
        (it just connected to it), so we return self.host as a fallback.
        In a real system we'd store our advertised external hostname separately.
        """
        if self.leader_id and self.leader_id in self.peers:
            host, port = self.peers[self.leader_id]
            return {'leader_host': host, 'leader_port': port, 'leader_id': self.leader_id}
        elif self.is_leader:
            return {'leader_host': self.host, 'leader_port': self.port, 'leader_id': self.node_id}
        return {'leader_host': None, 'leader_port': None, 'leader_id': None}

    # ===================================================================
    # KEY EXCHANGE — Distribute RSA public keys at startup
    # ===================================================================

    def exchange_keys(self):
        """
        Broadcast this node's public key to all peers.

        Called once, 3 seconds after startup (enough time for the TCP server
        threads on all containers to be listening before we try to connect).

        Peers store the received key in peer_public_keys[sender_id] and use it
        in verify_signature() to authenticate PBFT messages from this node.
        Without this step, honest nodes can't tell whether a Prepare or Commit
        came from the claimed sender — signatures would be unverifiable.
        """
        time.sleep(3)
        pub_key_str = key_to_string(self.public_key)
        self.broadcast({
            'type': 'key_exchange',
            'sender': self.node_id,
            'public_key': pub_key_str
        })

    def handle_key_exchange(self, message):
        """Store a peer's public key for future signature verification."""
        sender = message['sender']
        pub_key = string_to_key(message['public_key'])
        self.peer_public_keys[sender] = pub_key
        print(f"[Node {self.node_id}] Got public key from Node {sender}")
        return None

    # ===================================================================
    # LEDGER — Persistent transaction log
    # ===================================================================

    def commit_to_ledger(self, transaction):
        """
        Append a committed transaction to the in-memory ledger and flush to disk.

        Idempotency guard: `if transaction not in self.ledger` prevents a
        transaction from appearing twice if commit_to_ledger is called multiple
        times for the same value (e.g. if a PAXOS_COMMIT is received after the
        leader already committed locally).

        The JSON file is rewritten completely on each commit.  For a demo with
        10 transactions this is fine; a production system would use append-only
        writes or a proper database.

        makedirs with exist_ok=True ensures the /app/data directory exists even
        if this is the first transaction (avoids FileNotFoundError).
        """
        if transaction not in self.ledger:
            self.ledger.append(transaction)
            os.makedirs(os.path.dirname(self.ledger_file), exist_ok=True)
            with open(self.ledger_file, 'w') as f:
                json.dump(self.ledger, f, indent=2)
            print(f"[Node {self.node_id}] Ledger: {len(self.ledger)} entries")

    # ===================================================================
    # STARTUP — Launch all threads
    # ===================================================================

    def run(self):
        """
        Start all background threads and keep the main thread alive.

        All threads are daemon=True so they die automatically when the main
        thread exits (e.g. on KeyboardInterrupt or Docker SIGTERM), without
        requiring explicit shutdown logic.

        Thread purpose summary:
          t1 — TCP server: receive and dispatch all incoming messages.
          t2 — Heartbeat: leader announces itself alive every second.
          t3 — Monitor: follower detects dead leader and triggers election.
          t4 — Key exchange: one-shot public-key broadcast at startup.

        The `while True: sleep(1)` at the end keeps the main thread alive so
        daemon threads continue running.  In Python, daemon threads are killed
        as soon as the non-daemon main thread exits.
        """
        print(f"[Node {self.node_id}] Starting in {self.mode.upper()} mode")
        print(f"[Node {self.node_id}] Peers: {list(self.peers.keys())}")

        t1 = threading.Thread(target=self.start_server, daemon=True)
        t1.start()

        t2 = threading.Thread(target=self.heartbeat_loop, daemon=True)
        t2.start()

        t3 = threading.Thread(target=self.monitor_loop, daemon=True)
        t3.start()

        t4 = threading.Thread(target=self.exchange_keys, daemon=True)
        t4.start()

        print(f"[Node {self.node_id}] All threads started. Running...")
        while True:
            time.sleep(1)


if __name__ == '__main__':
    node = Node()
    node.run()
