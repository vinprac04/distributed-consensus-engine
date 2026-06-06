"""
Byzantine Adversary Node — a deliberately malicious cluster member.

In Byzantine Fault Tolerance (BFT) research a "Byzantine" node is one that
can deviate from the protocol in *any* way — not just crash, but actively lie,
selectively drop messages, or send contradictory data to different peers.
This class simulates those failures so we can verify that honest nodes still
reach consensus despite one bad actor.

PBFT's safety guarantee: with N=5 nodes and f=1 Byzantine node, the cluster
can still commit correctly because 2f+1 = 3 honest nodes always form a quorum.

Attack modes (set via ATTACK_MODE env var):
  equivocate  — Send a *different* fake digest to every peer (the classic
                Byzantine double-speak: "I told A one thing, B another").
  suppress    — Randomly silently drop Prepare and Commit messages, starving
                the quorum counts on other nodes.
  forge       — Broadcast fabricated Pre-Prepare messages with invalid sequence
                numbers and transactions, trying to pollute honest nodes' logs.

None of these attacks can break a correct PBFT implementation because:
  - Digest mismatches are caught by `compute_digest` on each receiver.
  - Dropped messages just mean the quorum is built from the remaining honest nodes.
  - Forged sequence numbers don't align with any legitimate Pre-Prepare, so they
    fail the digest check and are discarded.
"""

import os
import time
import random
import threading

from src.node import Node
from src.crypto_utils import sign_message


class AdversaryNode(Node):
    def __init__(self):
        # Inherit all state from the honest Node: crypto keys, ledger, peers,
        # Raft election logic, etc.  We only *override* specific message handlers
        # to inject malicious behaviour.
        super().__init__()
        self.attack_mode = os.environ.get('ATTACK_MODE', 'equivocate')
        print(f"[ADVERSARY] *** MALICIOUS NODE ACTIVATED ***")
        print(f"[ADVERSARY] *** Attack mode: {self.attack_mode} ***")

    # -----------------------------------------------------------------------
    # ATTACK 1: Equivocation
    #
    # The classic Byzantine failure — sending *inconsistent* Prepare messages
    # to different peers.  In an honest PBFT round every node sends the same
    # digest for a given (view, seq) pair.  An equivocating node sends a
    # *unique* fake digest to each peer, so no two honest nodes see the same
    # Prepare from this node.
    #
    # Why this can't break PBFT:
    #   Honest nodes require 2f+1 matching (view, seq, digest) Prepare messages
    #   before moving to Commit.  Because this node sends a different digest to
    #   each peer, no honest node can collect a quorum of matching Prepares from
    #   this node's messages alone — they'll simply be ignored as mismatches.
    # -----------------------------------------------------------------------
    def handle_pbft_pre_prepare(self, message):
        if self.attack_mode == 'equivocate':
            seq = message['seq']
            view = message['view']

            print(f"[ADVERSARY] *** EQUIVOCATING on seq={seq} ***")

            # Store the real pre-prepare so our own state stays consistent
            # (the adversary still needs to track sequence numbers).
            with self.lock:
                self.pre_prepare_log[seq] = message

            # Send a *different* fake digest to every peer.
            # FAKE_<peer_id>_<random> ensures each peer sees a unique value —
            # maximising inconsistency across the cluster.
            for peer_id in self.peers:
                fake_digest = f"FAKE_{peer_id}_{random.randint(1000, 9999)}"
                msg_to_sign = {'view': view, 'seq': seq, 'digest': fake_digest}
                # We sign with our real private key so the message passes the
                # sender-authentication check, but the digest itself is garbage.
                signature = sign_message(self.private_key, msg_to_sign)

                evil_msg = {
                    'type': 'pbft_prepare',
                    'view': view,
                    'seq': seq,
                    'digest': fake_digest,
                    'sender': self.node_id,
                    'signature': signature
                }
                self.send_message(peer_id, evil_msg)
                print(f"[ADVERSARY] *** Sent fake digest to node {peer_id} ***")

            return None  # Don't fall through to honest handling
        else:
            # In non-equivocate modes, behave honestly during Pre-Prepare.
            return super().handle_pbft_pre_prepare(message)

    # -----------------------------------------------------------------------
    # ATTACK 2: Message Suppression (Prepare phase)
    #
    # Randomly ignore incoming Prepare messages, as if they were lost in transit.
    # In suppress mode every Prepare is dropped.  Even in other modes, a 50%
    # random drop rate is applied to create intermittent quorum delays.
    #
    # Why this can't break PBFT:
    #   Dropping messages from *one* node doesn't prevent the remaining 4 honest
    #   nodes from exchanging 2f+1 = 3 matching Prepares among themselves.
    #   The cluster slows down but still commits.
    # -----------------------------------------------------------------------
    def handle_pbft_prepare(self, message):
        if self.attack_mode == 'suppress' or random.random() < 0.5:
            print(f"[ADVERSARY] *** DROPPING prepare from node {message['sender']} ***")
            return None  # Silently discard — sender gets no error, no response
        return super().handle_pbft_prepare(message)

    # -----------------------------------------------------------------------
    # ATTACK 3: Commit Suppression
    #
    # Drop 70% of incoming Commit messages regardless of attack mode.
    # This tries to prevent this node from accumulating a Commit quorum,
    # stalling its own state machine.  Honest nodes are unaffected because
    # they receive Commits from the other 4 nodes and still hit quorum (3).
    # -----------------------------------------------------------------------
    def handle_pbft_commit(self, message):
        if random.random() < 0.7:
            print(f"[ADVERSARY] *** DROPPING commit from node {message['sender']} ***")
            return None
        return super().handle_pbft_commit(message)

    # -----------------------------------------------------------------------
    # ATTACK 4: Heartbeat Withholding (Raft/Bully disruption)
    #
    # If this adversary wins the Bully election and becomes leader, it goes
    # silent — it never sends heartbeats.  Honest nodes will timeout (3 s) and
    # trigger a new election, demoting this node.  This simulates a "lying
    # leader" that claims leadership but refuses to do the work.
    #
    # Why this resolves itself:
    #   The monitor_loop in each honest node detects the missing heartbeat and
    #   calls start_election().  With 4 remaining nodes, a new leader is elected
    #   quickly, and the adversary loses its leader status.
    # -----------------------------------------------------------------------
    def heartbeat_loop(self):
        while True:
            if self.is_leader:
                print("[ADVERSARY] *** WITHHOLDING heartbeat ***")
                # Intentionally do nothing — no broadcast, just silence.
                # This will cause honest nodes to time out and re-elect.
            time.sleep(1)

    # -----------------------------------------------------------------------
    # ATTACK 5: Transaction Forgery (forge mode only)
    #
    # Broadcasts fabricated PBFT Pre-Prepare messages with:
    #   - A nonsense sequence number (9999+) that no honest leader ever assigned.
    #   - A fake transaction string.
    #   - A signature computed over *different* data, so the digest check fails.
    #
    # Why this can't pollute the ledger:
    #   Honest nodes call compute_digest(transaction) and compare it to the
    #   digest field in the Pre-Prepare.  Forged messages carry an invalid
    #   signature/digest pair and are silently discarded.
    # -----------------------------------------------------------------------
    def inject_fakes(self):
        """Periodically inject fake transactions (forge mode only)."""
        # Wait 15 s so the cluster has time to stabilise before we attack.
        time.sleep(15)
        while True:
            if self.attack_mode == 'forge':
                fake_tx = f"FORGED_TX_{random.randint(0, 9999)}: Steal $1000000"
                print(f"[ADVERSARY] *** INJECTING: {fake_tx} ***")
                self.broadcast({
                    'type': 'pbft_pre_prepare',
                    'view': self.view_number,
                    # Seq 9999+ will never match any honest Pre-Prepare entry.
                    'seq': 9999 + random.randint(0, 100),
                    'transaction': fake_tx,
                    'digest': self.compute_digest(fake_tx),
                    # Signature is computed over empty/wrong data — digest check
                    # on the receiver side will fail and the message is dropped.
                    'sender': self.node_id,
                    'signature': sign_message(self.private_key, {'view': 0, 'seq': 9999, 'digest': ''})
                })
            time.sleep(10)

    def run(self):
        """
        Start the adversary node with the same thread structure as an honest node,
        plus an optional forge thread.

        Thread layout:
          t1 — TCP server (receives and dispatches incoming messages)
          t2 — Heartbeat loop (overridden to withhold heartbeats if leader)
          t3 — Monitor loop (inherited — still participates in elections so we
               can win and then withhold heartbeats)
          t4 — Key exchange (inherited — we send our real public key so that
               honest nodes can verify our signatures, which makes equivocation
               more realistic: the message authenticates but the content lies)
          t5 — Fake injector (forge mode only)
        """
        print(f"[ADVERSARY] Node {self.node_id} starting ({self.attack_mode} mode)")

        t1 = threading.Thread(target=self.start_server, daemon=True)
        t1.start()

        t2 = threading.Thread(target=self.heartbeat_loop, daemon=True)
        t2.start()

        t3 = threading.Thread(target=self.monitor_loop, daemon=True)
        t3.start()

        t4 = threading.Thread(target=self.exchange_keys, daemon=True)
        t4.start()

        if self.attack_mode == 'forge':
            t5 = threading.Thread(target=self.inject_fakes, daemon=True)
            t5.start()

        print(f"[ADVERSARY] All threads started. Running...")
        while True:
            time.sleep(1)


if __name__ == '__main__':
    adversary = AdversaryNode()
    adversary.run()
