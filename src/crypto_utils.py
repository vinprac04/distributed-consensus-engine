"""
Crypto Utilities — Message signing and verification for node authentication.

Why cryptography in a consensus engine?
  In crash-fault-tolerant protocols (Paxos, Raft) we assume nodes fail by
  stopping — they never send false messages.  But Byzantine protocols (PBFT)
  need to detect *lying* nodes.  Digital signatures let every honest node
  verify that a message genuinely came from the claimed sender and was not
  tampered with in transit.

Algorithm choices:
  - RSA-2048 with PSS padding: widely audited, supported by the `cryptography`
    library's hazmat layer, and strong enough for this demonstration.
  - SHA-256 as the hash function: collision-resistant and fast enough that
    signing latency doesn't dominate the consensus round-trip time.
  - PEM encoding for key transport: a self-describing text format that survives
    JSON serialisation without base64 escaping complications.
"""

import json
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization


def generate_keypair():
    """
    Generate a fresh RSA-2048 key pair for one node.

    Called once per node at startup inside Node.__init__.  The private key
    never leaves the process; the public key is broadcast to all peers via the
    key_exchange message so they can verify this node's future signatures.

    public_exponent=65537:  The standard choice — a Fermat prime that makes
                            modular exponentiation fast while keeping security.
    key_size=2048:          2048-bit modulus gives ~112-bit security, adequate
                            for a demo cluster that isn't storing real assets.
    """
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key


def sign_message(private_key, data):
    """
    Produce a deterministic RSA-PSS signature over a Python dict.

    Steps:
      1. Serialise `data` to JSON with sorted keys.  Sorting is critical:
         Python dicts have insertion-ordered iteration, so two dicts with the
         same key-value pairs but different insertion orders would produce
         different byte strings — and therefore different signatures — without
         sort_keys=True.
      2. Sign the UTF-8 bytes using RSA-PSS (Probabilistic Signature Scheme).
         PSS is preferred over the older PKCS#1 v1.5 padding because it has a
         tight security proof in the random-oracle model.
      3. Return the raw signature bytes as a lowercase hex string so it can
         safely ride inside a JSON message field.

    Returns: hex-encoded signature string (512 chars for RSA-2048).
    """
    msg_bytes = json.dumps(data, sort_keys=True).encode()
    signature = private_key.sign(
        msg_bytes,
        # MGF1 (Mask Generation Function 1) with SHA-256 is the standard MGF
        # for PSS.  MAX_LENGTH salt maximises the security of the scheme.
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256()
    )
    return signature.hex()


def verify_signature(public_key, data, signature_hex):
    """
    Verify that `signature_hex` was produced by the holder of `public_key`
    over `data`.

    The verification process mirrors signing exactly:
      1. Re-serialise `data` with sort_keys=True to get the same byte string.
      2. Convert the hex signature back to raw bytes.
      3. Call public_key.verify() — it raises an exception on failure, returns
         None on success.  We wrap it in try/except and return True/False.

    Returns True if the signature is valid; False if it was forged, corrupted,
    or signed with a different key.  Callers treat False as "reject this message."
    """
    try:
        msg_bytes = json.dumps(data, sort_keys=True).encode()
        public_key.verify(
            bytes.fromhex(signature_hex),
            msg_bytes,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256()
        )
        return True
    except Exception:
        # cryptography raises InvalidSignature (a subclass of Exception) on
        # any mismatch.  We catch broadly so callers don't need to import the
        # specific exception type.
        return False


def key_to_string(public_key):
    """
    Serialise a public key to a PEM-encoded string for network transport.

    PEM (Privacy-Enhanced Mail) wraps the DER-encoded SubjectPublicKeyInfo
    structure in Base64 with -----BEGIN PUBLIC KEY----- / -----END PUBLIC KEY-----
    headers.  The resulting string is safe to embed in a JSON field because it
    contains only printable ASCII characters.

    This is called in Node.exchange_keys() before broadcasting the key to peers.
    """
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()


def string_to_key(pem_string):
    """
    Deserialise a PEM string back into a public key object.

    Called in Node.handle_key_exchange() when a peer's public key arrives.
    The resulting object is stored in Node.peer_public_keys[sender_id] and
    used later by verify_signature() to authenticate that peer's PBFT messages.
    """
    return serialization.load_pem_public_key(pem_string.encode())
