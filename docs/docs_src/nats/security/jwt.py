import base64
from pathlib import Path

import nkeys

from faststream.nats import NatsBroker, NatsJWT

JWT_PATH = Path("/run/secrets/nats/user.jwt")
SEED_PATH = Path("/run/secrets/nats/user.nk")


def load_jwt() -> bytes:
    return JWT_PATH.read_bytes().strip()


def sign_nonce(nonce: str) -> bytes:
    key_pair = nkeys.from_seed(SEED_PATH.read_bytes().strip())
    try:
        return base64.b64encode(key_pair.sign(nonce.encode()))
    finally:
        key_pair.wipe()


broker = NatsBroker(security=NatsJWT(load_jwt, sign_nonce))
