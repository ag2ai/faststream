from uuid import uuid4

import pytest


@pytest.fixture()
def queue() -> str:
    return f"DEV.Q{uuid4().hex[:20].upper()}"
