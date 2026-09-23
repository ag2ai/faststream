import pytest

from faststream.message import StreamMessage


@pytest.mark.asyncio()
async def test_decode_caches_none_result() -> None:
    """StreamMessage.decode() must cache a None decoder result.

    Previously, the cache lookup used ``dict.get(key)`` which returns
    None for missing keys — the same value a decoder may legitimately
    return.  This caused every subsequent ``decode()`` call to re-invoke
    the decoder instead of returning the cached None.
    """
    call_count = 0

    async def decoder_returning_none(_msg: StreamMessage[bytes]) -> None:
        nonlocal call_count
        call_count += 1

    msg: StreamMessage[bytes] = StreamMessage(raw_message=b"", body=b"")
    msg.set_decoder(decoder_returning_none)

    first = await msg.decode()
    second = await msg.decode()

    assert first is None
    assert second is None
    assert call_count == 1, (
        f"Decoder was called {call_count} times; expected exactly 1 (cached None)"
    )


@pytest.mark.asyncio()
async def test_decode_caches_non_none_result() -> None:
    """Regression guard: non-None results must still be cached."""
    call_count = 0
    sentinel = {"key": "value"}

    async def decoder(_msg: StreamMessage[bytes]) -> dict[str, str]:
        nonlocal call_count
        call_count += 1
        return sentinel

    msg: StreamMessage[bytes] = StreamMessage(raw_message=b"", body=b"")
    msg.set_decoder(decoder)

    first = await msg.decode()
    second = await msg.decode()

    assert first is sentinel
    assert second is sentinel
    assert call_count == 1, f"Decoder was called {call_count} times; expected exactly 1"
