from faststream import Depends, apply_types

def simple_dependency(a: int, b: int = 3) -> int:
    return a + b

@apply_types
def method(a: int, d: int = Depends(simple_dependency)) -> int:
    return a + d

def test_sync_dependency() -> None:
    assert method(1) == 5
