import ast
import traceback
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path
from typing import cast


def is_contains_context_name(scip_name: str, name: str) -> bool:
    stack = traceback.extract_stack()
    # Walk out past this frame and the ``TestBroker.__init__`` chain (the
    # abstract base plus the concrete subclass override) to the user frame
    # that opened the ``with`` block, regardless of how many ``__init__``
    # frames sit in between.
    idx = len(stack) - 2
    while idx > 0 and stack[idx].name == "__init__":
        idx -= 1
    frame = stack[idx]

    tree = _read_source_ast(frame.filename)
    node = cast("ast.With | ast.AsyncWith", _find_ast_node(tree, frame.lineno))
    context_calls = _get_withitem_calls(node)

    try:
        pos = context_calls.index(scip_name)
    except ValueError:
        pos = 1

    return name in context_calls[pos:]


@lru_cache
def _read_source_ast(filename: str) -> ast.Module:
    return ast.parse(Path(filename).read_text(encoding="utf-8"))


def _find_ast_node(module: ast.Module, lineno: int | None) -> ast.AST | None:
    if lineno is not None:  # pragma: no branch
        for i in getattr(module, "body", ()):
            if i.lineno == lineno:
                return cast("ast.AST", i)

            r = _find_ast_node(i, lineno)
            if r is not None:
                return r

    return None


def _find_withitems(node: ast.With | ast.AsyncWith) -> Iterator[ast.withitem]:
    if isinstance(node, (ast.With, ast.AsyncWith)):
        yield from node.items

    for i in getattr(node, "body", ()):
        yield from _find_withitems(i)


def _get_withitem_calls(node: ast.With | ast.AsyncWith) -> list[str]:
    return [
        id
        for i in _find_withitems(node)
        if (id := getattr(i.context_expr.func, "id", None))  # type: ignore[attr-defined]
    ]
