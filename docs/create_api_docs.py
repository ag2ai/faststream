import itertools
from importlib import import_module
from inspect import getmembers, isclass, isfunction
from pathlib import Path
from pkgutil import walk_packages
from types import FunctionType, ModuleType
from typing import Any, List, Optional, Tuple, Type, Union


def _get_submodules(package_name: str) -> List[str]:
    """Get all submodules of a package.

    Args:
        package_name: The name of the package.

    Returns:
        A list of submodules.

    !!! note

        The above docstring is autogenerated by docstring-gen library (https://github.com/airtai/docstring-gen)
    """
    try:
        # nosemgrep: python.lang.security.audit.non-literal-import.non-literal-import
        m = import_module(package_name)
    except ModuleNotFoundError as e:
        raise e
    submodules = [
        info.name for info in walk_packages(m.__path__, prefix=f"{package_name}.")
    ]
    submodules = [
        x
        for x in submodules
        if not any([name.startswith("_") for name in x.split(".")])
    ]
    return [package_name] + submodules


def _import_submodules(module_name: str) -> List[ModuleType]:
    def _import_module(name: str) -> Optional[ModuleType]:
        try:
            # nosemgrep: python.lang.security.audit.non-literal-import.non-literal-import
            return import_module(name)
        except Exception as e:
            return None

    package_names = _get_submodules(module_name)
    modules = [_import_module(n) for n in package_names]
    return [m for m in modules if m is not None]


def _import_functions_and_classes(
    m: ModuleType,
) -> List[Tuple[str, Union[FunctionType, Type[Any]]]]:
    funcs_and_classes = [
        (x, y) for x, y in getmembers(m) if isfunction(y) or isclass(y)
    ]
    if hasattr(m, "__all__"):
        for t in m.__all__:
            obj = getattr(m, t)
            if isfunction(obj) or isclass(obj):
                funcs_and_classes.append((t, m.__name__ + "." + t))

    return funcs_and_classes


def _is_private(name: str) -> bool:
    parts = name.split(".")
    return any([part.startswith("_") for part in parts])


def _import_all_members(module_name: str) -> List[str]:
    submodules = _import_submodules(module_name)
    members: List[Tuple[str, Union[FunctionType, Type[Any]]]] = list(
        itertools.chain(*[_import_functions_and_classes(m) for m in submodules])
    )

    names = [
        y if isinstance(y, str) else f"{y.__module__}.{y.__name__}" for x, y in members
    ]
    names = [
        name for name in names if not _is_private(name) and name.startswith(module_name)
    ]
    return names


def _merge_lists(members: List[str], submodules: List[str]) -> List[str]:
    members_copy = members[:]
    for sm in submodules:
        for i, el in enumerate(members_copy):
            if el.startswith(sm):
                members_copy.insert(i, sm)
                break
    return members_copy


def _add_all_submodules(members: List[str]) -> List[str]:
    def _f(x: str) -> List[str]:
        xs = x.split(".")
        return [".".join(xs[:i]) + "." for i in range(1, len(xs))]

    submodules = list(set(itertools.chain(*[_f(x) for x in members])))
    members = _merge_lists(members, submodules)
    members = list(dict.fromkeys(members))
    return sorted(members)


def _get_api_summary_item(x: str) -> str:
    xs = x.split(".")
    if x.endswith("."):
        indent = " " * (4 * (len(xs) - 1))
        return f"{indent}- {xs[-2]}"
    else:
        indent = " " * (4 * (len(xs)))
        return f"{indent}- [{xs[-1]}](api/{'/'.join(xs)}.md)"


def _get_api_summary(members: List[str]) -> str:
    return "\n".join([_get_api_summary_item(x) for x in members]) + "\n"


def _generate_api_doc(name: str, docs_path: Path) -> Path:
    xs = name.split(".")
    module_name = ".".join(xs[:-1])
    member_name = xs[-1]
    path = docs_path / f"{('/').join(xs)}.md"
    content = f"::: {module_name}.{member_name}\n"

    path.parent.mkdir(exist_ok=True, parents=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

    return path


def _generate_api_docs(members: List[str], docs_path: Path) -> List[Path]:
    return [_generate_api_doc(x, docs_path) for x in members if not x.endswith(".")]


def _get_submodule_members(module_name: str) -> List[str]:
    """Get a list of all submodules contained within the module.

    Args:
        module_name: The name of the module to retrieve submodules from

    Returns:
        A list of submodule names within the module
    """
    members = _import_all_members(module_name)
    members_with_submodules = _add_all_submodules(members)
    members_with_submodules_str: List[str] = [
        x[:-1] if x.endswith(".") else x for x in members_with_submodules
    ]
    return members_with_submodules_str


def _load_submodules(
    module_name: str, members_with_submodules: List[str]
) -> List[Union[FunctionType, Type[Any]]]:
    """Load the given submodules from the module.

    Args:
        module_name: The name of the module whose submodules to load
        members_with_submodules: A list of submodule names to load

    Returns:
        A list of imported submodule objects.
    """
    submodules = _import_submodules(module_name)
    members: List[Tuple[str, Union[FunctionType, Type[Any]]]] = list(
        itertools.chain(*[_import_functions_and_classes(m) for m in submodules])
    )
    names = [
        y
        for x, y in members
        if (isinstance(y, str) and y in members_with_submodules)
        or (f"{y.__module__}.{y.__name__}" in members_with_submodules)
    ]
    return names


def _update_single_api_doc(
    symbol: Union[FunctionType, Type[Any]], docs_path: Path
) -> None:
    en_docs_path = docs_path / "docs" / "en"
    content_to_append = ""

    if isinstance(symbol, str):
        class_name = symbol.split(".")[-1]
        module_name = ".".join(symbol.split(".")[:-1])
        obj = getattr(import_module(module_name), class_name)
        filename = symbol
        content_to_append = "    options:\n      show_root_full_path: false\n"
    else:
        obj = symbol
        filename = f"{symbol.__module__}.{symbol.__name__}"

    content = f"\n\n::: {obj.__module__}.{obj.__qualname__}\n" + content_to_append

    target_file_path = "/".join(filename.split(".")) + ".md"

    with open((en_docs_path / "api" / target_file_path), "w", encoding="utf-8") as f:
        f.write(content)


def _update_api_docs(
    symbols: List[Union[FunctionType, Type[Any]]], docs_path: Path
) -> None:
    for symbol in symbols:
        _update_single_api_doc(symbol=symbol, docs_path=docs_path)


def _generate_api_docs_for_module(root_path: str, module_name: str) -> str:
    """Generate API documentation for a module.

    Args:
        root_path: The root path of the project.
        module_name: The name of the module.

    Returns:
        A string containing the API documentation for the module.

    !!! note

        The above docstring is autogenerated by docstring-gen library (https://github.com/airtai/docstring-gen)
    """
    members = _import_all_members(module_name)
    members_with_submodules = _add_all_submodules(members)
    api_summary = _get_api_summary(members_with_submodules)

    _generate_api_docs(members_with_submodules, Path(root_path) / "docs" / "en" / "api")

    members_with_submodules = _get_submodule_members(module_name)
    symbols = _load_submodules(module_name, members_with_submodules)

    _update_api_docs(symbols, Path(root_path))

    return api_summary


def create_api_docs(
    root_path: str,
    module: str,
):
    api = _generate_api_docs_for_module(root_path, module)

    docs_dir = Path(root_path) / "docs"

    # read summary template from file
    with open(Path(docs_dir) / "summary_template.txt", "r", encoding="utf-8") as f:
        summary_template = f.read()

    summary = summary_template.format(
        api=api,
    )
    summary = "\n".join(
        [l for l in [l.rstrip() for l in summary.split("\n")] if l != ""]
    )

    with open(Path(docs_dir) / "SUMMARY.md", mode="w", encoding="utf-8") as f:
        f.write(summary)
