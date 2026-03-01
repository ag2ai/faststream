"""Create API documentation for a module."""

import itertools
import shutil
from importlib import import_module
from inspect import getmembers, isclass, isfunction
from pathlib import Path
from pkgutil import walk_packages
from types import FunctionType, ModuleType
from typing import Any, List, Optional, Tuple, Type, Union

BASE_DIR = Path(__file__).resolve().parent
DOCS_DIR = BASE_DIR / "docs"
DOCS_CONTENT_DIR = DOCS_DIR / "en"
API_DIR = DOCS_CONTENT_DIR / "api"
MODULE = "faststream"

API_META = (
    "# 0.5 - API\n"
    "# 2 - Release\n"
    "# 3 - Contributing\n"
    "# 5 - Template Page\n"
    "# 10 - Default\n"
    "search:\n"
    "  boost: 0.5"
)

MD_API_META = "---\n" + API_META + "\n---\n\n"


PUBLIC_API_FILES = [
    "faststream/__init__.py",
    "faststream/nats/__init__.py",
    "faststream/rabbit/__init__.py",
    "faststream/confluent/__init__.py",
    "faststream/kafka/__init__.py",
    "faststream/redis/__init__.py",
    "faststream/asgi/__init__.py",
    "faststream/prometheus/__init__.py",
    "faststream/opentelemetry/__init__.py",
    "faststream/specification/__init__.py",
]


def _get_submodules(package_name: str) -> List[str]:
    """Get all submodules of a package.

    Args:
        package_name: The name of the package.

    Returns:
        A list of submodules.
    """
    try:
        m = import_module(package_name)
    except ModuleNotFoundError as e:
        raise e
    submodules = [
        info.name for info in walk_packages(m.__path__, prefix=f"{package_name}.")
    ]
    submodules = [
        x for x in submodules if not any(name.startswith("_") for name in x.split("."))
    ]
    return [package_name, *submodules]


def _import_submodules(
    module_name: str,
    include_public_api_only: bool = False,
) -> Optional[List[ModuleType]]:
    def _import_module(name: str) -> Optional[ModuleType]:
        try:
            return import_module(name)
        except Exception:
            return None

    package_names = _get_submodules(module_name)
    modules = [_import_module(n) for n in package_names]
    if include_public_api_only:
        repo_path = Path.cwd().parent

        # Extract only faststream/__init__.py or faststream/<something>/__init__.py
        public_api_modules = [
            m
            for m in modules
            if m and m.__file__.replace(str(repo_path) + "/", "") in PUBLIC_API_FILES
        ]

        return public_api_modules
    return [m for m in modules if m is not None]


def _import_functions_and_classes(
    m: ModuleType,
    include_public_api_only: bool = False,
) -> List[Tuple[str, Union[FunctionType, Type[Any]]]]:
    funcs_and_classes = []
    if not include_public_api_only:
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
    return any(part.startswith("_") for part in parts)


def _import_all_members(
    module_name: str,
    include_public_api_only: bool = False,
) -> List[str]:
    submodules = _import_submodules(
        module_name,
        include_public_api_only=include_public_api_only,
    )
    members: List[Tuple[str, Union[FunctionType, Type[Any]]]] = list(
        itertools.chain(
            *[
                _import_functions_and_classes(
                    m,
                    include_public_api_only=include_public_api_only,
                )
                for m in submodules
            ],
        ),
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

    def _get_sorting_key(item: str) -> str:
        y = item.split(".")
        z = [f"~{a}" for a in y[:-1]] + [y[-1]]
        return ".".join(z)

    submodules = list(set(itertools.chain(*[_f(x) for x in members])))
    members = _merge_lists(members, submodules)
    members = list(dict.fromkeys(members))
    return sorted(members, key=_get_sorting_key)


def _get_api_summary_item(x: str) -> str:
    xs = x.split(".")
    if x.endswith("."):
        indent = " " * (4 * (len(xs) - 1 + 1))
        return f"{indent}- {xs[-2]}"
    indent = " " * (4 * (len(xs) + 1))
    return f"{indent}- [{xs[-1]}](api/{'/'.join(xs)}.md)"


def _get_api_summary(members: List[str]) -> str:
    return "\n".join([_get_api_summary_item(x) for x in members])


def _generate_api_doc(name: str, docs_path: Path) -> Path:
    xs = name.split(".")
    module_name = ".".join(xs[:-1])
    member_name = xs[-1]
    path = docs_path / f"{('/').join(xs)}.md"
    content = f"::: {module_name}.{member_name}\n"

    path.parent.mkdir(exist_ok=True, parents=True)
    path.write_text(MD_API_META + content)

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
    module_name: str,
    members_with_submodules: List[str],
) -> List[Union[FunctionType, Type[Any]]]:
    """Load the given submodules from the module.

    Args:
        module_name: The name of the module whose submodules to load
        members_with_submodules: A list of submodule names to load

    Returns:
        A list of imported submodule objects.
    """
    submodules = _import_submodules(module_name)
    members = itertools.chain(*map(_import_functions_and_classes, submodules))

    names: list[Union[FunctionType, Type[Any]]] = []
    for _, y in members:
        if isinstance(y, str):
            name = y
        else:
            name = f"{y.__module__}.{y.__name__}"

        if name in members_with_submodules:
            names.append(y)

    return names


def _update_single_api_doc(
    symbol: Union[FunctionType, Type[Any]],
    docs_path: Path,
    module_name: str,
) -> None:
    if isinstance(symbol, str):
        class_name = symbol.split(".")[-1]
        module_name = ".".join(symbol.split(".")[:-1])
        obj = getattr(import_module(module_name), class_name)
        if obj.__module__.startswith(module_name):
            obj = symbol
        filename = symbol

    else:
        obj = symbol
        filename = f"{symbol.__module__}.{symbol.__name__}"

    content = "::: %s\n" % (
        obj if isinstance(obj, str) else f"{obj.__module__}.{obj.__qualname__}"
    )

    target_file_path = "/".join(filename.split(".")) + ".md"

    (API_DIR / target_file_path).write_text(MD_API_META + content)


def _update_api_docs(
    symbols: List[Union[FunctionType, Type[Any]]],
    docs_path: Path,
    module_name: str,
) -> None:
    for symbol in symbols:
        _update_single_api_doc(
            symbol=symbol,
            docs_path=docs_path,
            module_name=module_name,
        )


def _generate_api_docs_for_module() -> Tuple[str, str]:
    """Generate API documentation for a module.

    Args:
        root_path: The root path of the project.
        module_name: The name of the module.

    Returns:
        A string containing the API documentation for the module.

    """
    public_api_summary = _get_api_summary(
        _add_all_submodules(_import_all_members(MODULE, include_public_api_only=True)),
    )
    # Using public_api/ symlink pointing to api/ because of the issue
    # https://github.com/mkdocs/mkdocs/issues/1974
    public_api_summary = public_api_summary.replace("(api/", "(public_api/")

    members = _import_all_members(MODULE)
    members_with_submodules = _add_all_submodules(members)
    api_summary = _get_api_summary(members_with_submodules)

    API_DIR.mkdir(parents=True, exist_ok=True)
    (API_DIR / ".meta.yml").write_text(API_META)

    _generate_api_docs(members_with_submodules, API_DIR)

    members_with_submodules = _get_submodule_members(MODULE)
    symbols = _load_submodules(MODULE, members_with_submodules)

    _update_api_docs(symbols, API_DIR, MODULE)

    # TODO: fix the problem and remove this
    src = """                    - [ContactDict](api/faststream/asyncapi/schema/info/ContactDict.md)
"""
    dst = """                    - [ContactDict](api/faststream/asyncapi/schema/info/ContactDict.md)
                        - [EmailStr](api/faststream/asyncapi/schema/info/EmailStr.md)
"""
    api_summary = api_summary.replace(src, dst)

    return "    - All API\n" + api_summary, "    - Public API\n" + public_api_summary


def remove_api_dir() -> None:
    shutil.rmtree(API_DIR / MODULE, ignore_errors=True)


def render_navigation(api: str, public_api: str) -> None:
    navigation_template = (DOCS_DIR / "navigation_template.txt").read_text()

    summary = navigation_template.format(
        api=api,
        public_api=public_api,
    )

    summary = "\n".join(filter(bool, (x.rstrip() for x in summary.split("\n"))))
    (DOCS_DIR / "SUMMARY.md").write_text(summary)


def create_api_docs() -> None:
    remove_api_dir()
    api, public_api = _generate_api_docs_for_module()
    render_navigation(api=api, public_api=public_api)


if __name__ == "__main__":
    create_api_docs()
