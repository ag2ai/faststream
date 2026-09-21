import ast
from fnmatch import fnmatch
from pathlib import Path

from coverage import Coverage

ROOT = Path(__file__).parents[2]
PACKAGE = "docs.docs_src"


def test_every_snippet_is_imported_by_a_test() -> None:
    # a snippet left out on purpose goes to the coverage `omit` list in pyproject.toml,
    # a new one gets a test; this list only shrinks
    assert _untested_snippets() == NOT_TESTED_YET


BROKERS = ("confluent", "kafka", "mqtt", "nats", "rabbit", "redis")

NOT_TESTED_YET = {
    "confluent/security/custom_config.py",
    "getting_started/asyncapi/serve.py",
    "getting_started/multiple_brokers/add_broker.py",
    "index/dependencies_annotated.py",
    "integrations/no_http_frameworks_integrations/aiogram.py",
    "kafka/security/sasl_oauthbearer.py",
    *(
        "getting_started/" + snippet.format(b=broker)
        for broker in BROKERS
        for snippet in (
            "cli/{b}/extra_options.py",
            "cli/{b}/worker_id.py",
            "manual_run/{b}_base_run.py",
            "opentelemetry/{b}_telemetry.py",
            "prometheus/{b}.py",
            "prometheus/{b}_asgi.py",
            "subscription/{b}/dynamic.py",
            "subscription/{b}/dynamic_iter.py",
            "subscription/{b}/msgspec_fields.py",
            "subscription/{b}/msgspec_struct.py",
        )
    ),
    *(
        f"getting_started/lifespan/{broker}/basic.py"
        for broker in BROKERS
        if broker != "mqtt"
    ),
}


def _untested_snippets() -> set[str]:
    snippets = {
        _module_name(path): path
        for path in (ROOT / "docs" / "docs_src").rglob("*.py")
        if path.name != "__init__.py"
    }

    reached: set[str] = set()
    queue = list((ROOT / "tests").rglob("*.py"))
    while queue:
        for name in _referenced_modules(queue.pop()):
            if name in snippets and name not in reached:
                reached.add(name)
                # a snippet that another tested snippet imports runs with it
                queue.append(snippets[name])

    omitted = Coverage(config_file=str(ROOT / "pyproject.toml")).get_option("report:omit")
    assert isinstance(omitted, list)

    return {
        path.relative_to(ROOT / "docs" / "docs_src").as_posix()
        for name, path in snippets.items()
        if name not in reached
        and not any(
            fnmatch(path.relative_to(ROOT).as_posix(), pattern) for pattern in omitted
        )
    }


def _referenced_modules(path: Path) -> set[str]:
    package = _module_name(path).rsplit(".", 1)[0].split(".")
    names: set[str] = set()

    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.ImportFrom):
            base = package[: len(package) - node.level + 1] if node.level else []
            module = ".".join([*base, *([node.module] if node.module else [])])
            names.add(module)
            names.update(f"{module}.{alias.name}" for alias in node.names)

        elif isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)

        # "docs.docs_src.kafka.basic.basic:app", as the CLI tests spell it
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value.startswith(PACKAGE):
                names.add(node.value.partition(":")[0])

    return names


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(ROOT).with_suffix("").parts)
