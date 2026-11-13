import inspect
from collections.abc import Callable
from typing import Any


# an option the broker offers is offered by every level that registers the same
# endpoint (#2871); `Route` and the publisher object are hand-written copies of the
# registrator arguments, so they are the ones that fall behind
def assert_every_level_takes_the_options_the_broker_takes(
    broker: Any,
    router: Any,
    route: Any,
    publisher: Any,
) -> None:
    assert {
        "Router.subscriber": _lost(broker.subscriber, router.subscriber),
        "Router.publisher": _lost(broker.publisher, router.publisher),
        "Route": _lost(broker.subscriber, route),
        "Publisher": _lost(broker.publisher, publisher),
    } == {
        "Router.subscriber": [],
        "Router.publisher": [],
        "Route": [],
        "Publisher": [],
    }


def _lost(offered: Callable[..., Any], accepted: Callable[..., Any]) -> list[str]:
    return sorted(_options(offered) - _options(accepted))


def _options(obj: Callable[..., Any]) -> set[str]:
    # `*args` carries the destination, which each level spells its own way
    return {
        parameter.name
        for parameter in inspect.signature(obj).parameters.values()
        if parameter.kind in {parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY}
        and parameter.name != "self"
    }
