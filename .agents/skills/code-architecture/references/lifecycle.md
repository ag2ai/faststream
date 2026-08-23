# Lifecycle phases

Every Broker, Subscriber and Publisher moves through four phases in this order. The phase a
method belongs to is not a matter of taste: it follows from what the method is allowed to
read and whether it is allowed to touch the network. Put a new method in the wrong phase and
it either reads a value that is not final yet or blocks a path that is supposed to be
synchronous.

| Phase | Mutates | Reads | I/O |
| --- | --- | --- | --- |
| **1. Composition** | the options composition | declared options only | no |
| **2. Preparation** | the endpoint's derived state | the composition, now final | no |
| **3. Connection** | the connection | resolved values | yes |
| **4. Start** | subscriptions | resolved values | yes |

## 1. Composition

Declaration. Endpoints are constructed, Routers are included, the App attaches its own
`BrokerConfig`. The composition is a live, mutable chain — a Subscriber holds a *reference* to
it, not a copy — so nothing derived from it can be kept yet.

| Method | Note |
| --- | --- |
| `Registrator.subscriber()` / `publisher()` | registers on the Registrator it is called on |
| `Registrator.include_router()` / `include_routers()` | composes the Router's config into this one |
| `Registrator.add_middleware()` / `insert_middleware()` | |
| `BrokerUsecase._update_config()` | how an App attaches its level |
| `ConfigComposition.add_config()` / `reset()` | |
| `Endpoint.__init__`, `SubscriberUsecase.__init__` | stores declared options; derives nothing |

**Invariant.** Nothing is resolved. A read whose answer depends on a Config value or on the
Router prefix raises `IncorrectState` — see `Resolved` in
`faststream/_internal/endpoint/derived.py`. A read of a *declared* value — the handler
collection, the ack policy, `repr` — answers at any moment.

**Adding a method here?** It may read `self._outer_config` for options that are not addresses,
and it must not memoise anything derived from the composition.

## 2. Preparation

The pass that runs once the composition is final and before anything talks to the network.
Synchronous by contract, and safe to drive as often as you like — see *idempotence* below.

| Method | Note |
| --- | --- |
| `BrokerUsecase._prepare()` | endpoints first, then `_setup_logger()`; deliberately unguarded |
| `Endpoint.prepare()` | where the guard is: returns early on `Endpoint._prepared` |
| `Endpoint._prepare()` | the override point; every Broker's Subscriber and Publisher overrides it |
| `SubscriberUsecase._prepare()` | `check_addresses()`, `_build_parser()`, `_build_fastdepends_model()`, `lock = MultiLock()` |
| `SubscriberUsecase._build_parser()` | per Broker; the capture regex is a value by now |
| `BrokerUsecase._setup_logger()` | reads every Subscriber's log context, so it runs last |
| `BrokerUsecase._prepared_for_a_read()` | Preparation scoped to a render |
| `BrokerUsecase._invalidate()` / `Endpoint.invalidate()` | the undo |

**What triggers it.** The list is closed:

- `BrokerUsecase.connect()`
- `StartAbleApplication._start_broker()`, across every Broker before I/O on any
- `AsyncAPI.to_specification()`
- attaching a Publisher to a composition a prepared Broker is the root of
- a Subscriber's own `start()`

These are not exclusive, and more than one fires on an ordinary start-up: an App drives `_start_broker()` and then each Broker's `start()`, which drives `connect()`, which prepares again. Do not add a guard to make that number one — the number that matters is the endpoint's, and it is already one.

**Invariants.**

- *No I/O.* An implementation that needs the network belongs in phase 3 or 4. This is what
  lets an App prepare all its Brokers before connecting any, and lets schema generation run
  with no event loop at all.
- *Resolution happens here and only here.* Afterwards an address is a field. A read before it
  raises rather than answering from an incomplete composition.
- *Preparation belongs to the connection it precedes.* `stop()` undoes it, so a Broker used
  twice resolves twice. The one trigger that opens no connection — a schema render — undoes
  what it performed, and only what it performed.
- *An endpoint prepares when everything Preparation reads is final.* One rule, three moments:
  the Broker's pass for endpoints declared before it, attachment for a Publisher added after
  it, the Subscriber's own `start()` for a Subscriber added after it — because its handlers
  arrive after `broker.subscriber(...)` has returned.
- *Attachment reads the root of the composition,* not the object it lands on. Only a Broker
  ever prepares; a Router answers "not prepared" for the whole of its life.
- *A declaration mistake is refused here,* so no Subscriber is left consuming while another
  fails to start.
- *Idempotence is per endpoint, not per pass.* `Endpoint.prepare()` carries the flag and
  returns early, so the work — resolution, the parser, the FastDepends model, the lock —
  happens exactly once between one invalidation and the next. The Broker's pass around it
  needs no flag of its own because both of its remaining steps settle: registering a log
  context widens a column width through `max()`, and the logger object is built only if there
  is not one. The `_prepared` flag the Broker does keep answers a different question — whether
  an endpoint attached from now on has missed the pass.

**Adding a method here?** Register anything it memoises with `self._derived` at construction
(`DerivedReads`), and it is forgotten on invalidation with no override needed.

## 3. Connection

The first I/O. Opens the connection and nothing else — no subscription is made yet.

| Method | Note |
| --- | --- |
| `BrokerUsecase.connect()` | `_prepare()`, then `_connect()`; guarded on `_connection` |
| `BrokerUsecase._connect()` | per Broker; builds the client and the producer |
| `BrokerUsecase.__aenter__` | `connect()` |
| `BrokerUsecase.ping()` | |

**Invariant.** `connect()` is idempotent through `_connection`, and re-entering it after a
`stop()` prepares again, because `stop()` invalidated.

## 4. Start

The second I/O. Subscriptions are made and messages begin arriving.

| Method | Note |
| --- | --- |
| `<Broker>.start()` | `await self.connect()`, then `super().start()` |
| `BrokerUsecase.start()` | starts every Subscriber, then every Publisher; sets `running` |
| `SubscriberUsecase.start()` | `prepare()` — a no-op unless this Subscriber is a late one — then subscribe |
| `PublisherUsecase.start()` | `prepare()`, for symmetry with the Subscriber |
| `BrokerUsecase.stop()` | stops every Subscriber, clears `running`, then `_invalidate()` |
| `SubscriberUsecase.stop()` | |

**Invariant.** `stop()` invalidates *after* the Subscribers have stopped reading through their
addresses and *before* the next `connect()` derives them again.

## Where the phases are driven from

Four surfaces drive this, and each one is a place to check when a phase boundary moves:

- `faststream/_internal/broker/broker.py` — `connect`, `_prepare`, `_invalidate`,
  `_prepared_for_a_read`, `start`, `stop`
- `faststream/_internal/application.py` — `_start_broker`, which prepares every Broker before
  connecting any and undoes them all if one refuses
- `faststream/specification/asyncapi/factory.py` — `to_specification`, the trigger that opens
  no connection
- `faststream/_internal/testing/broker.py` — `TestBroker`, which connects the whole group
  before any member scans it, and invalidates around its Config values so a second context
  does not reuse the first one's addresses

## Rules the phases rest on

Four rules from elsewhere in the design, restated here because a phase boundary moves the
moment one of them is forgotten:

- **A Config value is fixed at Preparation**, not at each read. What a value object compiles —
  a queue name, a stream's consumer group — it compiles inside itself, and Preparation keeps
  the rebuilt object rather than re-deriving it per read.
- **The Router prefix decorates literal declarations and leaves resolved values alone.** A
  Config value is an address in its own right; prefixing one would silently rewrite what the
  operator configured.
- **The schema shows the Address template, not the Broker address.** Preparation resolving
  early does not change what is rendered — only when it is computed.
- **`IncorrectState` is the refusal**, already exported from `faststream.exceptions` and
  already what the logger raises when asked to log before it was built. A read that came too
  early is the same class of mistake, so it is the same exception.
