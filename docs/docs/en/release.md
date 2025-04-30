---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 2
hide:
  - navigation
  - footer
---

# Release Notes
## 0.5.36

### What's Changed

* fix #2088: respect parsed sasl_mechanism by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2092](https://github.com/airtai/faststream/pull/2092){.external-link target="_blank"}
* Add delay to unassigned consumer warning by @Arseniy-Popov in [#2090](https://github.com/airtai/faststream/pull/2090){.external-link target="_blank"}
* fix: resolve forward refs in FastAPI integration by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2096](https://github.com/airtai/faststream/pull/2096){.external-link target="_blank"}
* fix: unwrap handler function when getting dependant by @Yakov-Varnaev in [#2093](https://github.com/airtai/faststream/pull/2093){.external-link target="_blank"}
* fix: make `CriticalLogMiddleware` respect broker log level (#2130) by [@fadedDexofan](https://github.com/fadedDexofan){.external-link target="_blank"} in [#2131](https://github.com/airtai/faststream/pull/2131){.external-link target="_blank"}
* Fixed nested dataclasses schema generation by [@NelsonNotes](https://github.com/NelsonNotes){.external-link target="_blank"} in [#2097](https://github.com/airtai/faststream/pull/2097){.external-link target="_blank"}
* refactoring ConfluentConfig by [@diskream](https://github.com/diskream){.external-link target="_blank"} in [#2098](https://github.com/airtai/faststream/pull/2098){.external-link target="_blank"}
* refactored subscription Annotation Serialization by [@MaximGit1](https://github.com/MaximGit1){.external-link target="_blank"} in [#2112](https://github.com/airtai/faststream/pull/2112){.external-link target="_blank"}
* docs: added pros and cons to publishers page by [@0xWEBMILK](https://github.com/0xWEBMILK){.external-link target="_blank"} in [#2103](https://github.com/airtai/faststream/pull/2103){.external-link target="_blank"}
* docs: follow off logging recommendations by [@guitvcer](https://github.com/guitvcer){.external-link target="_blank"} in [#2127](https://github.com/airtai/faststream/pull/2127){.external-link target="_blank"}
* docs: add example for probes by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#2108](https://github.com/airtai/faststream/pull/2108){.external-link target="_blank"}
* docs: fix footer after gurubase integration by [@Rusich90](https://github.com/Rusich90){.external-link target="_blank"} in [#2120](https://github.com/airtai/faststream/pull/2120){.external-link target="_blank"}
* docs: Toggle Gurubase widget theme by [@bshelkhonov](https://github.com/bshelkhonov){.external-link target="_blank"} in [#2117](https://github.com/airtai/faststream/pull/2117){.external-link target="_blank"}
* docs(#2109): Inlines the include files directly into the location where it is used(faststream.md, scheduling.md) by [@MaestroXXXVIII](https://github.com/MaestroXXXVIII){.external-link target="_blank"} in [#2113](https://github.com/airtai/faststream/pull/2113){.external-link target="_blank"}
* docs(#2109): "getting-started/subscription" section has been updated by [@MaximGit1](https://github.com/MaximGit1){.external-link target="_blank"} in [#2114](https://github.com/airtai/faststream/pull/2114){.external-link target="_blank"}
* docs(#2109): make PR reference by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2111](https://github.com/airtai/faststream/pull/2111){.external-link target="_blank"}
* docs(#2109): inline includes getting-started/lifespan by [@Sabfo](https://github.com/Sabfo){.external-link target="_blank"} in [#2115](https://github.com/airtai/faststream/pull/2115){.external-link target="_blank"}
* docs(#2109): inline includes getting-started/dependencies by [@Sabfo](https://github.com/Sabfo){.external-link target="_blank"} in [#2116](https://github.com/airtai/faststream/pull/2116){.external-link target="_blank"}
* docs(#2109): inline includes getting-started/serialization/decoder.md by @s-v-y-a-t in [#2118](https://github.com/airtai/faststream/pull/2118){.external-link target="_blank"}
* docs(#2109): deleted include code in getting_started/index by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2119](https://github.com/airtai/faststream/pull/2119){.external-link target="_blank"}
* docs(#2109): Docs/include inline by @close2code-palm in [#2123](https://github.com/airtai/faststream/pull/2123){.external-link target="_blank"}
* docs(#2109): deleted include code in getting_started/opentelemetry by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2125](https://github.com/airtai/faststream/pull/2125){.external-link target="_blank"}
* docs(#2109): deleted include code in getting_started/prometheus by [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} in [#2124](https://github.com/airtai/faststream/pull/2124){.external-link target="_blank"}
* docs(#2109): inline includes getting-started/integrations by [@TurtleOld](https://github.com/TurtleOld){.external-link target="_blank"} in [#2121](https://github.com/airtai/faststream/pull/2121){.external-link target="_blank"}
* docs(#2109): remove docs includes context, serialization, publishing by [@TurtleOld](https://github.com/TurtleOld){.external-link target="_blank"} in [#2126](https://github.com/airtai/faststream/pull/2126){.external-link target="_blank"}

### New Contributors
* [@NelsonNotes](https://github.com/NelsonNotes){.external-link target="_blank"} made their first contribution in [#2097](https://github.com/airtai/faststream/pull/2097){.external-link target="_blank"}
* [@diskream](https://github.com/diskream){.external-link target="_blank"} made their first contribution in [#2098](https://github.com/airtai/faststream/pull/2098){.external-link target="_blank"}
* [@MaximGit1](https://github.com/MaximGit1){.external-link target="_blank"} made their first contribution in [#2112](https://github.com/airtai/faststream/pull/2112){.external-link target="_blank"}
* [@MaestroXXXVIII](https://github.com/MaestroXXXVIII){.external-link target="_blank"} made their first contribution in [#2113](https://github.com/airtai/faststream/pull/2113){.external-link target="_blank"}
* [@Sabfo](https://github.com/Sabfo){.external-link target="_blank"} made their first contribution in [#2115](https://github.com/airtai/faststream/pull/2115){.external-link target="_blank"}
* @s-v-y-a-t made their first contribution in [#2118](https://github.com/airtai/faststream/pull/2118){.external-link target="_blank"}
* [@bshelkhonov](https://github.com/bshelkhonov){.external-link target="_blank"} made their first contribution in [#2117](https://github.com/airtai/faststream/pull/2117){.external-link target="_blank"}
* [@RenameMe1](https://github.com/RenameMe1){.external-link target="_blank"} made their first contribution in [#2119](https://github.com/airtai/faststream/pull/2119){.external-link target="_blank"}
* @close2code-palm made their first contribution in [#2123](https://github.com/airtai/faststream/pull/2123){.external-link target="_blank"}
* [@TurtleOld](https://github.com/TurtleOld){.external-link target="_blank"} made their first contribution in [#2121](https://github.com/airtai/faststream/pull/2121){.external-link target="_blank"}
* [@guitvcer](https://github.com/guitvcer){.external-link target="_blank"} made their first contribution in [#2127](https://github.com/airtai/faststream/pull/2127){.external-link target="_blank"}
* [@fadedDexofan](https://github.com/fadedDexofan){.external-link target="_blank"} made their first contribution in [#2131](https://github.com/airtai/faststream/pull/2131){.external-link target="_blank"}

**Full Changelog**: [#0.5.35...0.5.36](https://github.com/airtai/faststream/compare/0.5.35...0.5.36){.external-link target="_blank"}

## 0.5.35

### What's Changed

* Add concurrent-between-partitions kafka subscriber by @Arseniy-Popov in [#2017](https://github.com/airtai/faststream/pull/2017){.external-link target="_blank"}
* chore: make uv sync working by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2041](https://github.com/airtai/faststream/pull/2041){.external-link target="_blank"}
* docs: add Ask AI button with Gurubase widget by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2042](https://github.com/airtai/faststream/pull/2042){.external-link target="_blank"}
* docs: create FastStream image shield by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2062](https://github.com/airtai/faststream/pull/2062){.external-link target="_blank"}
* Close #2060 by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#2063](https://github.com/airtai/faststream/pull/2063){.external-link target="_blank"}
* feat: allow broker setting in on_startup hook by @Yakov-Varnaev in [#2073](https://github.com/airtai/faststream/pull/2073){.external-link target="_blank"}
* ImportError raises change by [@0xWEBMILK](https://github.com/0xWEBMILK){.external-link target="_blank"} in [#2080](https://github.com/airtai/faststream/pull/2080){.external-link target="_blank"}
* docs: Fix distributed spelling error in task scheduling page by [@Sandldan](https://github.com/Sandldan){.external-link target="_blank"} in [#2081](https://github.com/airtai/faststream/pull/2081){.external-link target="_blank"}
* Exclude confluent-kafka 2.8.1 by @Yakov-Varnaev in [#2084](https://github.com/airtai/faststream/pull/2084){.external-link target="_blank"}

### New Contributors
* @Arseniy-Popov made their first contribution in [#2017](https://github.com/airtai/faststream/pull/2017){.external-link target="_blank"}
* @Yakov-Varnaev made their first contribution in [#2073](https://github.com/airtai/faststream/pull/2073){.external-link target="_blank"}
* [@0xWEBMILK](https://github.com/0xWEBMILK){.external-link target="_blank"} made their first contribution in [#2080](https://github.com/airtai/faststream/pull/2080){.external-link target="_blank"}
* [@Sandldan](https://github.com/Sandldan){.external-link target="_blank"} made their first contribution in [#2081](https://github.com/airtai/faststream/pull/2081){.external-link target="_blank"}

**Full Changelog**: [#0.5.34...0.5.35](https://github.com/airtai/faststream/compare/0.5.34...0.5.35){.external-link target="_blank"}

## 0.5.34

### What's Changed

* fix: when / present in virtual host name and passing as uri by [@pepellsd](https://github.com/pepellsd){.external-link target="_blank"} in [#1979](https://github.com/airtai/faststream/pull/1979){.external-link target="_blank"}
* fix (#2013): allow to create publisher in already connected broker by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#2024](https://github.com/airtai/faststream/pull/2024){.external-link target="_blank"}
* feat: add BatchBufferOverflowException by [@spataphore1337](https://github.com/spataphore1337){.external-link target="_blank"} in [#1990](https://github.com/airtai/faststream/pull/1990){.external-link target="_blank"}
* feat: add static instrumentation info by [@draincoder](https://github.com/draincoder){.external-link target="_blank"} in [#1996](https://github.com/airtai/faststream/pull/1996){.external-link target="_blank"}
* docs: remove reference of "faststream.access" by [@rishabhc32](https://github.com/rishabhc32){.external-link target="_blank"} in [#1995](https://github.com/airtai/faststream/pull/1995){.external-link target="_blank"}
* docs: fixed typo in publishing/test.md by [@AlexPetul](https://github.com/AlexPetul){.external-link target="_blank"} in [#2009](https://github.com/airtai/faststream/pull/2009){.external-link target="_blank"}
* docs: ability to declare queue/exchange binding by [@MagicAbdel](https://github.com/MagicAbdel){.external-link target="_blank"} in [#2011](https://github.com/airtai/faststream/pull/2011){.external-link target="_blank"}
* docs: fix spelling mistake of `/health` by [@herotomg](https://github.com/herotomg){.external-link target="_blank"} in [#2023](https://github.com/airtai/faststream/pull/2023){.external-link target="_blank"}
* docs: update aio-pika external docs URL as it has been moved by [@HybridBit](https://github.com/HybridBit){.external-link target="_blank"} in [#1984](https://github.com/airtai/faststream/pull/1984){.external-link target="_blank"}
* refactor: add type annotations for RabbitQueue and enum for queue type by [@pepellsd](https://github.com/pepellsd){.external-link target="_blank"} in [#2002](https://github.com/airtai/faststream/pull/2002){.external-link target="_blank"}

### New Contributors
* [@HybridBit](https://github.com/HybridBit){.external-link target="_blank"} made their first contribution in [#1984](https://github.com/airtai/faststream/pull/1984){.external-link target="_blank"}
* [@rishabhc32](https://github.com/rishabhc32){.external-link target="_blank"} made their first contribution in [#1995](https://github.com/airtai/faststream/pull/1995){.external-link target="_blank"}
* [@AlexPetul](https://github.com/AlexPetul){.external-link target="_blank"} made their first contribution in [#2009](https://github.com/airtai/faststream/pull/2009){.external-link target="_blank"}
* [@MagicAbdel](https://github.com/MagicAbdel){.external-link target="_blank"} made their first contribution in [#2011](https://github.com/airtai/faststream/pull/2011){.external-link target="_blank"}
* [@herotomg](https://github.com/herotomg){.external-link target="_blank"} made their first contribution in [#2023](https://github.com/airtai/faststream/pull/2023){.external-link target="_blank"}

**Full Changelog**: [#0.5.33...0.5.34](https://github.com/airtai/faststream/compare/0.5.33...0.5.34){.external-link target="_blank"}

## 0.5.33

### What's Changed

Just a Confluent & Kafka hotfix. Messages without body (with key only) parsing correctly now.

* fix: Confluent, read messages under lock by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1963](https://github.com/airtai/faststream/pull/1963){.external-link target="_blank"}
* fix #1967: correct empty kafka message body processing by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1968](https://github.com/airtai/faststream/pull/1968){.external-link target="_blank"}

**Full Changelog**: [#0.5.32...0.5.33](https://github.com/airtai/faststream/compare/0.5.32...0.5.33){.external-link target="_blank"}

## 0.5.32

### What's Changed

Thanks to [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} one more time for a new release! Now you have an ability to consume Confluent messages (in autocommit mode) concurrently!

```python
from faststream.confluent import KafkaBroker

broker = KafkaBroker()

@broker.subscriber("topic", max_workers=10)
async def handler():
    """Using `max_workers` option you can process up to 10 messages by one subscriber concurrently"""
```

Also, thanks to [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} for his ASGI CLI support bugfixes

* fix #1959: propagate logger to Confluent by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1960](https://github.com/airtai/faststream/pull/1960){.external-link target="_blank"}
* Concurrent confluent kafka by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#1961](https://github.com/airtai/faststream/pull/1961){.external-link target="_blank"}
* fix: extend validation for --factory param by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1964](https://github.com/airtai/faststream/pull/1964){.external-link target="_blank"}
* fix: support only uvicorn ASGI Runner by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1965](https://github.com/airtai/faststream/pull/1965){.external-link target="_blank"}

**Full Changelog**: [#0.5.31...0.5.32](https://github.com/airtai/faststream/compare/0.5.31...0.5.32){.external-link target="_blank"}

## 0.5.31

### What's Changed

Well, you (community) made a new breathtaken release for us!
Thanks to all of this release contributors.

Special thanks to [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"}. He promores a new perfect feature - concurrent Kafka subscriber (with autocommit mode)

```python
from faststream.kafka import KafkaBroker

broker = KafkaBroker()

@broker.subscriber("topic", max_workers=10)
async def handler():
    """Using `max_workers` option you can process up to 10 messages by one subscriber concurrently"""
```

Also, thanks to [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} with his ASGI CLI start fixins - now you can use FastStream CLI to scale your AsgiFastStream application by workers

```bash
faststream run main:asgi --workers 2
```

There are a lot of other incredible changes you made:

* feat: add NatsMessage ack_sync method #1906 by [@wpn10](https://github.com/wpn10){.external-link target="_blank"} in [#1909](https://github.com/airtai/faststream/pull/1909){.external-link target="_blank"}
* feat: support running ASGI app with Uvicorn using file descriptor by [@minhyeoky](https://github.com/minhyeoky){.external-link target="_blank"} in [#1923](https://github.com/airtai/faststream/pull/1923){.external-link target="_blank"}
* feat: Add kafka concurrent subscriber by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#1912](https://github.com/airtai/faststream/pull/1912){.external-link target="_blank"}
* fix: bug when using one register for several middleware by @roma-frolov in [#1921](https://github.com/airtai/faststream/pull/1921){.external-link target="_blank"}
* fix: change oauth type in asyncapi schema by [@spataphore1337](https://github.com/spataphore1337){.external-link target="_blank"} in [#1926](https://github.com/airtai/faststream/pull/1926){.external-link target="_blank"}
* fix: HandlerException ignored by @roma-frolov in [#1928](https://github.com/airtai/faststream/pull/1928){.external-link target="_blank"}
* fix: Pomo/nats router by [@Drakorgaur](https://github.com/Drakorgaur){.external-link target="_blank"} in [#1932](https://github.com/airtai/faststream/pull/1932){.external-link target="_blank"}
* fix: RabbitBroker's ping is more objective by @roma-frolov in [#1933](https://github.com/airtai/faststream/pull/1933){.external-link target="_blank"}
* fix: AsyncAPI 2.6.0 fix empty channels for KafkaSubscriber and ConfluentSubscriber if partitions provided by [@KrySeyt](https://github.com/KrySeyt){.external-link target="_blank"} in [#1930](https://github.com/airtai/faststream/pull/1930){.external-link target="_blank"}
* fix: #1874 support workers for ASGI FastStream by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1936](https://github.com/airtai/faststream/pull/1936){.external-link target="_blank"}
* fix: correct middlewares order by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [#1935](https://github.com/airtai/faststream/pull/1935){.external-link target="_blank"}
* chore: run PR altering automated check in same CI job by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1942](https://github.com/airtai/faststream/pull/1942){.external-link target="_blank"}
* chore: pin typer version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1947](https://github.com/airtai/faststream/pull/1947){.external-link target="_blank"}

### New Contributors

* [@wpn10](https://github.com/wpn10){.external-link target="_blank"} made their first contribution in [#1909](https://github.com/airtai/faststream/pull/1909){.external-link target="_blank"}
* [@minhyeoky](https://github.com/minhyeoky){.external-link target="_blank"} made their first contribution in [#1923](https://github.com/airtai/faststream/pull/1923){.external-link target="_blank"}

**Full Changelog**: [#0.5.30...0.5.31](https://github.com/airtai/faststream/compare/0.5.30...0.5.31){.external-link target="_blank"}

## 0.5.30

### What's Changed
* Introducing FastStream Guru on Gurubase.io by [@kursataktas](https://github.com/kursataktas){.external-link target="_blank"} in [#1903](https://github.com/airtai/faststream/pull/1903){.external-link target="_blank"}
* docs: add gurubase badge to the doc by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1905](https://github.com/airtai/faststream/pull/1905){.external-link target="_blank"}
* fix: allow users to pass `nkeys_seed_str` as argument for NATS broker. by [@Drakorgaur](https://github.com/Drakorgaur){.external-link target="_blank"} in [#1908](https://github.com/airtai/faststream/pull/1908){.external-link target="_blank"}
* Add more warning's to nats subscription factory by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [#1907](https://github.com/airtai/faststream/pull/1907){.external-link target="_blank"}
* fix: correct working with dependencies versions by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1918](https://github.com/airtai/faststream/pull/1918){.external-link target="_blank"}

### New Contributors
* [@kursataktas](https://github.com/kursataktas){.external-link target="_blank"} made their first contribution in [#1903](https://github.com/airtai/faststream/pull/1903){.external-link target="_blank"}
* [@Drakorgaur](https://github.com/Drakorgaur){.external-link target="_blank"} made their first contribution in [#1908](https://github.com/airtai/faststream/pull/1908){.external-link target="_blank"}

**Full Changelog**: [#0.5.29...0.5.30](https://github.com/airtai/faststream/compare/0.5.29...0.5.30){.external-link target="_blank"}

## 0.5.29

### What's Changed

* feat: add explicit message source enum by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1866](https://github.com/airtai/faststream/pull/1866){.external-link target="_blank"}
* Change uv manual installation to setup-uv in CI by [@pavelepanov](https://github.com/pavelepanov){.external-link target="_blank"} in [#1871](https://github.com/airtai/faststream/pull/1871){.external-link target="_blank"}
* refactor: make Task and Concurrent mixins broker-agnostic by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1873](https://github.com/airtai/faststream/pull/1873){.external-link target="_blank"}
* Add support for environment variables in faststream run command by [@ulbwa](https://github.com/ulbwa){.external-link target="_blank"} in [#1876](https://github.com/airtai/faststream/pull/1876){.external-link target="_blank"}
* fastapi example update by [@xodiumx](https://github.com/xodiumx){.external-link target="_blank"} in [#1875](https://github.com/airtai/faststream/pull/1875){.external-link target="_blank"}
* Do not import `fake_context` if not needed by [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} in [#1877](https://github.com/airtai/faststream/pull/1877){.external-link target="_blank"}
* build: add warning about manual lifespan_context by [@vectorvp](https://github.com/vectorvp){.external-link target="_blank"} in [#1878](https://github.com/airtai/faststream/pull/1878){.external-link target="_blank"}
* Add trending badge by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1882](https://github.com/airtai/faststream/pull/1882){.external-link target="_blank"}
* feat: add class method to create a baggage instance from headers by [@vectorvp](https://github.com/vectorvp){.external-link target="_blank"} in [#1885](https://github.com/airtai/faststream/pull/1885){.external-link target="_blank"}
* ops: update docker compose commands to compose V2 in scripts by [@vectorvp](https://github.com/vectorvp){.external-link target="_blank"} in [#1889](https://github.com/airtai/faststream/pull/1889){.external-link target="_blank"}

### New Contributors
* [@pavelepanov](https://github.com/pavelepanov){.external-link target="_blank"} made their first contribution in [#1871](https://github.com/airtai/faststream/pull/1871){.external-link target="_blank"}
* [@xodiumx](https://github.com/xodiumx){.external-link target="_blank"} made their first contribution in [#1875](https://github.com/airtai/faststream/pull/1875){.external-link target="_blank"}
* [@sobolevn](https://github.com/sobolevn){.external-link target="_blank"} made their first contribution in [#1877](https://github.com/airtai/faststream/pull/1877){.external-link target="_blank"}
* [@vectorvp](https://github.com/vectorvp){.external-link target="_blank"} made their first contribution in [#1878](https://github.com/airtai/faststream/pull/1878){.external-link target="_blank"}

**Full Changelog**: [#0.5.28...0.5.29](https://github.com/airtai/faststream/compare/0.5.28...0.5.29){.external-link target="_blank"}

## 0.5.28

### What's Changed

There were a lot of time since [**0.5.7 OpenTelemetry** release](https://github.com/airtai/faststream/releases/tag/0.5.7) and now we completed **Observability** features we planned! **FastStream** supports **Prometheus** metrics in a native way!

Special thanks to @roma-frolov and @draincoder (again) for it!

To collect **Prometheus** metrics for your **FastStream** application you just need to install special distribution

```cmd
pip install faststream[prometheus]
```

And use **PrometheusMiddleware**. Also, it could be helpful to use our [**ASGI**](https://faststream.airt.ai/latest/getting-started/asgi/) to serve metrics endpoint in the same app.

```python
from prometheus_client import CollectorRegistry, make_asgi_app
from faststream.asgi import AsgiFastStream
from faststream.nats import NatsBroker
from faststream.nats.prometheus import NatsPrometheusMiddleware

registry = CollectorRegistry()

broker = NatsBroker(
    middlewares=(
        NatsPrometheusMiddleware(registry=registry),
    )
)

app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", make_asgi_app(registry)),
    ]
)
```

Moreover, we have a ready-to-use [**Grafana** dashboard](https://grafana.com/grafana/dashboards/22130-faststream-metrics/) you can just import and use!

To find more information about **Prometheus** support, just visit [our documentation](https://faststream.airt.ai/latest/getting-started/prometheus/).

### All changes

* docs: Correct minimum FastAPI version for lifespan handling by @tim-hutchinson in https://github.com/airtai/faststream/pull/1853
* add aiogram example by @IvanKirpichnikov in https://github.com/airtai/faststream/pull/1858
* Feature: Prometheus Middleware by @roma-frolov in https://github.com/airtai/faststream/pull/1791
* Add in-progress tutorial to how-to section by @sheldygg in https://github.com/airtai/faststream/pull/1859
* docs: Add info about Grafana dashboard by @draincoder in https://github.com/airtai/faststream/pull/1863

### New Contributors

* @tim-hutchinson made their first contribution in https://github.com/airtai/faststream/pull/1853

**Full Changelog**: https://github.com/airtai/faststream/compare/0.5.27...0.5.28

## 0.5.27

### What's Changed

* fix: anyio major version parser by [@dotX12](https://github.com/dotX12){.external-link target="_blank"} in [#1850](https://github.com/airtai/faststream/pull/1850){.external-link target="_blank"}

### New Contributors
* [@dotX12](https://github.com/dotX12){.external-link target="_blank"} made their first contribution in [#1850](https://github.com/airtai/faststream/pull/1850){.external-link target="_blank"}

**Full Changelog**: [#0.5.26...0.5.27](https://github.com/airtai/faststream/compare/0.5.26...0.5.27){.external-link target="_blank"}

## 0.5.26

### What's Changed

This it the official **Python 3.13** support! Now, **FastStream** works (and tested) at **Python 3.8 - 3.13** versions!

Warning: **Python3.8** is EOF since **3.13** release and we plan to drop it support in **FastStream 0.6.0** version.

Also, current release has little bugfixes related to **CLI** and **AsyncAPI** schema.

* fix: asgi docs by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1828](https://github.com/airtai/faststream/pull/1828){.external-link target="_blank"}
* docs: add link to RU TG community by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1831](https://github.com/airtai/faststream/pull/1831){.external-link target="_blank"}
* docs: add dynaconf NATS HowTo example by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [#1832](https://github.com/airtai/faststream/pull/1832){.external-link target="_blank"}
* Fix AsyncAPI 2.6.0 operation label by [@KrySeyt](https://github.com/KrySeyt){.external-link target="_blank"} in [#1835](https://github.com/airtai/faststream/pull/1835){.external-link target="_blank"}
* fix: correct CLI factory behavior by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1838](https://github.com/airtai/faststream/pull/1838){.external-link target="_blank"}
* Autocommit precommit changes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1840](https://github.com/airtai/faststream/pull/1840){.external-link target="_blank"}
* Add devcontainers supporting all the brokers by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1839](https://github.com/airtai/faststream/pull/1839){.external-link target="_blank"}
* Replace async Event with bool by [@Olegt0rr](https://github.com/Olegt0rr){.external-link target="_blank"} in [#1846](https://github.com/airtai/faststream/pull/1846){.external-link target="_blank"}
* Add support for Python 3.13 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1845](https://github.com/airtai/faststream/pull/1845){.external-link target="_blank"}

**Full Changelog**: [#0.5.25...0.5.26](https://github.com/airtai/faststream/compare/0.5.25...0.5.26){.external-link target="_blank"}

## 0.5.25

### What's Changed

* fix: CLI hotfix by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1816](https://github.com/airtai/faststream/pull/1816){.external-link target="_blank"}

**Full Changelog**: [#0.5.24...0.5.25](https://github.com/airtai/faststream/compare/0.5.24...0.5.25){.external-link target="_blank"}

## 0.5.24

### What's Changed

* Replace while-sleep with Event by [@Olegt0rr](https://github.com/Olegt0rr){.external-link target="_blank"} in [#1683](https://github.com/airtai/faststream/pull/1683){.external-link target="_blank"}
* feat: add explicit CLI import error by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1785](https://github.com/airtai/faststream/pull/1785){.external-link target="_blank"}
* fix (#1780): replace / in generated json refs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1786](https://github.com/airtai/faststream/pull/1786){.external-link target="_blank"}
* Fix: this commit resolve #1765 by [@Flosckow](https://github.com/Flosckow){.external-link target="_blank"} in [#1789](https://github.com/airtai/faststream/pull/1789){.external-link target="_blank"}
* fix (#1792): make RMQ publisher.publish reply_to optional by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1795](https://github.com/airtai/faststream/pull/1795){.external-link target="_blank"}
* fix (#1793): FastStream Response support in FastAPI integration by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1796](https://github.com/airtai/faststream/pull/1796){.external-link target="_blank"}
* Update exception.md by [@pepellsd](https://github.com/pepellsd){.external-link target="_blank"} in [#1803](https://github.com/airtai/faststream/pull/1803){.external-link target="_blank"}
* Fixes the CI bug that allows PRs with failed tests to be merged. by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1807](https://github.com/airtai/faststream/pull/1807){.external-link target="_blank"}
* feat: add CLI support for AsgiFastStream by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1782](https://github.com/airtai/faststream/pull/1782){.external-link target="_blank"}
* docs: add contributors page by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1808](https://github.com/airtai/faststream/pull/1808){.external-link target="_blank"}
* fix: correct dependency injection of custom context fields implementing partial __eq__/__ne__ by [@antoinehumbert](https://github.com/antoinehumbert){.external-link target="_blank"} in [#1809](https://github.com/airtai/faststream/pull/1809){.external-link target="_blank"}
* do not assume discriminator is not a property by @lecko-cngroup in [#1811](https://github.com/airtai/faststream/pull/1811){.external-link target="_blank"}

### New Contributors

* [@Olegt0rr](https://github.com/Olegt0rr){.external-link target="_blank"} made their first contribution in [#1683](https://github.com/airtai/faststream/pull/1683){.external-link target="_blank"}
* [@pepellsd](https://github.com/pepellsd){.external-link target="_blank"} made their first contribution in [#1803](https://github.com/airtai/faststream/pull/1803){.external-link target="_blank"}
* [@antoinehumbert](https://github.com/antoinehumbert){.external-link target="_blank"} made their first contribution in [#1809](https://github.com/airtai/faststream/pull/1809){.external-link target="_blank"}
* @lecko-cngroup made their first contribution in [#1811](https://github.com/airtai/faststream/pull/1811){.external-link target="_blank"}

**Full Changelog**: [#0.5.23...0.5.24](https://github.com/airtai/faststream/compare/0.5.23...0.5.24){.external-link target="_blank"}

## 0.5.23

### What's Changed

We made last release just a few days ago, but there are some big changes here already!

1. First of all - you can't use `faststream run ...` command without `pip install faststream[cli]` distribution anymore. It was made to minify default (and production) distribution by removing **typer** (**rich** and **click**) dependencies. **CLI** is a development-time feature, so if you don't need - just don't install! Special thanks to @RubenRibGarcia for this change

2. The next big change - **Kafka** publish confirmations by default! Previous **FastStream** version was working in *publish & forgot* style, but the new one blocks your `broker.publish(...)` call until **Kafka** confirmation frame received. To fallback to previous logic just use a new flag `broker.publish(..., no_confirm=True)`

3. Also, we made one more step forward to our **1.0.0** features plan! @KrySeyt implements `get_one` feature. Now you can use any broker subscriber to get messages in imperative style:

```python
subscriber = broker.subscriber("in")
...
msg = await subscriber.get_one(timeout=5.0)
```

4. And the last one: @draincoder continues to develop OTEL support! Now he provides us with an ability to use **OTEL spans** and **baggage** in a comfortable **FastStream**-style. Just take a look at the [new documentation section](https://faststream.airt.ai/latest/getting-started/opentelemetry/#baggage)

Big thanks to all new and old contributors who makes such a great release!

* feat: AsgiFastStream hooks init options by @Lancetnik in https://github.com/airtai/faststream/pull/1768
* fix (#1748): add Kafka publish no_confirm option by @Lancetnik in https://github.com/airtai/faststream/pull/1749
* Fix GeneralExceptionHandler typehint by @sheldygg in https://github.com/airtai/faststream/pull/1773
* Add `broker.subscriber().get_one()` by @KrySeyt in https://github.com/airtai/faststream/pull/1726
* Add OTel baggage support by @draincoder in https://github.com/airtai/faststream/pull/1692
* build(#1430): separate cli faststream to its own distribution by @RubenRibGarcia in https://github.com/airtai/faststream/pull/1769

### New Contributors
* @RubenRibGarcia made their first contribution in https://github.com/airtai/faststream/pull/1769

**Full Changelog**: https://github.com/airtai/faststream/compare/0.5.22...0.5.23

## 0.5.22

### What's Changed

* fix: FastAPI 0.112.4+ compatibility by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1766](https://github.com/airtai/faststream/pull/1766){.external-link target="_blank"}

**Full Changelog**: [#0.5.21...0.5.22](https://github.com/airtai/faststream/compare/0.5.21...0.5.22){.external-link target="_blank"}

## 0.5.21

### What's Changed

* feat (#1168): allow include regular router to FastAPI integration by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1747](https://github.com/airtai/faststream/pull/1747){.external-link target="_blank"}
* In case if core-subscriber receive a JetStream message. by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [#1751](https://github.com/airtai/faststream/pull/1751){.external-link target="_blank"}
* feat: explicit final message commit status by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1754](https://github.com/airtai/faststream/pull/1754){.external-link target="_blank"}
* Fix/context get local default by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1752](https://github.com/airtai/faststream/pull/1752){.external-link target="_blank"}
* fix (#1759): correct ConfluentConfig with enums by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1762](https://github.com/airtai/faststream/pull/1762){.external-link target="_blank"}
* Adds SASLOAuthBearer flow to AIO Kafka's Faststream Security Parsing by [@sifex](https://github.com/sifex){.external-link target="_blank"} in [#1761](https://github.com/airtai/faststream/pull/1761){.external-link target="_blank"}
* fix: FastAPI 0.112.3 compatibility by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1763](https://github.com/airtai/faststream/pull/1763){.external-link target="_blank"}

**Full Changelog**: [#0.5.20...0.5.21](https://github.com/airtai/faststream/compare/0.5.20...0.5.21){.external-link target="_blank"}

## 0.5.20

### What's Changed
* Refactor: change publisher fake subscriber generation logic by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1729](https://github.com/airtai/faststream/pull/1729){.external-link target="_blank"}
* Remove docs/api directory before running create_api_docs script by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1730](https://github.com/airtai/faststream/pull/1730){.external-link target="_blank"}
* CI: automatically approve & merge Dependabot PRs by [@dolfinus](https://github.com/dolfinus){.external-link target="_blank"} in [#1720](https://github.com/airtai/faststream/pull/1720){.external-link target="_blank"}
* Run check broken links after docs deploy by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1733](https://github.com/airtai/faststream/pull/1733){.external-link target="_blank"}
* Feature: extend FastStream.__init__ by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1734](https://github.com/airtai/faststream/pull/1734){.external-link target="_blank"}
* Fix Dependabot group names by [@dolfinus](https://github.com/dolfinus){.external-link target="_blank"} in [#1737](https://github.com/airtai/faststream/pull/1737){.external-link target="_blank"}
* Fix: respect ignored exceptions by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1735](https://github.com/airtai/faststream/pull/1735){.external-link target="_blank"}
* Fix: update FastAPI to 0.112.2 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1736](https://github.com/airtai/faststream/pull/1736){.external-link target="_blank"}


**Full Changelog**: [#0.5.19...0.5.20](https://github.com/airtai/faststream/compare/0.5.19...0.5.20){.external-link target="_blank"}

## 0.5.19

### What's Changed

The current release is planned as a latest feature release before **0.6.0**. All other **0.5.19+** releases will contain only minor bugfixes and all the team work will be focused on next major one.

There's a lot of changes we want to present you now though!

#### New RPC feature

Our old `broker.publish(..., rpc=True)` implementation was very limited and ugly. Now we present you a much suitable way to do the same thing - `broker.request(...)`

```python
from faststream import FastStream
from faststream.nats import NatsBroker, NatsResponse, NatsMessage

broker = NatsBroker()

@broker.subscriber("test")
async def echo_handler(msg):
    return NatsResponse(msg, headers={"x-token": "some-token"})

@app.after_startup
async def test():
    # The old implementation was returning just a message body,
    # so you wasn't be able to check response headers, etc
    msg_body: str = await broker.publish("ping", "test", rpc=True)
    assert msg_body == "ping"

    # Now request return the whole message and you can validate any part of it
    # moreover it triggers all your middlewares
    response: NatsMessage = await broker.request("ping", "test")
```

#### Exception Middleware

Community asked and community did! Sorry, we've been putting off this job for too long. Thanks for @Rusich90 to help us!
Now you can wrap your application by a suitable exception handlers. Just check the new [documentation](https://faststream.airt.ai/latest/getting-started/middlewares/exception/) to learn more.

#### Details

Also, there are a lot of minor changes you can find below. Big thanks to all our old and new contributors! You are amazing ones!

* Bug: resolve missing seek on kafka fakeconsumer by @JonathanSerafini in https://github.com/airtai/faststream/pull/1682
* replace pip with uv in CI by @newonlynew in https://github.com/airtai/faststream/pull/1688
* Added support for JSON serialization and deserialization by other libraries by @ulbwa in https://github.com/airtai/faststream/pull/1687
* Fix batch nack by @kumaranvpl in https://github.com/airtai/faststream/pull/1689
* Remove unused ignores by @kumaranvpl in https://github.com/airtai/faststream/pull/1690
* docs: add Kafka HowTo section by @Lancetnik in https://github.com/airtai/faststream/pull/1686
* Add missed out group_instance_id as subscriber and router parameter by @kumaranvpl in https://github.com/airtai/faststream/pull/1695
* Set warn_unused_ignores mypy config to true by @kumaranvpl in https://github.com/airtai/faststream/pull/1694
* Skip building docs in pre-commit CI job by @kumaranvpl in https://github.com/airtai/faststream/pull/1704
* Fix to run check-docs-changes workflow in forks by @kumaranvpl in https://github.com/airtai/faststream/pull/1710
* feature/exception_middleware add exception middleware by @Rusich90 in https://github.com/airtai/faststream/pull/1604
* Remove mentions of faststream-gen by @kumaranvpl in https://github.com/airtai/faststream/pull/1717
* Fix multiple docs issues by @kumaranvpl in https://github.com/airtai/faststream/pull/1718
* CI: group Dependabot updates into one PR by @dolfinus in https://github.com/airtai/faststream/pull/1719
* feat: CLI DX improvements by @Lancetnik in https://github.com/airtai/faststream/pull/1723
* fix: use async test subscribers functions by @Lancetnik in https://github.com/airtai/faststream/pull/1725
* feat: add broker.request method by @Lancetnik in https://github.com/airtai/faststream/pull/1649

### New Contributors
* @JonathanSerafini made their first contribution in https://github.com/airtai/faststream/pull/1682
* @Rusich90 made their first contribution in https://github.com/airtai/faststream/pull/1604
* @dolfinus made their first contribution in https://github.com/airtai/faststream/pull/1719

**Full Changelog**: https://github.com/airtai/faststream/compare/0.5.18...0.5.19

## 0.5.18

### What's Changed

* Added additional parameters to HandlerException by [@ulbwa](https://github.com/ulbwa){.external-link target="_blank"} in [#1659](https://github.com/airtai/faststream/pull/1659){.external-link target="_blank"}
* Removed Doc and Added docstrings instead by @Kirill-Stepankov in [#1662](https://github.com/airtai/faststream/pull/1662){.external-link target="_blank"}
* feat (#1663): support default values for Header by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1667](https://github.com/airtai/faststream/pull/1667){.external-link target="_blank"}
* fix (#1660): correct patch nested JStream subjects by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1666](https://github.com/airtai/faststream/pull/1666){.external-link target="_blank"}
* fix: add ConfluentRouter FastAPI missed  init options by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1664](https://github.com/airtai/faststream/pull/1664){.external-link target="_blank"}
* Add kerberos support for confluent broker by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1670](https://github.com/airtai/faststream/pull/1670){.external-link target="_blank"}
* Fix nack for kafka and confluent brokers by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1678](https://github.com/airtai/faststream/pull/1678){.external-link target="_blank"}
* fix: support all RMQ exchanges in AsyncAPI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1679](https://github.com/airtai/faststream/pull/1679){.external-link target="_blank"}
* fix: catch parser errors by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1680](https://github.com/airtai/faststream/pull/1680){.external-link target="_blank"}

### New Contributors
* [@ulbwa](https://github.com/ulbwa){.external-link target="_blank"} made their first contribution in [#1659](https://github.com/airtai/faststream/pull/1659){.external-link target="_blank"}
* @Kirill-Stepankov made their first contribution in [#1662](https://github.com/airtai/faststream/pull/1662){.external-link target="_blank"}

**Full Changelog**: [#0.5.17...0.5.18](https://github.com/airtai/faststream/compare/0.5.17...0.5.18){.external-link target="_blank"}

## 0.5.17

### What's Changed

Just a hotfix for the following case:

```python
@broker.subscriber(...)
async def handler():
    return NatsResponse(...)

await broker.publish(..., rpc=True)
```

* chore(deps): bump semgrep from 1.83.0 to 1.84.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#1650](https://github.com/airtai/faststream/pull/1650){.external-link target="_blank"}
* chore(deps): bump mkdocs-material from 9.5.30 to 9.5.31 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#1651](https://github.com/airtai/faststream/pull/1651){.external-link target="_blank"}
* Update Release Notes for 0.5.16 by @faststream-release-notes-updater in [#1652](https://github.com/airtai/faststream/pull/1652){.external-link target="_blank"}
* hotfix: correct NatsResponse processing in RPC case by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1654](https://github.com/airtai/faststream/pull/1654){.external-link target="_blank"}


**Full Changelog**: [#0.5.16...0.5.17](https://github.com/airtai/faststream/compare/0.5.16...0.5.17){.external-link target="_blank"}

## 0.5.16

### What's Changed

Well, seems like it is the biggest patch release ever 😃

#### Detail Responses

First of all, thanks to all new contributors, who helps us to improve the project! They made a huge impact to this release by adding new Kafka security mechanisms and extend Response API - now you can use `broker.Response` to publish detail information from handler

```python
@broker.subscriber("in")
@broker.publisher("out")
async def handler(msg):
    return Response(msg, headers={"response_header": "Hi!"})   # or KafkaResponse, etc
```

#### ASGI

Also, we added a new huge feature - [**ASGI** support](https://faststream.airt.ai/latest/getting-started/asgi/#other-asgi-compatibility)!

Nope, we are not HTTP-framework now, but it is a little ASGI implementation to provide you with an ability to host documentation, use k8s http-probes and serve metrics in the same with you broker runtime without any dependencies.

You just need to use **AsgiFastStream** class

```python
from faststream.nats import NatsBroker
from faststream.asgi import AsgiFastStream, make_ping_asgi

from prometheus_client import make_asgi_app
from prometheus_client.registry import CollectorRegistry

broker = NatsBroker()

prometheus_registry = CollectorRegistry()

app = AsgiFastStream(
    broker,
    asyncapi_path="/docs",
    asgi_routes=[
        ("/health", make_ping_asgi(broker, timeout=5.0)),
        ("/metrics", make_asgi_app(registry=prometheus_registry))
    ]
)
```

And then you can run it like a regular ASGI app

```shell
uvicorn main:app
```

#### Confluent partitions

One more thing - manual topic partition assignment for Confluent. We have it already for aiokafka, but missed it here... Now it was fixed!

```python
from faststream.confluent import TopicPartition

@broker.subscriber(partitions=[
    TopicPartition("test-topic", partition=0),
])
async def handler():
    ...
```

#### Detail changes

* feat: add RMQ `fail_fast` option in #1647
* fix: correct nested `NatsRouter` subjects prefixes behavior
* fix typos by @newonlynew in https://github.com/airtai/faststream/pull/1609
* Feat: extend response api by @Flosckow in https://github.com/airtai/faststream/pull/1607
* Feature: GSSAPI (Kerberos) support by @roma-frolov in https://github.com/airtai/faststream/pull/1633
* feat: add oauth support by @filip-danieluk in https://github.com/airtai/faststream/pull/1632
* fix: patch broker within testbroker context only by @sfran96 in https://github.com/airtai/faststream/pull/1619
* feat: ASGI support by @Lancetnik in https://github.com/airtai/faststream/pull/1635

### New Contributors
* @newonlynew made their first contribution in https://github.com/airtai/faststream/pull/1609
* @roma-frolov made their first contribution in https://github.com/airtai/faststream/pull/1633
* @filip-danieluk made their first contribution in https://github.com/airtai/faststream/pull/1632
* @sfran96 made their first contribution in https://github.com/airtai/faststream/pull/1619

**Full Changelog**: https://github.com/airtai/faststream/compare/0.5.15...0.5.16

## 0.5.15

### What's Changed

Finally, **FastStream** has a Kafka pattern subscription! This is another step forward in our [**Roadmap**](https://github.com/airtai/faststream/issues/1510) moving us to **0.6.0** and further!

```python
from faststream import Path
from faststream.kafka import KafkaBroker

broker = KafkaBroker()

@broker.subscriber(pattern="logs.{level}")
async def base_handler(
    body: str,
    level: str = Path(),
):
    ...
```

Also, all brokers now supports a new `ping` method to check real broker connection

```python
is_connected: bool = await broker.ping()
```

This is a little, but important change for [K8S probes](https://github.com/airtai/faststream/issues/1181) support

More other there are a lot of bugfixes and improvements from our contributors! Thanks to all of these amazing people!

* feat(multiprocess): restart child processes if they are not alive by @gostilovichd in https://github.com/airtai/faststream/pull/1550
* fix: use typing_extensions.TypedDict import by @Lancetnik in https://github.com/airtai/faststream/pull/1575
* fix: correct single dataclass argument AsyncAPI payload generation by @Lancetnik in https://github.com/airtai/faststream/pull/1591
* fix (#1598): use config with NATS PullSub by @Lancetnik in https://github.com/airtai/faststream/pull/1599
* feat: default call_name for broker.subscriber by @KrySeyt in https://github.com/airtai/faststream/pull/1589
* Feat: init ping method by @Flosckow in https://github.com/airtai/faststream/pull/1592
* chore: bump nats-py requirement by @Lancetnik in https://github.com/airtai/faststream/pull/1600
* fix: add pattern checking by @spataphore1337 in https://github.com/airtai/faststream/pull/1590

### New Contributors

* @gostilovichd made their first contribution in https://github.com/airtai/faststream/pull/1550
* @KrySeyt made their first contribution in https://github.com/airtai/faststream/pull/1589
* @Flosckow made their first contribution in https://github.com/airtai/faststream/pull/1592

**Full Changelog**: https://github.com/airtai/faststream/compare/0.5.14...0.5.15

## 0.5.14

### What's Changed
* Update Release Notes for 0.5.13 by @faststream-release-notes-updater in [#1548](https://github.com/airtai/faststream/pull/1548){.external-link target="_blank"}
* Add allow_auto_create_topics to make automatic topic creation configurable by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1556](https://github.com/airtai/faststream/pull/1556){.external-link target="_blank"}


**Full Changelog**: [#0.5.13...0.5.14](https://github.com/airtai/faststream/compare/0.5.13...0.5.14){.external-link target="_blank"}

## 0.5.13

### What's Changed

* feat: nats filter JS subscription support by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1519](https://github.com/airtai/faststream/pull/1519){.external-link target="_blank"}
* fix: correct RabbitExchange processing by OTEL in broker.publish case by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1521](https://github.com/airtai/faststream/pull/1521){.external-link target="_blank"}
* fix: correct Nats ObjectStorage get file behavior inside watch subscriber by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1523](https://github.com/airtai/faststream/pull/1523){.external-link target="_blank"}
* Resolve Issue 1386, Add rpc_prefix by [@aKardasz](https://github.com/aKardasz){.external-link target="_blank"} in [#1484](https://github.com/airtai/faststream/pull/1484){.external-link target="_blank"}
* fix: correct spans linking in batches case by [@draincoder](https://github.com/draincoder){.external-link target="_blank"} in [#1532](https://github.com/airtai/faststream/pull/1532){.external-link target="_blank"}
* fix (#1539): correct anyio.create_memory_object_stream annotation by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1541](https://github.com/airtai/faststream/pull/1541){.external-link target="_blank"}
* fix: correct publish_coverage CI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1536](https://github.com/airtai/faststream/pull/1536){.external-link target="_blank"}
* Add NatsBroker.new_inbox() by [@maxalbert](https://github.com/maxalbert){.external-link target="_blank"} in [#1543](https://github.com/airtai/faststream/pull/1543){.external-link target="_blank"}
* fix (#1544): correct Redis message nack & reject signature by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1546](https://github.com/airtai/faststream/pull/1546){.external-link target="_blank"}

### New Contributors
* [@aKardasz](https://github.com/aKardasz){.external-link target="_blank"} made their first contribution in [#1484](https://github.com/airtai/faststream/pull/1484){.external-link target="_blank"}
* [@maxalbert](https://github.com/maxalbert){.external-link target="_blank"} made their first contribution in [#1543](https://github.com/airtai/faststream/pull/1543){.external-link target="_blank"}

**Full Changelog**: [#0.5.12...0.5.13](https://github.com/airtai/faststream/compare/0.5.12...0.5.13){.external-link target="_blank"}

## 0.5.12

### What's Changed

Now, `FastStream` provides users with the ability to pass the `config` dictionary to `confluent-kafka-python` for greater customizability. The following example sets the parameter `topic.metadata.refresh.fast.interval.ms`'s value to `300` instead of the default value `100` via the `config` parameter.

```python
from faststream import FastStream
from faststream.confluent import KafkaBroker

config = {"topic.metadata.refresh.fast.interval.ms": 300}
broker = KafkaBroker("localhost:9092", config=config)
app = FastStream(broker)
```

* Update Release Notes for 0.5.11 by @faststream-release-notes-updater in [#1511](https://github.com/airtai/faststream/pull/1511){.external-link target="_blank"}
* docs: update filters example by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1516](https://github.com/airtai/faststream/pull/1516){.external-link target="_blank"}
* Add config param to pass additional parameters to confluent-kafka-python by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1505](https://github.com/airtai/faststream/pull/1505){.external-link target="_blank"}


**Full Changelog**: [#0.5.11...0.5.12](https://github.com/airtai/faststream/compare/0.5.11...0.5.12){.external-link target="_blank"}

## 0.5.11

### What's Changed
* Update Release Notes for 0.5.10 by @faststream-release-notes-updater in [#1482](https://github.com/airtai/faststream/pull/1482){.external-link target="_blank"}
* feat: provide with an ability to create default RMQ Exchange by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1485](https://github.com/airtai/faststream/pull/1485){.external-link target="_blank"}
* docs: fix typos by [@crazymidnight](https://github.com/crazymidnight){.external-link target="_blank"} in [#1489](https://github.com/airtai/faststream/pull/1489){.external-link target="_blank"}
* chore: update CI triggers to minify useless runs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1483](https://github.com/airtai/faststream/pull/1483){.external-link target="_blank"}
* Update link to badges by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1496](https://github.com/airtai/faststream/pull/1496){.external-link target="_blank"}
* Run tests every day at 12:00 AM by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1497](https://github.com/airtai/faststream/pull/1497){.external-link target="_blank"}
* Chore: update deps by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1503](https://github.com/airtai/faststream/pull/1503){.external-link target="_blank"}
* fix: include NatsRouter streams to original broker by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1509](https://github.com/airtai/faststream/pull/1509){.external-link target="_blank"}

### New Contributors
* [@crazymidnight](https://github.com/crazymidnight){.external-link target="_blank"} made their first contribution in [#1489](https://github.com/airtai/faststream/pull/1489){.external-link target="_blank"}

**Full Changelog**: [#0.5.10...0.5.11](https://github.com/airtai/faststream/compare/0.5.10...0.5.11){.external-link target="_blank"}

## 0.5.10

### What's Changed

Now you can return Response class to set more specific outgoing message parameters:

```python
from faststream import Response

@broker.subscriber("in")
@broker.subscriber("out")
async def handler():
    return Response(body=b"", headers={})
```

* Pass logger to confluent producer and consumer by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1464](https://github.com/airtai/faststream/pull/1464){.external-link target="_blank"}
* Fixes  #1412 with `TestKafkaBroker` behaviour where Consumer Groups weren't being respected by [@sifex](https://github.com/sifex){.external-link target="_blank"} in [#1413](https://github.com/airtai/faststream/pull/1413){.external-link target="_blank"}
* Chore: update dependency versions by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1478](https://github.com/airtai/faststream/pull/1478){.external-link target="_blank"}
* Remove typing-extensions version restriction by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1477](https://github.com/airtai/faststream/pull/1477){.external-link target="_blank"}
* feat (#1431): add Response class by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1481](https://github.com/airtai/faststream/pull/1481){.external-link target="_blank"}

### New Contributors
* [@sifex](https://github.com/sifex){.external-link target="_blank"} made their first contribution in [#1413](https://github.com/airtai/faststream/pull/1413){.external-link target="_blank"}

**Full Changelog**: [#0.5.9...0.5.10](https://github.com/airtai/faststream/compare/0.5.9...0.5.10){.external-link target="_blank"}

## 0.5.9

### What's Changed
* Update Release Notes for 0.5.8 by @faststream-release-notes-updater in [#1462](https://github.com/airtai/faststream/pull/1462){.external-link target="_blank"}
* Exclude typing_extensions version 4.12.* by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1467](https://github.com/airtai/faststream/pull/1467){.external-link target="_blank"}
* fix: add group/consumer to hash to avoid overwriting by [@fbraem](https://github.com/fbraem){.external-link target="_blank"} in [#1463](https://github.com/airtai/faststream/pull/1463){.external-link target="_blank"}
* Bump version to 0.5.9 by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1468](https://github.com/airtai/faststream/pull/1468){.external-link target="_blank"}

### New Contributors
* [@fbraem](https://github.com/fbraem){.external-link target="_blank"} made their first contribution in [#1463](https://github.com/airtai/faststream/pull/1463){.external-link target="_blank"}

**Full Changelog**: [#0.5.8...0.5.9](https://github.com/airtai/faststream/compare/0.5.8...0.5.9){.external-link target="_blank"}

## 0.5.8

### What's Changed

This is the time for a new **NATS** features! **FastStream** supports **NATS Key-Value** and **Object Storage** subscription features in a native way now (big thx for @sheldygg)!

1. KeyValue creation and watching API added (you can read updated [documentation section](https://faststream.airt.ai/latest/nats/jetstream/key-value/) for changes):

   ```python
    from faststream import FastStream, Logger
    from faststream.nats import NatsBroker

    broker = NatsBroker()
    app = FastStream(broker)

    @broker.subscriber("some-key", kv_watch="bucket")
    async def handler(msg: int, logger: Logger):
        logger.info(msg)

    @app.after_startup
    async def test():
        kv = await broker.key_value("bucket")
        await kv.put("some-key", b"1")
   ```

2. ObjectStore API added as well (you can read updated [documentation section](https://faststream.airt.ai/latest/nats/jetstream/object/) for changes):

    ```python
    from faststream import FastStream, Logger
    from faststream.nats import NatsBroker

    broker = NatsBroker()
    app = FastStream(broker)

    @broker.subscriber("file-bucket", obj_watch=True)
    async def handler(filename: str, logger: Logger):
        logger.info(filename)

    @app.after_startup
    async def test():
        object_store = await broker.object_storage("file-bucket")
        await object_store.put("some-file.txt", b"1")
    ```

3. Also now you can use just `pull_sub=True` instead of `pull_sub=PullSub()` in basic case:

   ```python
    from faststream import FastStream, Logger
    from faststream.nats import NatsBroker

    broker = NatsBroker()
    app = FastStream(broker)

    @broker.subscriber("test", stream="stream", pull_sub=True)
    async def handler(msg, logger: Logger):
        logger.info(msg)
    ```

Finally, we have a new feature, related to all brokers: special flag to suppress automatic RPC and reply_to responses:

```python
@broker.subscriber("tests", no_reply=True)
async def handler():
    ....

# will fail with timeout, because there is no automatic response
msg = await broker.publish("msg", "test", rpc=True)
```

* fix: when headers() returns None in AsyncConfluentParser, replace it with an empty tuple by @andreaimprovised in https://github.com/airtai/faststream/pull/1460
* Implement Kv/Obj watch. by @sheldygg in https://github.com/airtai/faststream/pull/1383
* feat: add subscriber no-reply option by @Lancetnik in https://github.com/airtai/faststream/pull/1461

### New Contributors
* @andreaimprovised made their first contribution in https://github.com/airtai/faststream/pull/1460

**Full Changelog**: https://github.com/airtai/faststream/compare/0.5.7...0.5.8

## 0.5.7

### What's Changed

Finally, FastStream supports [OpenTelemetry](https://opentelemetry.io/) in a native way to collect the full trace of your services! Big thanks for @draincoder for that!

First of all you need to install required dependencies to support OpenTelemetry:

```bash
pip install faststream[otel]
```

Then you can just add a middleware for your broker and that's it!

```python
from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.nats.opentelemetry import NatsTelemetryMiddleware

broker = NatsBroker(
    middlewares=(
        NatsTelemetryMiddleware(),
    )
)
app = FastStream(broker)
```

To find detail information just visit our documentation about [telemetry](https://faststream.airt.ai/latest/getting-started/opentelemetry/)

P.S. The release includes basic OpenTelemetry support - messages tracing & basic metrics. Baggage support and correct spans linking in batch processing case will be added soon.

* fix: serialize TestClient rpc output to mock the real message by @Lancetnik in https://github.com/airtai/faststream/pull/1452
* feature (#916): Observability by @draincoder in https://github.com/airtai/faststream/pull/1398

### New Contributors
* @draincoder made their first contribution in https://github.com/airtai/faststream/pull/1398

**Full Changelog**: https://github.com/airtai/faststream/compare/0.5.6...0.5.7

## 0.5.6

### What's Changed

* feature: add --factory param by [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} in [#1440](https://github.com/airtai/faststream/pull/1440){.external-link target="_blank"}
* feat: add RMQ channels options, support for prefix for routing_key, a… by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1448](https://github.com/airtai/faststream/pull/1448){.external-link target="_blank"}
* feature: Add `from faststream.rabbit.annotations import Connection, Channel` shortcuts
* Bugfix: RabbitMQ RabbitRouter prefix now affects to queue routing key as well
* Feature (close #1402): add `broker.add_middleware` public API to append a middleware to already created broker
* Feature: add `RabbitBroker(channel_number: int, publisher_confirms: bool, on_return_raises: bool)` options to setup channel settings
* Feature (close #1447): add `StreamMessage.batch_headers` attribute to provide with access to whole batch messages headers

### New Contributors

* [@Sehat1137](https://github.com/Sehat1137){.external-link target="_blank"} made their first contribution in [#1440](https://github.com/airtai/faststream/pull/1440){.external-link target="_blank"}

**Full Changelog**: [#0.5.5...0.5.6](https://github.com/airtai/faststream/compare/0.5.5...0.5.6){.external-link target="_blank"}

## 0.5.5

### What's Changed

Add support for explicit partition assignment in aiokafka `KafkaBroker` (special thanks to @spataphore1337):

```python
from faststream import FastStream
from faststream.kafka import KafkaBroker, TopicPartition

broker = KafkaBroker()

topic_partition_first = TopicPartition("my_topic", 1)
topic_partition_second = TopicPartition("my_topic", 2)

@broker.subscribe(partitions=[topic_partition_first, topic_partition_second])
async def some_consumer(msg):
   ...
```

* Update Release Notes for 0.5.4 by @faststream-release-notes-updater in [#1421](https://github.com/airtai/faststream/pull/1421){.external-link target="_blank"}
* feature: manual partition assignment to Kafka by [@spataphore1337](https://github.com/spataphore1337){.external-link target="_blank"} in [#1422](https://github.com/airtai/faststream/pull/1422){.external-link target="_blank"}
* Chore/update deps by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1429](https://github.com/airtai/faststream/pull/1429){.external-link target="_blank"}
* Fix/correct dynamic subscriber registration by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1433](https://github.com/airtai/faststream/pull/1433){.external-link target="_blank"}
* chore: bump version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1435](https://github.com/airtai/faststream/pull/1435){.external-link target="_blank"}


**Full Changelog**: [#0.5.4...0.5.5](https://github.com/airtai/faststream/compare/0.5.4...0.5.5){.external-link target="_blank"}

## 0.5.4

### What's Changed

* Update Release Notes for 0.5.3 by @faststream-release-notes-updater in [#1400](https://github.com/airtai/faststream/pull/1400){.external-link target="_blank"}
* fix (#1415): raise SetupError if rpc and reply_to are using in TestCL… by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1419](https://github.com/airtai/faststream/pull/1419){.external-link target="_blank"}
* Chore/update deps2 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1418](https://github.com/airtai/faststream/pull/1418){.external-link target="_blank"}
* refactor: correct security with kwarg params merging by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1417](https://github.com/airtai/faststream/pull/1417){.external-link target="_blank"}
* fix (#1414): correct Message.ack error processing by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1420](https://github.com/airtai/faststream/pull/1420){.external-link target="_blank"}

**Full Changelog**: [#0.5.3...0.5.4](https://github.com/airtai/faststream/compare/0.5.3...0.5.4){.external-link target="_blank"}

## 0.5.3

### What's Changed
* Update Release Notes for 0.5.2 by @faststream-release-notes-updater in [#1382](https://github.com/airtai/faststream/pull/1382){.external-link target="_blank"}
* Fix/setup at broker connection instead of starting by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1385](https://github.com/airtai/faststream/pull/1385){.external-link target="_blank"}
* Tests/add path tests by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1388](https://github.com/airtai/faststream/pull/1388){.external-link target="_blank"}
* Fix/path with router prefix by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1395](https://github.com/airtai/faststream/pull/1395){.external-link target="_blank"}
* chore: update dependencies by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1396](https://github.com/airtai/faststream/pull/1396){.external-link target="_blank"}
* chore: bump version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1397](https://github.com/airtai/faststream/pull/1397){.external-link target="_blank"}
* chore: polishing by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1399](https://github.com/airtai/faststream/pull/1399){.external-link target="_blank"}


**Full Changelog**: [#0.5.2...0.5.3](https://github.com/airtai/faststream/compare/0.5.2...0.5.3){.external-link target="_blank"}

## 0.5.2

### What's Changed

Just a little bugfix patch. Fixes #1379 and #1376.

* Update Release Notes for 0.5.1 by @faststream-release-notes-updater in [#1378](https://github.com/airtai/faststream/pull/1378){.external-link target="_blank"}
* Tests/fastapi background by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1380](https://github.com/airtai/faststream/pull/1380){.external-link target="_blank"}
* Fix/0.5.2 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1381](https://github.com/airtai/faststream/pull/1381){.external-link target="_blank"}


**Full Changelog**: [#0.5.1...0.5.2](https://github.com/airtai/faststream/compare/0.5.1...0.5.2){.external-link target="_blank"}

## 0.5.1

### What's Changed

We already have some fixes related to `RedisBroker` (#1375, #1376) and some new features for you:

1. Now `broke.include_router(...)` allows to pass some arguments to setup router at including moment instead of creation

  ```python
  broker.include_router(
     router,
     prefix="test_",
     dependencies=[Depends(...)],
     middlewares=[BrokerMiddleware],
     include_in_schema=False,
  )
  ```

2. `KafkaBroker().subscriber(...)` now consumes `aiokafka.ConsumerRebalanceListener` object.
You can find more information about it in the official [**aiokafka** doc](https://aiokafka.readthedocs.io/en/stable/consumer.html?highlight=subscribe#topic-subscription-by-pattern)

  (close #1319)

  ```python
  broker = KafkaBroker()

  broker.subscriber(..., listener=MyRebalancer())
  ```

`pattern` option was added too, but it is still experimental and does not support `Path`

3. [`Path`](https://faststream.airt.ai/latest/nats/message/#subject-pattern-access) feature performance was increased. Also, `Path` is suitable for NATS `PullSub` batch subscription as well now.

```python
from faststream import NatsBroker, PullSub

broker = NastBroker()

@broker.subscriber(
    "logs.{level}",
    steam="test-stream",
    pull_sub=PullSub(batch=True),
)
async def base_handler(
    ...,
    level: str = Path(),
):
  ...
```

* Update Release Notes for 0.5.0 by @faststream-release-notes-updater in https://github.com/airtai/faststream/pull/1366
* chore: bump version by @Lancetnik in https://github.com/airtai/faststream/pull/1372
* feat: kafka listener, extended include_router by @Lancetnik in https://github.com/airtai/faststream/pull/1374
* Fix/1375 by @Lancetnik in https://github.com/airtai/faststream/pull/1377


**Full Changelog**: https://github.com/airtai/faststream/compare/0.5.0...0.5.1

## 0.5.0

### What's Changed

This is the biggest change since the creation of FastStream. We have completely refactored the entire package, changing the object registration mechanism, message processing pipeline, and application lifecycle. However, you won't even notice it—we've preserved all public APIs from breaking changes. The only feature not compatible with the previous code is the new middleware.

New features:

1. `await FastStream.stop()` method and `StopApplication` exception to stop a `FastStream` worker are added.

2. `broker.subscriber()` and `router.subscriber()` functions now return a `Subscriber` object you can use later.

```python
subscriber = broker.subscriber("test")

@subscriber(filter = lambda msg: msg.content_type == "application/json")
async def handler(msg: dict[str, Any]):
    ...

@subscriber()
async def handler(msg: dict[str, Any]):
    ...
 ```

This is the preferred syntax for [filtering](https://faststream.airt.ai/latest/getting-started/subscription/filtering/) now (the old one will be removed in `0.6.0`)

 3. The `router.publisher()` function now returns the correct `Publisher` object you can use later (after broker startup).

 ```python
 publisher = router.publisher("test")

 @router.subscriber("in")
 async def handler():
     await publisher.publish("msg")
 ```

 (Until `0.5.0` you could use it in this way with `broker.publisher` only)

 4. A list of `middlewares` can be passed to a `broker.publisher` as well:

 ```python
 broker = Broker(..., middlewares=())

 @broker.subscriber(..., middlewares=())
 @broker.publisher(..., middlewares=())  # new feature
 async def handler():
     ...
 ```

5. Broker-level middlewares now affect all ways to publish a message, so you can encode application outgoing messages here.

6. ⚠️ BREAKING CHANGE ⚠️ : both `subscriber` and `publisher` middlewares should be async context manager type

```python
async def subscriber_middleware(call_next, msg):
    return await call_next(msg)

async def publisher_middleware(call_next, msg, **kwargs):
    return await call_next(msg, **kwargs)

@broker.subscriber(
    "in",
    middlewares=(subscriber_middleware,),
)
@broker.publisher(
    "out",
    middlewares=(publisher_middleware,),
)
async def handler(msg):
    return msg
```

Such changes allow you two previously unavailable features:
* suppress any exceptions and pass fall-back message body to publishers, and
* patch any outgoing message headers and other parameters.

Without those features we could not implement [Observability Middleware](https://github.com/airtai/faststream/issues/916) or any similar tool, so it is the job that just had to be done.
7. A better **FastAPI** compatibility: `fastapi.BackgroundTasks` and `response_class` subscriber option are supported.

8. All `.pyi` files are removed, and explicit docstrings and methods options are added.

9. New subscribers can be registered in runtime (with an already-started broker):

```python
subscriber = broker.subscriber("dynamic")
subscriber(handler_method)
...
broker.setup_subscriber(subscriber)
await subscriber.start()
...
await subscriber.close()
```

10. `faststream[docs]` distribution is removed.

* Update Release Notes for 0.4.7 by @faststream-release-notes-updater in https://github.com/airtai/faststream/pull/1295
* 1129 - Create a publish command for the CLI by @MRLab12 in https://github.com/airtai/faststream/pull/1151
* Chore: packages upgraded by @davorrunje in https://github.com/airtai/faststream/pull/1306
* docs: fix typos by @omahs in https://github.com/airtai/faststream/pull/1309
* chore: update dependencies by @Lancetnik in https://github.com/airtai/faststream/pull/1323
* docs: fix misc by @Lancetnik in https://github.com/airtai/faststream/pull/1324
* docs (#1327): correct RMQ exchanges behavior by @Lancetnik in https://github.com/airtai/faststream/pull/1328
* fix: typer 0.12 exclude by @Lancetnik in https://github.com/airtai/faststream/pull/1341
* 0.5.0 by @Lancetnik in https://github.com/airtai/faststream/pull/1326
  * close #1103
  * close #840
  * fix #690
  * fix #1206
  * fix #1227
  * close #568
  * close #1303
  * close #1287
  * feat #607
* Generate docs and linter fixes by @davorrunje in https://github.com/airtai/faststream/pull/1348
* Fix types by @davorrunje in https://github.com/airtai/faststream/pull/1349
* chore: update dependencies by @Lancetnik in https://github.com/airtai/faststream/pull/1358
* feat: final middlewares by @Lancetnik in https://github.com/airtai/faststream/pull/1357
* Docs/0.5.0 features by @Lancetnik in https://github.com/airtai/faststream/pull/1360

### New Contributors
* @MRLab12 made their first contribution in https://github.com/airtai/faststream/pull/1151
* @omahs made their first contribution in https://github.com/airtai/faststream/pull/1309

**Full Changelog**: https://github.com/airtai/faststream/compare/0.4.7...0.5.0

## 0.5.0rc2

### What's Changed

This is the final API change before stable `0.5.0` release

⚠️ HAS BREAKING CHANGE

In it, we stabilize the behavior of publishers & subscribers middlewares

```python
async def subscriber_middleware(call_next, msg):
    return await call_next(msg)

async def publisher_middleware(call_next, msg, **kwargs):
    return await call_next(msg, **kwargs)

@broker.subscriber(
    "in",
    middlewares=(subscriber_middleware,),
)
@broker.publisher(
    "out",
    middlewares=(publisher_middleware,),
)
async def handler(msg):
    return msg
```

Such changes allows you two features previously unavailable

* suppress any exceptions and pas fall-back message body to publishers
* patch any outgoing message headers and other parameters

Without these features we just can't implement [Observability Middleware](https://github.com/airtai/faststream/issues/916) or any similar tool, so it is the job to be done.

Now you are free to get access at any message processing stage and we are one step closer to the framework we would like to create!

* Update Release Notes for 0.5.0rc0 by @faststream-release-notes-updater in https://github.com/airtai/faststream/pull/1347
* Generate docs and linter fixes by @davorrunje in https://github.com/airtai/faststream/pull/1348
* Fix types by @davorrunje in https://github.com/airtai/faststream/pull/1349
* chore: update dependencies by @Lancetnik in https://github.com/airtai/faststream/pull/1358
* feat: final middlewares by @Lancetnik in https://github.com/airtai/faststream/pull/1357


**Full Changelog**: https://github.com/airtai/faststream/compare/0.5.0rc0...0.5.0rc2

## 0.5.0rc0

### What's Changed

This is the biggest change since the creation of FastStream. We have completely refactored the entire package, changing the object registration mechanism, message processing pipeline, and application lifecycle. However, you won't even notice it—we've preserved all public APIs from breaking changes. The only feature not compatible with the previous code is the new middleware.

This is still an RC (Release Candidate) for you to test before the stable release. You can manually install it in your project:

```console
pip install faststream==0.5.0rc0
```

We look forward to your feedback!

New features:

1. `await FastStream.stop()` method and `StopApplication` exception to stop a `FastStream` worker are added.

2. `broker.subscriber()` and `router.subscriber()` functions now return a `Subscriber` object you can use later.

```python
subscriber = broker.subscriber("test")

@subscriber(filter = lambda msg: msg.content_type == "application/json")
async def handler(msg: dict[str, Any]):
    ...

@subscriber()
async def handler(msg: dict[str, Any]):
    ...
 ```

This is the preferred syntax for [filtering](https://faststream.airt.ai/latest/getting-started/subscription/filtering/) now (the old one will be removed in `0.6.0`)

 3. The `router.publisher()` function now returns the correct `Publisher` object you can use later (after broker startup).

 ```python
 publisher = router.publisher("test")

 @router.subscriber("in")
 async def handler():
     await publisher.publish("msg")
 ```

 (Until `0.5.0` you could use it in this way with `broker.publisher` only)

 4. A list of `middlewares` can be passed to a `broker.publisher` as well:

 ```python
 broker = Broker(..., middlewares=())

 @broker.subscriber(..., middlewares=())
 @broker.publisher(..., middlewares=())  # new feature
 async def handler():
     ...
 ```

5. Broker-level middlewares now affect all ways to publish a message, so you can encode application outgoing messages here.

6. ⚠️ BREAKING CHANGE ⚠️ : both `subscriber` and `publisher` middlewares should be async context manager type

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def subscriber_middleware(msg_body):
    yield msg_body

@asynccontextmanager
async def publisher_middleware(
    msg_to_publish,
    **publish_arguments,
):
    yield msg_to_publish

@broker.subscriber("in", middlewares=(subscriber_middleware,))
@broker.publisher("out", middlewares=(publisher_middleware,))
async def handler():
    ...
```

7. A better **FastAPI** compatibility: `fastapi.BackgroundTasks` and `response_class` subscriber option are supported.

8. All `.pyi` files are removed, and explicit docstrings and methods options are added.

9. New subscribers can be registered in runtime (with an already-started broker):

```python
subscriber = broker.subscriber("dynamic")
subscriber(handler_method)
...
broker.setup_subscriber(subscriber)
await subscriber.start()
...
await subscriber.close()
```

10. `faststream[docs]` distribution is removed.

* Update Release Notes for 0.4.7 by @faststream-release-notes-updater in https://github.com/airtai/faststream/pull/1295
* 1129 - Create a publish command for the CLI by @MRLab12 in https://github.com/airtai/faststream/pull/1151
* Chore: packages upgraded by @davorrunje in https://github.com/airtai/faststream/pull/1306
* docs: fix typos by @omahs in https://github.com/airtai/faststream/pull/1309
* chore: update dependencies by @Lancetnik in https://github.com/airtai/faststream/pull/1323
* docs: fix misc by @Lancetnik in https://github.com/airtai/faststream/pull/1324
* docs (#1327): correct RMQ exchanges behavior by @Lancetnik in https://github.com/airtai/faststream/pull/1328
* fix: typer 0.12 exclude by @Lancetnik in https://github.com/airtai/faststream/pull/1341
* 0.5.0 by @Lancetnik in https://github.com/airtai/faststream/pull/1326
* close #1103
* close #840
* fix #690
* fix #1206
* fix #1227
* close #568
* close #1303
* close #1287
* feat #607

### New Contributors

* @MRLab12 made their first contribution in https://github.com/airtai/faststream/pull/1151
* @omahs made their first contribution in https://github.com/airtai/faststream/pull/1309

**Full Changelog**: https://github.com/airtai/faststream/compare/0.4.7...0.5.0rc0


## 0.4.7

### What's Changed

* Update Release Notes for 0.4.6 by @faststream-release-notes-updater in [#1286](https://github.com/airtai/faststream/pull/1286){.external-link target="_blank"}
* fix (#1263): correct nested discriminator msg type AsyncAPI schema by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1288](https://github.com/airtai/faststream/pull/1288){.external-link target="_blank"}
* docs: add `apply_types` warning notice to subscription/index.md by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1291](https://github.com/airtai/faststream/pull/1291){.external-link target="_blank"}
* chore: fixed nats-py version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1294](https://github.com/airtai/faststream/pull/1294){.external-link target="_blank"}

**Full Changelog**: [#0.4.6...0.4.7](https://github.com/airtai/faststream/compare/0.4.6...0.4.7){.external-link target="_blank"}

## 0.4.6

### What's Changed
* Add poll in confluent producer to fix BufferError by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1277](https://github.com/airtai/faststream/pull/1277){.external-link target="_blank"}
* Cover confluent asyncapi tests by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1279](https://github.com/airtai/faststream/pull/1279){.external-link target="_blank"}
* chore: bump package versions by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1285](https://github.com/airtai/faststream/pull/1285){.external-link target="_blank"}


**Full Changelog**: [#0.4.5...0.4.6](https://github.com/airtai/faststream/compare/0.4.5...0.4.6){.external-link target="_blank"}

## 0.4.5

### What's Changed
* Update Release Notes for 0.4.4 by @faststream-release-notes-updater in [#1260](https://github.com/airtai/faststream/pull/1260){.external-link target="_blank"}
* Removed unused pytest dependency from redis/schemas.py by [@ashambalev](https://github.com/ashambalev){.external-link target="_blank"} in [#1261](https://github.com/airtai/faststream/pull/1261){.external-link target="_blank"}
* chore: bumped package versions by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1270](https://github.com/airtai/faststream/pull/1270){.external-link target="_blank"}
* fix (#1263): correct AsyncAPI schema in discriminator case by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1272](https://github.com/airtai/faststream/pull/1272){.external-link target="_blank"}

### New Contributors
* [@ashambalev](https://github.com/ashambalev){.external-link target="_blank"} made their first contribution in [#1261](https://github.com/airtai/faststream/pull/1261){.external-link target="_blank"}

**Full Changelog**: [#0.4.4...0.4.5](https://github.com/airtai/faststream/compare/0.4.4...0.4.5){.external-link target="_blank"}

## 0.4.4

### What's Changed

Add RedisStream batch size option

```python
@broker.subscriber(stream=StreamSub("input", batch=True, max_records=3))
async def on_input_data(msgs: list[str]):
    assert len(msgs) <= 3
```

* Update Release Notes for 0.4.3 by @faststream-release-notes-updater in [#1247](https://github.com/airtai/faststream/pull/1247){.external-link target="_blank"}
* docs: add manual run section by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1249](https://github.com/airtai/faststream/pull/1249){.external-link target="_blank"}
* feat (#1252): respect Redis StreamSub last_id with consumer group by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1256](https://github.com/airtai/faststream/pull/1256){.external-link target="_blank"}
* fix: correct Redis consumer group behavior by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1258](https://github.com/airtai/faststream/pull/1258){.external-link target="_blank"}
* feat: add Redis Stream max_records option by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1259](https://github.com/airtai/faststream/pull/1259){.external-link target="_blank"}


**Full Changelog**: [#0.4.3...0.4.4](https://github.com/airtai/faststream/compare/0.4.3...0.4.4){.external-link target="_blank"}

## 0.4.3

### What's Changed

Allow to specify **Redis Stream** maxlen option in publisher:

```python
@broker.publisher(stream=StreamSub("Output", max_len=10))
async def on_input_data():
    ....
```

* chore: bump version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1198](https://github.com/airtai/faststream/pull/1198){.external-link target="_blank"}
* Update Release Notes for 0.4.2 by @faststream-release-notes-updater in [#1199](https://github.com/airtai/faststream/pull/1199){.external-link target="_blank"}
* Add missing API documentation for apply_pattern by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1201](https://github.com/airtai/faststream/pull/1201){.external-link target="_blank"}
* chore: polishing by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1203](https://github.com/airtai/faststream/pull/1203){.external-link target="_blank"}
* Comment out retry and timeout in a confluent test by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1207](https://github.com/airtai/faststream/pull/1207){.external-link target="_blank"}
* Commit offsets only if auto_commit is True by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1208](https://github.com/airtai/faststream/pull/1208){.external-link target="_blank"}
* Add a CI job to check for missed docs changes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1217](https://github.com/airtai/faststream/pull/1217){.external-link target="_blank"}
* fix: inconsistent NATS publisher signature by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1218](https://github.com/airtai/faststream/pull/1218){.external-link target="_blank"}
* Upgrade packages by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1226](https://github.com/airtai/faststream/pull/1226){.external-link target="_blank"}
* chore: bump dawidd6/action-download-artifact from 3.0.0 to 3.1.1 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#1239](https://github.com/airtai/faststream/pull/1239){.external-link target="_blank"}
* chore: bump dependencies by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1246](https://github.com/airtai/faststream/pull/1246){.external-link target="_blank"}
* feat (#1235): StreamSub maxlen parameter by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1245](https://github.com/airtai/faststream/pull/1245){.external-link target="_blank"}
* fix (#1234): correct FastAPI path passing, fix typehints by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1236](https://github.com/airtai/faststream/pull/1236){.external-link target="_blank"}
* fix (#1231): close RMQ while reconnecting by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1238](https://github.com/airtai/faststream/pull/1238){.external-link target="_blank"}


**Full Changelog**: [#0.4.2...0.4.3](https://github.com/airtai/faststream/compare/0.4.2...0.4.3){.external-link target="_blank"}

## 0.4.2

### What's Changed

#### Bug fixes

* fix: correct RMQ Topic testing routing by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1196](https://github.com/airtai/faststream/pull/1196){.external-link target="_blank"}
* fix #1191: correct RMQ ssl default port by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1195](https://github.com/airtai/faststream/pull/1195){.external-link target="_blank"}
* fix #1143: ignore Depends in AsyncAPI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1197](https://github.com/airtai/faststream/pull/1197){.external-link target="_blank"}


**Full Changelog**: [#0.4.1...0.4.2](https://github.com/airtai/faststream/compare/0.4.1...0.4.2){.external-link target="_blank"}

## 0.4.1

### What's Changed

#### Bug fixes

* Fix: use FastAPI overrides in subscribers by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1189](https://github.com/airtai/faststream/pull/1189){.external-link target="_blank"}
* Handle confluent consumer commit failure by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1193](https://github.com/airtai/faststream/pull/1193){.external-link target="_blank"}

#### Documentation

* Include Confluent in home and features pages by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1186](https://github.com/airtai/faststream/pull/1186){.external-link target="_blank"}
* Use pydantic model for publishing in docs example by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1187](https://github.com/airtai/faststream/pull/1187){.external-link target="_blank"}


**Full Changelog**: [#0.4.0...0.4.1](https://github.com/airtai/faststream/compare/0.4.0...0.4.1){.external-link target="_blank"}

## 0.4.0

### What's Changed

This release adds support for the [Confluent's Python Client for Apache Kafka (TM)](https://github.com/confluentinc/confluent-kafka-python). Confluent's Python Client for Apache Kafka does not support natively `async` functions and its integration with modern async-based services is a bit trickier. That was the reason why our initial supported by Kafka broker used [aiokafka](https://github.com/aio-libs/aiokafka). However, that choice was a less fortunate one as it is as well maintained as the Confluent version. After receiving numerous requests, we finally decided to bite the bullet and create an `async` wrapper around Confluent's Python Client and add full support for it in FastStream.

If you want to try it out, install it first with:
```sh
pip install "faststream[confluent]>=0.4.0"
```

To connect to Kafka using the FastStream KafkaBroker module, follow these steps:

1. Initialize the KafkaBroker instance: Start by initializing a KafkaBroker instance with the necessary configuration, including Kafka broker address.

2. Create your processing logic: Write a function that will consume the incoming messages in the defined format and produce a response to the defined topic

3. Decorate your processing function: To connect your processing function to the desired Kafka topics you need to decorate it with `@broker.subscriber(...)` and `@broker.publisher(...)` decorators. Now, after you start your application, your processing function will be called whenever a new message in the subscribed topic is available and produce the function return value to the topic defined in the publisher decorator.

Here's a simplified code example demonstrating how to establish a connection to Kafka using FastStream's KafkaBroker module:

```python
from faststream import FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

@broker.subscriber("in-topic")
@broker.publisher("out-topic")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
```

For more information, please visit the documentation at:

https://faststream.airt.ai/latest/confluent/

#### List of Changes

* Update Release Notes for 0.3.13 by @faststream-release-notes-updater in https://github.com/airtai/faststream/pull/1119
* docs: close #1125 by @Lancetnik in https://github.com/airtai/faststream/pull/1126
* Add support for confluent python lib by @kumaranvpl in https://github.com/airtai/faststream/pull/1042
* Update tutorial docs to include confluent code examples by @kumaranvpl in https://github.com/airtai/faststream/pull/1131
* Add installation instructions for confluent by @kumaranvpl in https://github.com/airtai/faststream/pull/1132
* Update Release Notes for 0.4.0rc0 by @faststream-release-notes-updater in https://github.com/airtai/faststream/pull/1130
* chore: remove useless branch from CI by @Lancetnik in https://github.com/airtai/faststream/pull/1135
* chore: bump mkdocs-git-revision-date-localized-plugin from 1.2.1 to 1.2.2 by @dependabot in https://github.com/airtai/faststream/pull/1140
* chore: strict fast-depends version by @Lancetnik in https://github.com/airtai/faststream/pull/1145
* chore: update copyright by @Lancetnik in https://github.com/airtai/faststream/pull/1144
* fix: correct Windows shutdown by @Lancetnik in https://github.com/airtai/faststream/pull/1148
* docs: fix typo by @saroz014 in https://github.com/airtai/faststream/pull/1154
* Middleware Document Syntax Error by @SepehrBazyar in https://github.com/airtai/faststream/pull/1156
* fix: correct FastAPI Context type hints by @Lancetnik in https://github.com/airtai/faststream/pull/1155
* Fix bug which results in lost confluent coverage report by @kumaranvpl in https://github.com/airtai/faststream/pull/1160
* Fix failing ack tests for confluent by @kumaranvpl in https://github.com/airtai/faststream/pull/1166
* Update version to 0.4.0 and update docs by @kumaranvpl in https://github.com/airtai/faststream/pull/1171
* feat #1180: add StreamRouter.on_broker_shutdown hook by @Lancetnik in https://github.com/airtai/faststream/pull/1182
* Fix bug - using old upload-artifact version by @kumaranvpl in https://github.com/airtai/faststream/pull/1183
* Release 0.4.0 by @davorrunje in https://github.com/airtai/faststream/pull/1184

### New Contributors
* @saroz014 made their first contribution in https://github.com/airtai/faststream/pull/1154

**Full Changelog**: https://github.com/airtai/faststream/compare/0.3.13...0.4.0

## 0.4.0rc0

### What's Changed

This is a **preview version** of 0.4.0 release introducing support for Confluent-based Kafka broker.

Here's a simplified code example demonstrating how to establish a connection to Kafka using FastStream's KafkaBroker module:
```python
from faststream import FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

@broker.subscriber("in-topic")
@broker.publisher("out-topic")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
```

#### Changes

* Add support for confluent python lib by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1042](https://github.com/airtai/faststream/pull/1042){.external-link target="_blank"}


**Full Changelog**: [#0.3.13...0.4.0rc0](https://github.com/airtai/faststream/compare/0.3.13...0.4.0rc0){.external-link target="_blank"}

## 0.3.13

### What's Changed

#### New features

* New shutdown logic by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1117](https://github.com/airtai/faststream/pull/1117){.external-link target="_blank"}

#### Bug fixes

* Fix minor typos in documentation and code  by [@mj0nez](https://github.com/mj0nez){.external-link target="_blank"} in [#1116](https://github.com/airtai/faststream/pull/1116){.external-link target="_blank"}

### New Contributors
* [@mj0nez](https://github.com/mj0nez){.external-link target="_blank"} made their first contribution in [#1116](https://github.com/airtai/faststream/pull/1116){.external-link target="_blank"}

**Full Changelog**: [#0.3.12...0.3.13](https://github.com/airtai/faststream/compare/0.3.12...0.3.13){.external-link target="_blank"}

## 0.3.12

### What's Changed

#### Bug fixes

* fix (#1110): correct RMQ Topic pattern test publish by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1112](https://github.com/airtai/faststream/pull/1112){.external-link target="_blank"}

#### Misc

* chore: upgraded packages, black replaced with ruff format by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1097](https://github.com/airtai/faststream/pull/1097){.external-link target="_blank"}
* chore: upgraded packages by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1111](https://github.com/airtai/faststream/pull/1111){.external-link target="_blank"}


**Full Changelog**: [#0.3.11...0.3.12](https://github.com/airtai/faststream/compare/0.3.11...0.3.12){.external-link target="_blank"}

## 0.3.11

### What's Changed

NATS concurrent subscriber:

By default,  NATS subscriber consumes messages with a block per subject. So, you can't process multiple messages from the same subject at the same time. But, with the `broker.subscriber(..., max_workers=...)` option, you can! It creates an async tasks pool to consume multiple messages from the same subject and allows you to process them concurrently!

```python
from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStream()

@broker.subscriber("test-subject", max_workers=10)
async def handler(...):
   """Can process up to 10 messages in the same time."""
```

* Update Release Notes for 0.3.10 by @faststream-release-notes-updater in [#1091](https://github.com/airtai/faststream/pull/1091){.external-link target="_blank"}
* fix (#1100): FastAPI 0.106 compatibility by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1102](https://github.com/airtai/faststream/pull/1102){.external-link target="_blank"}

**Full Changelog**: [#0.3.10...0.3.11](https://github.com/airtai/faststream/compare/0.3.10...0.3.11){.external-link target="_blank"}

## 0.3.10

### What's Changed

#### New features

* feat: Context initial option by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1086](https://github.com/airtai/faststream/pull/1086){.external-link target="_blank"}

#### Bug fixes

* fix (#1087): add app_dir option to docs serve/gen commands by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1088](https://github.com/airtai/faststream/pull/1088){.external-link target="_blank"}

#### Documentation

* docs: add Context initial section by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1089](https://github.com/airtai/faststream/pull/1089){.external-link target="_blank"}

#### Other

* chore: linting by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1081](https://github.com/airtai/faststream/pull/1081){.external-link target="_blank"}
* chore: delete accidentally added .bak file by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1085](https://github.com/airtai/faststream/pull/1085){.external-link target="_blank"}

**Full Changelog**: [#0.3.9...0.3.10](https://github.com/airtai/faststream/compare/0.3.9...0.3.10){.external-link target="_blank"}

## 0.3.9

### What's Changed

#### Bug fixes:

* fix (#1082): correct NatsTestClient stream publisher by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1083](https://github.com/airtai/faststream/pull/1083){.external-link target="_blank"}

#### Chore:

* chore: adding pragmas for detect-secrets by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1080](https://github.com/airtai/faststream/pull/1080){.external-link target="_blank"}


**Full Changelog**: [#0.3.8...0.3.9](https://github.com/airtai/faststream/compare/0.3.8...0.3.9){.external-link target="_blank"}

## 0.3.8

### What's Changed

* bug: Fix `faststream.redis.fastapi.RedisRouter` stream and list subscription
* bug: Fix `TestNatsClient` with `batch=True`
* chore: add citation file by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1061](https://github.com/airtai/faststream/pull/1061){.external-link target="_blank"}
* docs: remove pragma comments by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1063](https://github.com/airtai/faststream/pull/1063){.external-link target="_blank"}
* docs: update README by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1064](https://github.com/airtai/faststream/pull/1064){.external-link target="_blank"}
* chore: increase rate limit and max connections by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1070](https://github.com/airtai/faststream/pull/1070){.external-link target="_blank"}
* chore: packages updated by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1076](https://github.com/airtai/faststream/pull/1076){.external-link target="_blank"}
* tests (#570): cover docs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1077](https://github.com/airtai/faststream/pull/1077){.external-link target="_blank"}

**Full Changelog**: [#0.3.7...0.3.8](https://github.com/airtai/faststream/compare/0.3.7...0.3.8){.external-link target="_blank"}

## 0.3.7

### What's Changed

* feat (#974): add FastAPI Context by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1060](https://github.com/airtai/faststream/pull/1060){.external-link target="_blank"}
* chore: update pre-commit by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1058](https://github.com/airtai/faststream/pull/1058){.external-link target="_blank"}

Support regular FastStream Context with FastAPI plugin

```python
from fastapi import FastAPI
from faststream.redis.fastapi import RedisRouter, Logger

router = RedisRouter()

@router.subscriber("test")
async def handler(msg, logger: Logger):
    logger.info(msg)

app = FastAPI(lifespan=router.lifespan_context)
app.include_router(router)
```

**Full Changelog**: [#0.3.6...0.3.7](https://github.com/airtai/faststream/compare/0.3.6...0.3.7){.external-link target="_blank"}

## 0.3.6

### What's Changed

* chore: correct update release CI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1050](https://github.com/airtai/faststream/pull/1050){.external-link target="_blank"}
* Update Release Notes for main by [@faststream](https://github.com/faststream){.external-link target="_blank"}-release-notes-updater in [#1051](https://github.com/airtai/faststream/pull/1051){.external-link target="_blank"}
* chore: fix building docs script by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1055](https://github.com/airtai/faststream/pull/1055){.external-link target="_blank"}
* 0.3.6 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1056](https://github.com/airtai/faststream/pull/1056){.external-link target="_blank"}
  * bug: remove `packaging` dependency
  * bug: correct **FastAPI** batch consuming
  * docs: add search meta to all pages
  * docs: polish all pages styles, fix typos
  * chore: add ruff rule to check print

**Full Changelog**: [#0.3.5...0.3.6](https://github.com/airtai/faststream/compare/0.3.5...0.3.6){.external-link target="_blank"}

## 0.3.5

### What's Changed

A large update by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1048](https://github.com/airtai/faststream/pull/1048){.external-link target="_blank"}

Provides with the ability to setup `graceful_timeout` to wait for consumed messages processed correctly before application shutdown - `#!python Broker(graceful_timeout=30.0)` (waits up to `#!python 30` seconds)

* allows to get access to `#!python context.get_local("message")` from **FastAPI** plugin
* docs: fix Avro custom serialization example
* docs: add KafkaBroker `publish_batch` notice
* docs: add RabbitMQ security page
* fix: respect retry attempts with `NackMessage` exception
* test Kafka nack and reject behavior
* bug: fix import error with anyio version 4.x by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1049](https://github.com/airtai/faststream/pull/1049){.external-link target="_blank"}

**Full Changelog**: [#0.3.4...0.3.5](https://github.com/airtai/faststream/compare/0.3.4...0.3.5){.external-link target="_blank"}

## 0.3.4

### What's Changed

#### Features:

* feat: add support for anyio 4.x by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1044](https://github.com/airtai/faststream/pull/1044){.external-link target="_blank"}

#### Documentation

* docs: add multiple FastAPI routers section by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1041](https://github.com/airtai/faststream/pull/1041){.external-link target="_blank"}

#### Chore

* chore: updated release notes by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1040](https://github.com/airtai/faststream/pull/1040){.external-link target="_blank"}
* chore: use Github App to generate token for release notes PR by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1043](https://github.com/airtai/faststream/pull/1043){.external-link target="_blank"}

**Full Changelog**: [#0.3.3...0.3.4](https://github.com/airtai/faststream/compare/0.3.3...0.3.4){.external-link target="_blank"}

## 0.3.3

### What's Changed

Features:

* feat: add support for Python 3.12 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1034](https://github.com/airtai/faststream/pull/1034){.external-link target="_blank"}

Chores:

* chore: updated release notes and upgraded packages by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1029](https://github.com/airtai/faststream/pull/1029){.external-link target="_blank"}

**Full Changelog**: [#0.3.2...0.3.3](https://github.com/airtai/faststream/compare/0.3.2...0.3.3){.external-link target="_blank"}

## 0.3.2

### What's Changed

#### New features:

* feat: add Redis security configuration by [@sternakt](https://github.com/sternakt){.external-link target="_blank"} and [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1025](https://github.com/airtai/faststream/pull/1025){.external-link target="_blank"}
* feat: add list of Messages NATS PullSub by [@SepehrBazyar](https://github.com/SepehrBazyar){.external-link target="_blank"} in [#1023](https://github.com/airtai/faststream/pull/1023){.external-link target="_blank"}

#### Chore:

* chore: polishing by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1016](https://github.com/airtai/faststream/pull/1016){.external-link target="_blank"}
* chore: update release notes by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#1017](https://github.com/airtai/faststream/pull/1017){.external-link target="_blank"}
* chore: bump pytest-asyncio from 0.21.1 to 0.23.2 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#1019](https://github.com/airtai/faststream/pull/1019){.external-link target="_blank"}
* chore: bump semgrep from 1.50.0 to 1.51.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [#1018](https://github.com/airtai/faststream/pull/1018){.external-link target="_blank"}
* chore: add pull_request permission to workflow by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1022](https://github.com/airtai/faststream/pull/1022){.external-link target="_blank"}


**Full Changelog**: [#0.3.1...0.3.2](https://github.com/airtai/faststream/compare/0.3.1...0.3.2){.external-link target="_blank"}

## 0.3.1

### What's Changed

Features:

* feat: added reply-to delivery mode for RabbitMQ by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1015](https://github.com/airtai/faststream/pull/1015){.external-link target="_blank"}

Bug fixes:

* fix: non-payload information injected included in AsyncAPI docs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1015](https://github.com/airtai/faststream/pull/1015){.external-link target="_blank"}

Documentation:

* docs: fix misspelled FastDepends reference in README.md by @spectacularfailure in [#1013](https://github.com/airtai/faststream/pull/1013){.external-link target="_blank"}

### New Contributors

* @spectacularfailure made their first contribution in [#1013](https://github.com/airtai/faststream/pull/1013){.external-link target="_blank"}

**Full Changelog**: [#0.3.0...0.3.1](https://github.com/airtai/faststream/compare/0.3.0...0.3.1){.external-link target="_blank"}

## 0.3.0

### What's Changed

The main feature of the 0.3.0 release is added Redis support by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1003](https://github.com/airtai/faststream/pull/1003){.external-link target="_blank"}

You can install it by the following command:

```bash
pip install "faststream[redis]"
```

Here is a little code example

```python
from faststream import FastStream, Logger
from faststream.redis import RedisBroker

broker = RedisBroker()
app = FastStream(broker)

@broker.subscriber(
    channel="test",  # or
    # list="test",     or
    # stream="test",
)
async def handle(msg: str, logger: Logger):
    logger.info(msg)
```

#### Other features

* feat: show reload directories with `--reload` flag by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#981](https://github.com/airtai/faststream/pull/981){.external-link target="_blank"}
* feat: implement validate and no_ack subscriber options (#926) by [@mihail8531](https://github.com/mihail8531){.external-link target="_blank"} in [#988](https://github.com/airtai/faststream/pull/988){.external-link target="_blank"}
* other features by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1003](https://github.com/airtai/faststream/pull/1003){.external-link target="_blank"}
    * Improve error logs (missing CLI arguments, undefined starting)
    * Add `faststream docs serve --reload ...` option for documentation hotreload
    * Add `faststream run --reload-extension .env` option to watch by changes in such files
    * Support `faststream run -k 1 -k 2 ...` as `k=["1", "2"]` extra options
    * Add subscriber, publisher and router `include_in_schema: bool` argument to disable **AsyncAPI** render
    * remove `watchfiles` from default distribution
    * Allow create `#!python broker.publisher(...)` with already running broker
    * **FastAPI**-like lifespan `FastStream` application context manager
    * automatic `TestBroker(connect_only=...)` argument based on AST
    * add `NatsMessage.in_progress()` method

#### Testing

* test: improve coverage by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#983](https://github.com/airtai/faststream/pull/983){.external-link target="_blank"}

#### Documentation

* docs: fix module name in NATS example by [@SepehrBazyar](https://github.com/SepehrBazyar){.external-link target="_blank"} in [#993](https://github.com/airtai/faststream/pull/993){.external-link target="_blank"}
* docs: Update docs to add  how to customize asyncapi docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#999](https://github.com/airtai/faststream/pull/999){.external-link target="_blank"}
* docs: polish Redis pages by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1005](https://github.com/airtai/faststream/pull/1005){.external-link target="_blank"}
* docs: bump docs to the new taskiq-faststream version by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1009](https://github.com/airtai/faststream/pull/1009){.external-link target="_blank"}

#### Chore

* chore: add broken link checker by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#985](https://github.com/airtai/faststream/pull/985){.external-link target="_blank"}
* chore: disable verbose in check broken links workflow by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#986](https://github.com/airtai/faststream/pull/986){.external-link target="_blank"}
* chore: add left out md files to fix broken links by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#987](https://github.com/airtai/faststream/pull/987){.external-link target="_blank"}
* chore: update mike workflow to use config by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#982](https://github.com/airtai/faststream/pull/982){.external-link target="_blank"}
* chore: add workflow to update release notes automatically by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#992](https://github.com/airtai/faststream/pull/992){.external-link target="_blank"}
* chore: pip packages version updated by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#998](https://github.com/airtai/faststream/pull/998){.external-link target="_blank"}
* chore: create PR to merge updated release notes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#1004](https://github.com/airtai/faststream/pull/1004){.external-link target="_blank"}

### New Contributors
* [@SepehrBazyar](https://github.com/SepehrBazyar){.external-link target="_blank"} made their first contribution in [#993](https://github.com/airtai/faststream/pull/993){.external-link target="_blank"}
* [@mihail8531](https://github.com/mihail8531){.external-link target="_blank"} made their first contribution in [#988](https://github.com/airtai/faststream/pull/988){.external-link target="_blank"}

**Full Changelog**: [#0.2.15...0.3.0](https://github.com/airtai/faststream/compare/0.2.15...0.3.0){.external-link target="_blank"}

## 0.3.0rc0

### What's Changed

The main feature of the 0.3.x release is added Redis support by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#1003](https://github.com/airtai/faststream/pull/1003){.external-link target="_blank"}

You can install it manually:

```bash
pip install faststream==0.3.0rc0 && pip install "faststream[redis]"
```

#### Other features

* feat: show reload directories with `--reload` flag by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#981](https://github.com/airtai/faststream/pull/981){.external-link target="_blank"}
* Improve error logs (missing CLI arguments, undefined starting)
* Add `faststream docs serve --reload ...` option for documentation hotreload
* Add `faststream run --reload-extension .env` option to watch by changes in such files
* Support `faststream run -k 1 -k 2 ...` as `k=["1", "2"]` extra options
* Add subscriber, publisher and router `include_in_schema: bool` argument to disable **AsyncAPI** render
* remove `watchfiles` from default distribution
* Allow create `#!python @broker.publisher(...)` with already running broker
* **FastAPI**-like lifespan `FastStream` application context manager
* automatic `TestBroker(connect_only=...)` argument based on AST
* add `NatsMessage.in_progress()` method

#### Testing

* test: improve coverage by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#983](https://github.com/airtai/faststream/pull/983){.external-link target="_blank"}

#### Documentation

* docs: fix module name in NATS example by [@SepehrBazyar](https://github.com/SepehrBazyar){.external-link target="_blank"} in [#993](https://github.com/airtai/faststream/pull/993){.external-link target="_blank"}
* docs: Update docs to add  how to customize asyncapi docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#999](https://github.com/airtai/faststream/pull/999){.external-link target="_blank"}

#### Chore

* chore: add broken link checker by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#985](https://github.com/airtai/faststream/pull/985){.external-link target="_blank"}
* chore: disable verbose in check broken links workflow by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#986](https://github.com/airtai/faststream/pull/986){.external-link target="_blank"}
* chore: add left out md files to fix broken links by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#987](https://github.com/airtai/faststream/pull/987){.external-link target="_blank"}
* chore: update mike workflow to use config by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [#982](https://github.com/airtai/faststream/pull/982){.external-link target="_blank"}
* chore: add workflow to update release notes automatically by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [#992](https://github.com/airtai/faststream/pull/992){.external-link target="_blank"}
* chore: pip packages version updated by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [#998](https://github.com/airtai/faststream/pull/998){.external-link target="_blank"}

### New Contributors

* [@SepehrBazyar](https://github.com/SepehrBazyar){.external-link target="_blank"} made their first contribution in [#993](https://github.com/airtai/faststream/pull/993){.external-link target="_blank"}

**Full Changelog**: [#0.2.15...0.3.0rc0](https://github.com/airtai/faststream/compare/0.2.15...0.3.0rc0){.external-link target="_blank"}

## 0.2.15

### What's Changed

#### Bug fixes

* fix (#972): correct Context default behavior by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/973](https://github.com/airtai/faststream/pull/973){.external-link target="_blank"}
* fix: correct CLI run by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/978](https://github.com/airtai/faststream/pull/978){.external-link target="_blank"}

#### Documentation

* docs: update readme docs link by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/966](https://github.com/airtai/faststream/pull/966){.external-link target="_blank"}
* docs: add a new landing page for docs by [@harishmohanraj](https://github.com/harishmohanraj){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/954](https://github.com/airtai/faststream/pull/954){.external-link target="_blank"}
* docs: Fix broken internal links by [@harishmohanraj](https://github.com/harishmohanraj){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/976](https://github.com/airtai/faststream/pull/976){.external-link target="_blank"}
* docs: use mkdocs footer by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/977](https://github.com/airtai/faststream/pull/977){.external-link target="_blank"}

#### Misc

* test (#957): add AsyncAPI FastAPI security test by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/958](https://github.com/airtai/faststream/pull/958){.external-link target="_blank"}
* test: update tests for cli utils functions by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/960](https://github.com/airtai/faststream/pull/960){.external-link target="_blank"}
* chore: update release notes for version 0.2.14 by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/961](https://github.com/airtai/faststream/pull/961){.external-link target="_blank"}
* chore: Add back deleted index file for API Reference by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/963](https://github.com/airtai/faststream/pull/963){.external-link target="_blank"}
* chore: bump dirty-equals from 0.6.0 to 0.7.1.post0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/970](https://github.com/airtai/faststream/pull/970){.external-link target="_blank"}
* chore: bump semgrep from 1.48.0 to 1.50.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/968](https://github.com/airtai/faststream/pull/968){.external-link target="_blank"}
* chore: bump mkdocs-glightbox from 0.3.4 to 0.3.5 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/967](https://github.com/airtai/faststream/pull/967){.external-link target="_blank"}
* chore: bump mkdocs-material from 9.4.8 to 9.4.10 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/971](https://github.com/airtai/faststream/pull/971){.external-link target="_blank"}
* chore: bump ruff from 0.1.5 to 0.1.6 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/969](https://github.com/airtai/faststream/pull/969){.external-link target="_blank"}


**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.14...0.2.15](https://github.com/airtai/faststream/compare/0.2.14...0.2.15){.external-link target="_blank"}

## 0.2.14

### What's Changed

#### Bug fixes

* fix: usage pass apps module rather than file path by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/955](https://github.com/airtai/faststream/pull/955){.external-link target="_blank"}
* fix: trigger docs deployment by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/944](https://github.com/airtai/faststream/pull/944){.external-link target="_blank"}

#### Documentation

* docs: reduce built docs size by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/952](https://github.com/airtai/faststream/pull/952){.external-link target="_blank"}
* docs: fix update_release script by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/945](https://github.com/airtai/faststream/pull/945){.external-link target="_blank"}

#### Misc

* chore: polishing by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/946](https://github.com/airtai/faststream/pull/946){.external-link target="_blank"}
* сhore: add manual publish btn to CI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/950](https://github.com/airtai/faststream/pull/950){.external-link target="_blank"}
* chore: limit open dev dependency versions by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/953](https://github.com/airtai/faststream/pull/953){.external-link target="_blank"}


**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.13...0.2.14](https://github.com/airtai/faststream/compare/0.2.13...0.2.14){.external-link target="_blank"}


## 0.2.13

### What's Changed

* chore: Remove uvloop python 3.12 restriction from pyproject by [@sternakt](https://github.com/sternakt){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/914](https://github.com/airtai/faststream/pull/914){.external-link target="_blank"}
* fix: mike deploy command by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/919](https://github.com/airtai/faststream/pull/919){.external-link target="_blank"}
* chore: update dependencies by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/920](https://github.com/airtai/faststream/pull/920){.external-link target="_blank"}
* chore: use dev dependencies to build docs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/921](https://github.com/airtai/faststream/pull/921){.external-link target="_blank"}
* chore: update packages' versions by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/937](https://github.com/airtai/faststream/pull/937){.external-link target="_blank"}
* fix: FastAPI subscriber Path support by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/931](https://github.com/airtai/faststream/pull/931){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.12...0.2.13](https://github.com/airtai/faststream/compare/0.2.12...0.2.13){.external-link target="_blank"}

## 0.2.12

### What's Changed
* feat: NATS polling subscriber by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/912](https://github.com/airtai/faststream/pull/912){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.11...0.2.12](https://github.com/airtai/faststream/compare/0.2.11...0.2.12){.external-link target="_blank"}

## 0.2.11

### What's Changed

#### Bug fixes

* fix (#910): correct pydantic enum refs resolving by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/911](https://github.com/airtai/faststream/pull/911){.external-link target="_blank"}

#### Documentation

* docs: update the number of lines of code referred to in the documentation by [@vvanglro](https://github.com/vvanglro){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/905](https://github.com/airtai/faststream/pull/905){.external-link target="_blank"}
* docs: add API reference in docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/891](https://github.com/airtai/faststream/pull/891){.external-link target="_blank"}
* docs: add release notes for version 0.2.10 by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/907](https://github.com/airtai/faststream/pull/907){.external-link target="_blank"}
* docs: detail 0.2.10 release note by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/908](https://github.com/airtai/faststream/pull/908){.external-link target="_blank"}
* docs: proofread and update 0.2.10 release notes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/909](https://github.com/airtai/faststream/pull/909){.external-link target="_blank"}

### New Contributors
* [@vvanglro](https://github.com/vvanglro){.external-link target="_blank"} made their first contribution in [https://github.com/airtai/faststream/pull/905](https://github.com/airtai/faststream/pull/905){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.10...0.2.11](https://github.com/airtai/faststream/compare/0.2.10...0.2.11){.external-link target="_blank"}

* fix (#910): correct pydantic enum refs resolving by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/911](https://github.com/airtai/faststream/pull/911){.external-link target="_blank"}

#### Documentation

* docs: update the number of lines of code referred to in the documentation by [@vvanglro](https://github.com/vvanglro){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/905](https://github.com/airtai/faststream/pull/905){.external-link target="_blank"}
* docs: add API reference in docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/891](https://github.com/airtai/faststream/pull/891){.external-link target="_blank"}
* docs: add release notes for version 0.2.10 by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/907](https://github.com/airtai/faststream/pull/907){.external-link target="_blank"}
* docs: detail 0.2.10 release note by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/908](https://github.com/airtai/faststream/pull/908){.external-link target="_blank"}
* docs: proofread and update 0.2.10 release notes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/909](https://github.com/airtai/faststream/pull/909){.external-link target="_blank"}

### New Contributors
* [@vvanglro](https://github.com/vvanglro){.external-link target="_blank"} made their first contribution in [https://github.com/airtai/faststream/pull/905](https://github.com/airtai/faststream/pull/905){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.10...0.2.11](https://github.com/airtai/faststream/compare/0.2.10...0.2.11){.external-link target="_blank"}

## 0.2.10

### What's Changed

Now, you can hide your connection secrets in the **AsyncAPI** schema by manually setting up the server URL:

```python
broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",  # Connection URL
    asyncapi_url="amqp://****:****@localhost:5672/",  # Public schema URL
)
```

Additionally, the **RabbitMQ AsyncAPI** schema has been improved, adding support for `faststream.security`, and the connection scheme is now defined automatically.

**RabbitMQ** connection parameters are now merged, allowing you to define the main connection data as a URL string and customize it using kwargs:

```python
broker = RabbitBroker(
    "amqp://guest:guest@localhost:5672/",
    host="127.0.0.1",
)

# amqp://guest:guest@127.0.0.1:5672/ - The final URL
```
* A more suitable `faststream.security` import instead of `faststream.broker.security`
* chore: add release notes for 0.2.9 by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/894](https://github.com/airtai/faststream/pull/894){.external-link target="_blank"}
* chore: upgrade packages by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/901](https://github.com/airtai/faststream/pull/901){.external-link target="_blank"}
* chore: use js redirect and redirect to version by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/902](https://github.com/airtai/faststream/pull/902){.external-link target="_blank"}
* feat: add `asyncapi_url` broker arg by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/903](https://github.com/airtai/faststream/pull/903){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.9...0.2.10](https://github.com/airtai/faststream/compare/0.2.9...0.2.10){.external-link target="_blank"}

## 0.2.9

### What's Changed
* docs: fix grammatical errors in README.md by [@JanumalaAkhilendra](https://github.com/JanumalaAkhilendra){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/880](https://github.com/airtai/faststream/pull/880){.external-link target="_blank"}
* chore: update release notes by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/881](https://github.com/airtai/faststream/pull/881){.external-link target="_blank"}
* docs: use meta tag for redirect by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/886](https://github.com/airtai/faststream/pull/886){.external-link target="_blank"}
* chore: semgrep upgrade by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/888](https://github.com/airtai/faststream/pull/888){.external-link target="_blank"}
* docs: update README.md by [@bhargavshirin](https://github.com/bhargavshirin){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/889](https://github.com/airtai/faststream/pull/889){.external-link target="_blank"}
* fix (#892): use normalized subjects in NATS streams by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/893](https://github.com/airtai/faststream/pull/893){.external-link target="_blank"}

### New Contributors
* [@JanumalaAkhilendra](https://github.com/JanumalaAkhilendra){.external-link target="_blank"} made their first contribution in [https://github.com/airtai/faststream/pull/880](https://github.com/airtai/faststream/pull/880){.external-link target="_blank"}
* [@bhargavshirin](https://github.com/bhargavshirin){.external-link target="_blank"} made their first contribution in [https://github.com/airtai/faststream/pull/889](https://github.com/airtai/faststream/pull/889){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.8...0.2.9](https://github.com/airtai/faststream/compare/0.2.8...0.2.9){.external-link target="_blank"}

## 0.2.8

### What's Changed
* fix: FASTAPI_V2 always True by [@shepilov](https://github.com/shepilov){.external-link target="_blank"}-vladislav in [https://github.com/airtai/faststream/pull/877](https://github.com/airtai/faststream/pull/877){.external-link target="_blank"}
* feat: better RMQ AsyncAPI by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/879](https://github.com/airtai/faststream/pull/879){.external-link target="_blank"}

### New Contributors
* [@shepilov](https://github.com/shepilov){.external-link target="_blank"}-vladislav made their first contribution in [https://github.com/airtai/faststream/pull/877](https://github.com/airtai/faststream/pull/877){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.7...0.2.8](https://github.com/airtai/faststream/compare/0.2.7...0.2.8){.external-link target="_blank"}


## 0.2.7

### What's Changed
* fix: ImportError: typing 'override' from 'faststream._compat' (python 3.12) by [@Jaroslav2001](https://github.com/Jaroslav2001){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/870](https://github.com/airtai/faststream/pull/870){.external-link target="_blank"}
* fix: remove jsonref dependency by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/873](https://github.com/airtai/faststream/pull/873){.external-link target="_blank"}


**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.6...0.2.7](https://github.com/airtai/faststream/compare/0.2.6...0.2.7){.external-link target="_blank"}

## 0.2.6

### What's Changed
* docs: add avro encoding, decoding examples by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/844](https://github.com/airtai/faststream/pull/844){.external-link target="_blank"}
* docs: fix typo in README.md by [@omimakhare](https://github.com/omimakhare){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/849](https://github.com/airtai/faststream/pull/849){.external-link target="_blank"}
* fix: update mypy, semgrep versions and fix arg-type mypy error by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/851](https://github.com/airtai/faststream/pull/851){.external-link target="_blank"}
* docs: fix typo by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/859](https://github.com/airtai/faststream/pull/859){.external-link target="_blank"}
* docs: detail Release Notes by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/855](https://github.com/airtai/faststream/pull/855){.external-link target="_blank"}
* docs: write documentation for kafka security by [@sternakt](https://github.com/sternakt){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/860](https://github.com/airtai/faststream/pull/860){.external-link target="_blank"}
* docs: asyncapi tool config added by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/861](https://github.com/airtai/faststream/pull/861){.external-link target="_blank"}
* docs: retain GET params while redirecting by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/862](https://github.com/airtai/faststream/pull/862){.external-link target="_blank"}
* docs: add article for using FastStream with Django by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/864](https://github.com/airtai/faststream/pull/864){.external-link target="_blank"}
* chore: discord invite link changed by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/863](https://github.com/airtai/faststream/pull/863){.external-link target="_blank"}
* docs: add some Django integration details by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/866](https://github.com/airtai/faststream/pull/866){.external-link target="_blank"}
* fix: remove pydantic defs  in AsyncAPI schema by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/869](https://github.com/airtai/faststream/pull/869){.external-link target="_blank"}

### New Contributors
* [@omimakhare](https://github.com/omimakhare){.external-link target="_blank"} made their first contribution in [https://github.com/airtai/faststream/pull/849](https://github.com/airtai/faststream/pull/849){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.5...0.2.6](https://github.com/airtai/faststream/compare/0.2.5...0.2.6){.external-link target="_blank"}

## 0.2.5

### What's Changed

* fix: pass missing parameters and update docs by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/841](https://github.com/airtai/faststream/pull/841){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.4...0.2.5](https://github.com/airtai/faststream/compare/0.2.4...0.2.5){.external-link target="_blank"}

## 0.2.4

### New Functionalities

Now, `Context` provides access to inner [dict keys too](./getting-started/context/fields.md):

```python
# headers is a `dict`
async def handler(
  user_id: int = Context("message.headers.user_id", cast=True),
): ...
```

Added `Header` object as a shortcut to `#!python Context("message.headers.")` inner fields (**NATS** [example](./nats/message.md#headers-access)):

```python
# the same with the previous example
async def handler(
  user_id: int = Header(),
  u_id: int = Header("user_id"),  # with custom name
): ...
```

Added `Path` object to get access to [**NATS** wildcard](./nats/message.md#subject-pattern-access) subject or [**RabbitMQ** topic](./rabbit/message.md#topic-pattern-access) routing key (a shortcut to access `#!python Context("message.path.")` as well):

```python
@nats_broker.subscriber("logs.{level}")
async def handler(
  level: str = Path(),
)
```

Also, the original message `Context` annotation was copied from `faststream.[broker].annotations.[Broker]Message` to `faststream.[broker].[Broker]Message` to provide you with faster access to the most commonly used object (**NATS** [example](./nats/message.md#message-access)).

### What's Changed

* Remove faststream_gen docs and remove code to generate faststream_gen docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/824](https://github.com/airtai/faststream/pull/824){.external-link target="_blank"}
* Update docs article to use cookiecutter template by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/828](https://github.com/airtai/faststream/pull/828){.external-link target="_blank"}
* Split real broker tests to independent runs by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/825](https://github.com/airtai/faststream/pull/825){.external-link target="_blank"}
* Remove unused docs/docs_src/kafka examples and its tests by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/829](https://github.com/airtai/faststream/pull/829){.external-link target="_blank"}
* Run docs deployment only for specific file changes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/830](https://github.com/airtai/faststream/pull/830){.external-link target="_blank"}
* Fix formatting in deploy docs workflow by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/833](https://github.com/airtai/faststream/pull/833){.external-link target="_blank"}
* Path operations by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/823](https://github.com/airtai/faststream/pull/823){.external-link target="_blank"}
* Mypy error fixed for uvloop by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/839](https://github.com/airtai/faststream/pull/839){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.3...0.2.4](https://github.com/airtai/faststream/compare/0.2.3...0.2.4){.external-link target="_blank"}

## 0.2.3

### What's Changed

* Fix: disable test features with TestClient by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/813](https://github.com/airtai/faststream/pull/813){.external-link target="_blank"}
* New AsyncAPI naming by [@Sternakt](https://github.com/Sternakt){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/735](https://github.com/airtai/faststream/pull/735){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.2...0.2.3](https://github.com/airtai/faststream/compare/0.2.2...0.2.3){.external-link target="_blank"}

## 0.2.2

### What's Changed

* Adds specific mypy ignore comment by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/803](https://github.com/airtai/faststream/pull/803){.external-link target="_blank"}
* Adds redirect template with mike by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/808](https://github.com/airtai/faststream/pull/808){.external-link target="_blank"}
* Adds google analytics script to redirect template by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/809](https://github.com/airtai/faststream/pull/809){.external-link target="_blank"}
* Adds conditional import of uvloop for Python versions less than 3.12 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/798](https://github.com/airtai/faststream/pull/798){.external-link target="_blank"}
* Adds missing nats imports by [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/795](https://github.com/airtai/faststream/pull/795){.external-link target="_blank"}
* Adds Kafka acknowledgement by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/793](https://github.com/airtai/faststream/pull/793){.external-link target="_blank"}

### New Contributors

* [@sheldygg](https://github.com/sheldygg){.external-link target="_blank"} made their first contribution in [https://github.com/airtai/faststream/pull/795](https://github.com/airtai/faststream/pull/795){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.1...0.2.2](https://github.com/airtai/faststream/compare/0.2.1...0.2.2){.external-link target="_blank"}

## 0.2.1

### What's Changed

* Add custom 404 error page by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/792](https://github.com/airtai/faststream/pull/792){.external-link target="_blank"}
* Add README NATS mention by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/788](https://github.com/airtai/faststream/pull/788){.external-link target="_blank"}
* Conditional import of uvloop for Python versions less than 3.12 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/798](https://github.com/airtai/faststream/pull/798){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.2.0...0.2.1](https://github.com/airtai/faststream/compare/0.2.0...0.2.1){.external-link target="_blank"}

## 0.2.0

### What's Changed

* Add comprehensive guide on how to use faststream template by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/772](https://github.com/airtai/faststream/pull/772){.external-link target="_blank"}
* Open external links in new tab by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/774](https://github.com/airtai/faststream/pull/774){.external-link target="_blank"}
* Publish docs for minor version not for every patch by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/777](https://github.com/airtai/faststream/pull/777){.external-link target="_blank"}
* Complete Kafka part of faststream docs by [@Sternakt](https://github.com/Sternakt){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/775](https://github.com/airtai/faststream/pull/775){.external-link target="_blank"}
* Bump semgrep from 1.41.0 to 1.42.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/787](https://github.com/airtai/faststream/pull/787){.external-link target="_blank"}
* Add 0.2.0 NATS support by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/692](https://github.com/airtai/faststream/pull/692){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.1.6...0.2.0](https://github.com/airtai/faststream/compare/0.1.6...0.2.0){.external-link target="_blank"}

## 0.1.6

### What's Changed

* Add coverage badge at docs index by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/762](https://github.com/airtai/faststream/pull/762){.external-link target="_blank"}
* Fill asyncapi custom information page by [@Sternakt](https://github.com/Sternakt){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/767](https://github.com/airtai/faststream/pull/767){.external-link target="_blank"}
* Add article for using faststream template by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/768](https://github.com/airtai/faststream/pull/768){.external-link target="_blank"}
* Use httpx instead of requests by [@rjambrecic](https://github.com/rjambrecic){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/771](https://github.com/airtai/faststream/pull/771){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.1.5...0.1.6](https://github.com/airtai/faststream/compare/0.1.5...0.1.6){.external-link target="_blank"}

## 0.1.4

### What's Changed

* tiny typo by [@julzhk](https://github.com/julzhk){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/740](https://github.com/airtai/faststream/pull/740){.external-link target="_blank"}
* docs: add docs mention by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/744](https://github.com/airtai/faststream/pull/744){.external-link target="_blank"}
* Add code of conduct and include badge for it in README by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/747](https://github.com/airtai/faststream/pull/747){.external-link target="_blank"}
* Fixed docs building when pydantic version less than 2.4.0 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/748](https://github.com/airtai/faststream/pull/748){.external-link target="_blank"}
* fix: raise inner exceptions in `with_real` tests by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/751](https://github.com/airtai/faststream/pull/751){.external-link target="_blank"}
* docs fix by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/752](https://github.com/airtai/faststream/pull/752){.external-link target="_blank"}
* Bugfixes 745 by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/749](https://github.com/airtai/faststream/pull/749){.external-link target="_blank"}

### New Contributors

* [@julzhk](https://github.com/julzhk){.external-link target="_blank"} made their first contribution in [https://github.com/airtai/faststream/pull/740](https://github.com/airtai/faststream/pull/740){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.1.3...0.1.4](https://github.com/airtai/faststream/compare/0.1.3...0.1.4){.external-link target="_blank"}

## 0.1.3

### What's Changed

* docs: fix styles by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/717](https://github.com/airtai/faststream/pull/717){.external-link target="_blank"}
* test (#638): extra AsyncAPI channel naming test by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/719](https://github.com/airtai/faststream/pull/719){.external-link target="_blank"}
* test: cover docs_src/context by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/723](https://github.com/airtai/faststream/pull/723){.external-link target="_blank"}
* library to framework changed by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/724](https://github.com/airtai/faststream/pull/724){.external-link target="_blank"}
* Create templates for issues and pull requests by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/727](https://github.com/airtai/faststream/pull/727){.external-link target="_blank"}
* Bump actions/dependency-review-action from 2 to 3 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/728](https://github.com/airtai/faststream/pull/728){.external-link target="_blank"}
* Bump actions/cache from 2 to 3 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/729](https://github.com/airtai/faststream/pull/729){.external-link target="_blank"}
* Bump semgrep from 1.40.0 to 1.41.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/732](https://github.com/airtai/faststream/pull/732){.external-link target="_blank"}
* Bump ruff from 0.0.290 to 0.0.291 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/733](https://github.com/airtai/faststream/pull/733){.external-link target="_blank"}
* Polish contributing file and remove duplicate docker compose file by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/734](https://github.com/airtai/faststream/pull/734){.external-link target="_blank"}
* Bump dawidd6/action-download-artifact from 2.26.0 to 2.28.0 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/731](https://github.com/airtai/faststream/pull/731){.external-link target="_blank"}
* Bump actions/checkout from 3 to 4 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/730](https://github.com/airtai/faststream/pull/730){.external-link target="_blank"}
* Pydantic2.4.0 compat by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/738](https://github.com/airtai/faststream/pull/738){.external-link target="_blank"}
* fix: add url option to _connection_args by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/739](https://github.com/airtai/faststream/pull/739){.external-link target="_blank"}
* Fix typos and grammar in Kafka and RabbitMQ articles in the docs by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/736](https://github.com/airtai/faststream/pull/736){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/compare/0.1.1...0.1.3](https://github.com/airtai/faststream/compare/0.1.1...0.1.3){.external-link target="_blank"}

## 0.1.1

### What's Changed

* Bump ruff from 0.0.289 to 0.0.290 by [@dependabot](https://github.com/dependabot){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/672](https://github.com/airtai/faststream/pull/672){.external-link target="_blank"}
* Make docs port configurable in serve-docs.sh by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/675](https://github.com/airtai/faststream/pull/675){.external-link target="_blank"}
* Fix docs img by [@Sternakt](https://github.com/Sternakt){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/673](https://github.com/airtai/faststream/pull/673){.external-link target="_blank"}
* Added release notes by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/679](https://github.com/airtai/faststream/pull/679){.external-link target="_blank"}
* Fix typos, grammar mistakes in index and README by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/681](https://github.com/airtai/faststream/pull/681){.external-link target="_blank"}
* Add smokeshow workflow to update coverage badge by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/687](https://github.com/airtai/faststream/pull/687){.external-link target="_blank"}
* fix: correct rmq delayed handler router registration by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/691](https://github.com/airtai/faststream/pull/691){.external-link target="_blank"}
* Add faststream-gen section and crypto tutorial in Getting started by [@rjambrecic](https://github.com/rjambrecic){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/689](https://github.com/airtai/faststream/pull/689){.external-link target="_blank"}
* Fix typos and grammar mistakes by [@kumaranvpl](https://github.com/kumaranvpl){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/699](https://github.com/airtai/faststream/pull/699){.external-link target="_blank"}
* fix: correct StreamRouter broker annotation by [@Lancetnik](https://github.com/Lancetnik){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/700](https://github.com/airtai/faststream/pull/700){.external-link target="_blank"}
* typos fixed by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/701](https://github.com/airtai/faststream/pull/701){.external-link target="_blank"}
* Add faststream-gen section inside the README.md by [@rjambrecic](https://github.com/rjambrecic){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/707](https://github.com/airtai/faststream/pull/707){.external-link target="_blank"}
* Fix broken links in README file by [@harishmohanraj](https://github.com/harishmohanraj){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/706](https://github.com/airtai/faststream/pull/706){.external-link target="_blank"}
* publish to PyPi added to CI by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/710](https://github.com/airtai/faststream/pull/710){.external-link target="_blank"}
* Fix example and async docs images by [@Sternakt](https://github.com/Sternakt){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/713](https://github.com/airtai/faststream/pull/713){.external-link target="_blank"}
* 696 add example to faststream gen examples which uses datetime attribute by [@rjambrecic](https://github.com/rjambrecic){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/714](https://github.com/airtai/faststream/pull/714){.external-link target="_blank"}
* release 0.1.1 by [@davorrunje](https://github.com/davorrunje){.external-link target="_blank"} in [https://github.com/airtai/faststream/pull/715](https://github.com/airtai/faststream/pull/715){.external-link target="_blank"}

**Full Changelog**: [https://github.com/airtai/faststream/commits/0.1.1](https://github.com/airtai/faststream/commits/0.1.1){.external-link target="_blank"}

## 0.1.0

**FastStream** is a new package based on the ideas and experiences gained from [FastKafka](https://github.com/airtai/fastkafka){.external-link target="_blank"} and [Propan](https://github.com/lancetnik/propan){.external-link target="_blank"}. By joining our forces, we picked up the best from both packages and created the unified way to write services capable of processing streamed data regardless of the underlying protocol. We'll continue to maintain both packages, but new development will be in this project. If you are starting a new service, this package is the recommended way to do it.

### Features

[**FastStream**](https://faststream.airt.ai/latest/) simplifies the process of writing producers and consumers for message queues, handling all the
parsing, networking and documentation generation automatically.

Making streaming microservices has never been easier. Designed with junior developers in mind, **FastStream** simplifies your work while keeping the door open for more advanced use-cases. Here's a look at the core features that make **FastStream** a go-to framework for modern, data-centric microservices.

* **Multiple Brokers**: **FastStream** provides a unified API to work across multiple message brokers (**Kafka**, **RabbitMQ** support)

* [**Pydantic Validation**](./faststream.md/#writing-app-code): Leverage [**Pydantic's**](https://docs.pydantic.dev/){.external-link target="_blank"} validation capabilities to serialize and validates incoming messages

* [**Automatic Docs**](./faststream.md/#project-documentation): Stay ahead with automatic [AsyncAPI](https://www.asyncapi.com/){.external-link target="_blank"} documentation.

* **Intuitive**: full typed editor support makes your development experience smooth, catching errors before they reach runtime

* [**Powerful Dependency Injection System**](./faststream.md/#dependencies): Manage your service dependencies efficiently with **FastStream**'s built-in DI system.

* [**Testable**](./faststream.md/#testing-the-service): supports in-memory tests, making your CI/CD pipeline faster and more reliable

* **Extendable**: use extensions for lifespans, custom serialization and middlewares

* [**Integrations**](./faststream.md/#any-framework): **FastStream** is fully compatible with any HTTP framework you want ([**FastAPI**](./faststream.md/#fastapi-plugin) especially)

* **Built for Automatic Code Generation**: **FastStream** is optimized for automatic code generation using advanced models like GPT and Llama

That's **FastStream** in a nutshell—easy, efficient, and powerful. Whether you're just starting with streaming microservices or looking to scale, **FastStream** has got you covered.
