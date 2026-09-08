---
search:
  boost: 2
---

# Projects using FastStream

Below are open-source projects and organizations whose public repositories or published packages
declare **FastStream** as a dependency, together with tools that ship a **FastStream** integration
of their own.

!!! note "About this list"
    This list is compiled from public repositories and package metadata. Inclusion does not imply
    endorsement, support, or any affiliation with the **FastStream** project. If your organization
    is listed and you would prefer not to be, [open an issue](https://github.com/ag2ai/faststream/issues/new){.external-link target="_blank"}
    and we will remove the entry.

## Science and Public Sector

| Name | Who they are | Project |
| --- | --- | --- |
| **ECMWF** | European Centre for Medium-Range Weather Forecasts | [IonBeam](https://github.com/ecmwf/IonBeam){.external-link target="_blank"} — streaming library for IoT data |
| **Hydro-Québec** | Quebec's public electricity utility | [building-intelligence](https://github.com/hq-opensource/building-intelligence){.external-link target="_blank"} — building data middleware, four services, plus predictive control |
| **it@M** | IT services provider of the City of Munich | [zammad-ai](https://github.com/it-at-m/zammad-ai){.external-link target="_blank"} — GenAI agent for Zammad, and [riski](https://github.com/it-at-m/riski){.external-link target="_blank"} |
| **Rubin Observatory / LSST** | Science Quality and Reliability Engineering (SQuaRE) team | [Safir](https://github.com/lsst-sqre/safir){.external-link target="_blank"} — their framework for FastAPI services |
| **NCATS (NIH) / PolusAI** | National Center for Advancing Translational Sciences | [aithena](https://github.com/PolusAI/aithena){.external-link target="_blank"} — RAG systems |
| **Catasto Open** | Italian land registry | [catasto-cdc](https://github.com/catasto-open/catasto-cdc){.external-link target="_blank"} — change data capture for property changes |
| **Data Cellar** | EU federated energy dataspace | [participant-template](https://github.com/Data-Cellar/participant-template){.external-link target="_blank"} — participant onboarding services |
| **LMDDC** | Luxembourg Media & Digital Design Centre | [alice.skilltech.tools](https://github.com/lmddc-lu/alice.skilltech.tools){.external-link target="_blank"} — educational chatbots grounded in course materials |
| **NHS Lancashire & South Cumbria SDE** | NHS secure data environment | [neulander](https://github.com/lsc-sde/neulander-core){.external-link target="_blank"} — three services |
| **IT'IS Foundation** | Foundation for Research on Information Technologies in Society | [osparc-simcore](https://github.com/ITISFoundation/osparc-simcore){.external-link target="_blank"} — simulation framework |
| **CTIC** | CTIC Technology Centre, Spain | [connector-building-blocks](https://github.com/fundacionctic/connector-building-blocks){.external-link target="_blank"} — Eclipse Dataspace Components tooling |
| **QCrBox** | Quantum Crystallography Toolbox | [QCrBox](https://github.com/QCrBox/QCrBox){.external-link target="_blank"} — small-molecule crystallography |
| **NERSC** | National Energy Research Scientific Computing Center, US DOE | [interactEM](https://github.com/NERSC/interactEM){.external-link target="_blank"} |
| **HBB (AI·SW Maestro 17th)** | Dev team in AI·SW Maestro, a software talent programme in Korea | [Kkori-AI](https://github.com/SW-Maestro-17th-HBB/Kkori-AI){.external-link target="_blank"} — AI worker of Kkori, an interview-prep service: analyses uploaded resumes and generates interview reports |

## Companies

| Name | Who they are | Project |
| --- | --- | --- |
| **Red Hat** | enterprise open-source vendor | [qontract-reconcile](https://github.com/app-sre/qontract-reconcile){.external-link target="_blank"} — App-SRE reconciliation tooling |
| **MWS** | MTS Web Services, digital product ecosystem | [data-rentgen](https://github.com/MTSWebServices/data-rentgen){.external-link target="_blank"} — data lineage |
| **Numberly** | marketing technology company | [reviewate](https://github.com/numberly/reviewate){.external-link target="_blank"} — AI code review agent |
| **Gravitate** | AI platform for the fuel supply chain | [bb-integrations-library](https://pypi.org/project/bb-integrations-library/){.external-link target="_blank"} — shared integration-job logic |
| **hao.vc** | builders of AI-autonomous orchestrators | [haolib](https://github.com/hao-vc/haolib){.external-link target="_blank"} — their default backend template |
| **KIWIQ** | multi-agent AI vendor | [kiwiq](https://github.com/rcortx/kiwiq){.external-link target="_blank"} — multi-agent orchestration platform |
| **Lemma** | runtime for agent-built software | [lemma-platform](https://github.com/lemma-work/lemma-platform){.external-link target="_blank"} — workspace where humans and AI agents work as one team |
| **spoo.me** | link management service | [spoo](https://github.com/spoo-me/spoo){.external-link target="_blank"} — API-first link management infrastructure |
| **EggAI** | agentic workforce automation | [EggAI](https://github.com/eggai-tech/EggAI){.external-link target="_blank"} — async-first multi-agent meta framework |
| **AgentArea** | control layer for agent teams | [agentarea](https://github.com/agentarea/agentarea){.external-link target="_blank"} — cloud-native AI agent orchestration |
| **xi.effect / Sovlium** | education platform | [xi.back-2](https://github.com/xi-effect/xi.back-2){.external-link target="_blank"} — main backend service |
| **Aeluin Technologies** | data integration and analytics vendor | [Galadril](https://github.com/Aeluin-Technologies/Galadril){.external-link target="_blank"} — causal analysis and system foresight platform |
| **Waldiez** | multi-agent AI orchestration platforms | [runner](https://github.com/waldiez/runner){.external-link target="_blank"} — deploys Waldiez flows |
| **traide AI** | AI for customs processes | [traide-core-python](https://github.com/traide/traide-core-python){.external-link target="_blank"} — shared library for their services |
| **TogetherCrew** | open-source community tooling | [hivemind-bot](https://github.com/TogetherCrew/hivemind-bot){.external-link target="_blank"} — their LLM bot |
| **TL;DR.tv** | TL;DR.tv platform team | [tldr-common](https://pypi.org/project/tldr-common/){.external-link target="_blank"} — shared utilities and models |

## Tools Shipping a FastStream Integration

These are not users but neighbouring projects that took on maintaining an integration of their own.

| Name | Who they are | Integration |
| --- | --- | --- |
| **Pydantic Logfire** | observability platform from the **Pydantic** team | [FastStream integration](https://logfire.pydantic.dev/docs/integrations/event-streams/faststream/){.external-link target="_blank"} — documented and covered by CI tests |
| **RabbitMQ** | message broker | [client libraries and developer tools](https://www.rabbitmq.com/client-libraries/devtools){.external-link target="_blank"} — **FastStream** listed among the clients in the official documentation |
| **EMQX** | MQTT broker | [MQTT-Client-Examples](https://github.com/emqx/MQTT-Client-Examples){.external-link target="_blank"} — `mqtt-client-Python-FastStream` in the official client examples |
| **NATS** | messaging system | [official blog](https://nats.io/blog/){.external-link target="_blank"} — guest post on **FastStream** |
| **dependency-injector** | dependency injection framework | [examples/miniapps/faststream](https://github.com/ets-labs/python-dependency-injector/tree/master/examples/miniapps/faststream){.external-link target="_blank"} — plus a [documentation page](https://python-dependency-injector.ets-labs.org/){.external-link target="_blank"} |
| **dishka** | dependency injection framework | [dishka](https://github.com/reagento/dishka){.external-link target="_blank"} — built-in **FastStream** integration |
| **AnyDI** | dependency injection framework | [anydi](https://github.com/antonrh/anydi){.external-link target="_blank"} — built-in **FastStream** integration |
| **that-depends** | dependency injection framework | [that-depends](https://github.com/modern-python/that-depends){.external-link target="_blank"} — built-in **FastStream** integration |

## Community Extensions

Packages built on top of **FastStream** by the community:

* [`stompman`](https://github.com/community-of-python/stompman){.external-link target="_blank"} - STOMP client with a pleasant API
* [`faststream-mq`](https://github.com/davzucky/faststream-mq){.external-link target="_blank"} - standalone IBM MQ adapter
* [`zMQTT`](https://github.com/faststream-community/zMQTT){.external-link target="_blank"} - pure asyncio MQTT 3.1.1 and 5.0 client library, no **paho** dependency
* [`faststream-schema-registry`](https://github.com/mlovretovich/faststream-schema-registry){.external-link target="_blank"} - connects **FastStream** to the **Confluent** Schema Registry
* [`opentelemetry-instrumentation-faststream`](https://github.com/ashambalev/opentelemetry-instrumentation-faststream){.external-link target="_blank"} - **FastStream** instrumentation for **OpenTelemetry**
* [`faststream-prometheus`](https://gitlab.com/rocshers/python/faststream-prometheus){.external-link target="_blank"} - metrics collection for **Prometheus**
* [`faststream-compressors`](https://github.com/ulbwa/faststream-compressors){.external-link target="_blank"} - message compression middleware
* [`faststream-deadline-propagation`](https://github.com/ulbwa/faststream-deadline-propagation){.external-link target="_blank"} - deadline propagation for RPC requests
* [`faststream-outbox`](https://github.com/modern-python/faststream-outbox){.external-link target="_blank"} - transactional outbox backed by a **Postgres** table
* [`faststream-redis-timers`](https://github.com/modern-python/faststream-redis-timers){.external-link target="_blank"} - **Redis**-backed distributed timer scheduling
* [`faststream-concurrent-aiokafka`](https://github.com/modern-python/faststream-concurrent-aiokafka){.external-link target="_blank"} - concurrent message processing for **aiokafka**
* [`modern-di-faststream`](https://github.com/modern-python/modern-di-faststream){.external-link target="_blank"} - **modern-di** integration
* [`litestar-faststream`](https://github.com/hasansezertasan/litestar-faststream){.external-link target="_blank"} - **Litestar** integration for **FastStream** message brokers
* [`fast-healthchecks`](https://github.com/ZYLVEXT/fast-healthchecks){.external-link target="_blank"} - framework-agnostic health checks
* [`awesome-faststream`](https://github.com/lesnik512/awesome-faststream){.external-link target="_blank"} - curated list of libraries, tools, templates and resources

## Add Your Project

Using **FastStream** in production? Open a pull request adding your entry to
[`docs/docs/en/who-uses.md`](https://github.com/ag2ai/faststream/blob/main/docs/docs/en/who-uses.md){.external-link target="_blank"},
or tell us in the [discussions](https://github.com/ag2ai/faststream/discussions){.external-link target="_blank"}.

If you are listed and would prefer not to be, open an issue — no explanation needed and no questions asked.
