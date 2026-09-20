# FastStream Adopters

This file is a directory of organizations running **FastStream**, maintained **by the adopters
themselves**. If your organization uses FastStream, please add yourself — it takes a minute.

It is different from the [Used By](https://faststream.ag2.ai/latest/who-uses/) page, which we
compile ourselves from public repositories and package metadata. That page records what we can
observe; this file records what you tell us.

## Why add your organization?

- **Help others evaluate FastStream.** "Which brokers does anyone actually run this against, and at
  what scale?" is the first question every team asks, and the honest answer lives here.
- **Make your use case visible to the maintainers.** We prioritize by what people actually run.
  A broker, a deployment shape or a scale that shows up in this list gets attention in the roadmap.
- **Find peers.** Teams solving the same problem can find each other here instead of asking in chat.
- **Support the project.** Demonstrated real-world adoption is what keeps an open-source project
  credible and alive.

## How to add your organization

Pick whichever is easiest:

1. **Edit this file in the browser** — [edit ADOPTERS.md](https://github.com/ag2ai/faststream/edit/main/ADOPTERS.md).
   GitHub creates the fork and the pull request for you; no clone needed.
2. **Open a pull request** the usual way.
3. **Comment on [issue #3143](https://github.com/ag2ai/faststream/issues/3143)** if you would rather
   not open a pull request, and we will add the entry for you.

Add a row to the table below. Everything except the name is optional — a name and a link are already
useful, and the rest helps other teams more than it helps us.

| Field | What to put |
| --- | --- |
| **Organization** | Your organization, with a link to your site or GitHub org |
| **Brokers** | Which ones you run: Kafka, RabbitMQ, NATS, Redis, MQTT |
| **Use case** | One line — what FastStream does for you |
| **Environment** | Optional: Kubernetes, bare metal, a cloud, on-prem |
| **Scale** | Optional: messages per day, number of services, cluster size |
| **Contact** | Optional: a GitHub handle or a public channel, if you are open to questions |

> **Please add your organization only if you are authorized to represent it.** A pull request here
> is a public statement about your employer, and we take it at face value.

## Removing an entry

If your organization is listed and you would prefer it were not, open an issue or a pull request
removing the row. No explanation needed, no questions asked, and we will not ask you to reconsider.

## Trademarks

Listing an organization here records a fact about usage. It does not imply endorsement, support,
sponsorship or any affiliation with the FastStream project, and it grants FastStream no rights to
any organization's name or logo beyond this list.

---

## Adopters

Three tables, because the three kinds of entry are not the same thing.

### Confirmed by the organization

Added by someone from the organization itself. This is the list that matters — if you belong here,
please move your row up from the table below, or add a new one.

<!-- Please keep the table sorted alphabetically by organization. -->

| Organization | Brokers | Use case | Environment | Scale | Contact |
| --- | --- | --- | --- | --- | --- |
| [AG2](https://github.com/ag2ai) | NATS, Kafka | Event transport for agent workloads; FastStream is maintained here | — | — | [@Lancetnik](https://github.com/Lancetnik) |
| [Raiffeisenbank](https://habr.com/ru/companies/raiffeisenbank/articles/885792/) | Kafka, RabbitMQ | Event-driven backend services; the team contributed the Prometheus middleware upstream | — | — | — |
| [Tochka Bank](https://github.com/tochka-public) | RabbitMQ | Event transport between services | — | — | — |
| [IdaProject](https://github.com/idaproject) | RabbitMQ, Kafka | Transport between microservices (EDA) | — | — | — |

### Observed from public sources — please confirm

We found these in public repositories: a manifest or source file in the organization's own
repository declares FastStream. **Nobody from the organization has confirmed the entry**, and the
details below are what we could infer, which is never the interesting part.

**If this is your organization:** move your row to the table above and fill in what you actually run
— brokers, environment, scale. That is a minute of work and it is the part other teams read. If you
would rather not be listed at all, open an issue or a pull request removing the row; no explanation
needed.

| Organization | Who they are | Evidence |
| --- | --- | --- |
| [Aeluin Technologies](https://github.com/Aeluin-Technologies) | data integration and analytics vendor | [Galadril](https://github.com/Aeluin-Technologies/Galadril) |
| [AgentArea](https://github.com/agentarea) | control layer for agent teams | [agentarea](https://github.com/agentarea/agentarea) |
| [Catasto Open](https://github.com/catasto-open) | Italian land registry | [catasto-cdc](https://github.com/catasto-open/catasto-cdc) |
| [CTIC](https://github.com/fundacionctic) | CTIC Technology Centre, Spain | [connector-building-blocks](https://github.com/fundacionctic/connector-building-blocks) |
| [Data Cellar](https://github.com/Data-Cellar) | EU federated energy dataspace | [participant-template](https://github.com/Data-Cellar/participant-template) |
| [ECMWF](https://github.com/ecmwf) | European Centre for Medium-Range Weather Forecasts | [IonBeam](https://github.com/ecmwf/IonBeam) |
| [EggAI](https://github.com/eggai-tech) | agentic workforce automation | [EggAI](https://github.com/eggai-tech/EggAI) |
| [Gravitate](https://pypi.org/project/bb-integrations-library/) | AI platform for the fuel supply chain | [bb-integrations-library](https://pypi.org/project/bb-integrations-library/) |
| [hao.vc](https://github.com/hao-vc) | builders of AI-autonomous orchestrators | [haolib](https://github.com/hao-vc/haolib) |
| [HBB (AI·SW Maestro 17th)](https://github.com/SW-Maestro-17th-HBB) | dev team in a Korean software talent programme | [Kkori-AI](https://github.com/SW-Maestro-17th-HBB/Kkori-AI) |
| [Hydro-Québec](https://github.com/hq-opensource) | Quebec's public electricity utility | [building-intelligence](https://github.com/hq-opensource/building-intelligence) |
| [it@M](https://github.com/it-at-m) | IT services provider of the City of Munich | [zammad-ai](https://github.com/it-at-m/zammad-ai), [riski](https://github.com/it-at-m/riski) |
| [IT'IS Foundation](https://github.com/ITISFoundation) | Foundation for Research on Information Technologies in Society | [osparc-simcore](https://github.com/ITISFoundation/osparc-simcore) |
| [KIWIQ](https://github.com/rcortx) | multi-agent AI vendor | [kiwiq](https://github.com/rcortx/kiwiq) |
| [Lemma](https://github.com/lemma-work) | runtime for agent-built software | [lemma-platform](https://github.com/lemma-work/lemma-platform) |
| [LMDDC](https://github.com/lmddc-lu) | Luxembourg Media & Digital Design Centre | [alice.skilltech.tools](https://github.com/lmddc-lu/alice.skilltech.tools) |
| [MWS](https://github.com/MTSWebServices) | MTS Web Services | [data-rentgen](https://github.com/MTSWebServices/data-rentgen) |
| [NCATS (NIH) / PolusAI](https://github.com/PolusAI) | National Center for Advancing Translational Sciences | [aithena](https://github.com/PolusAI/aithena) |
| [NERSC](https://github.com/NERSC) | National Energy Research Scientific Computing Center, US DOE | [interactEM](https://github.com/NERSC/interactEM) |
| [NHS Lancashire & South Cumbria SDE](https://github.com/lsc-sde) | NHS secure data environment | [neulander-core](https://github.com/lsc-sde/neulander-core) |
| [Numberly](https://github.com/numberly) | marketing technology company | [reviewate](https://github.com/numberly/reviewate) |
| [QCrBox](https://github.com/QCrBox) | Quantum Crystallography Toolbox | [QCrBox](https://github.com/QCrBox/QCrBox) |
| [Red Hat](https://github.com/app-sre) | enterprise open-source vendor | [qontract-reconcile](https://github.com/app-sre/qontract-reconcile) |
| [Rubin Observatory / LSST](https://github.com/lsst-sqre) | Science Quality and Reliability Engineering team | [Safir](https://github.com/lsst-sqre/safir) |
| [spoo.me](https://github.com/spoo-me) | link management service | [spoo](https://github.com/spoo-me/spoo) |
| [TL;DR.tv](https://pypi.org/project/tldr-common/) | TL;DR.tv platform team | [tldr-common](https://pypi.org/project/tldr-common/) |
| [TogetherCrew](https://github.com/TogetherCrew) | open-source community tooling | [hivemind-bot](https://github.com/TogetherCrew/hivemind-bot) |
| [traide AI](https://github.com/traide) | AI for customs processes | [traide-core-python](https://github.com/traide/traide-core-python) |
| [Waldiez](https://github.com/waldiez) | multi-agent AI orchestration platforms | [runner](https://github.com/waldiez/runner) |
| [xi.effect / Sovlium](https://github.com/xi-effect) | education platform | [xi.back-2](https://github.com/xi-effect/xi.back-2) |

### Named in the organization's own job postings — please confirm

A job posting is the organization speaking for itself: "this is our stack, come work with it". It is
weaker evidence than a manifest — it says what the team expects to work with, not what we can read in
code — and postings disappear within weeks, so every link below goes to an archived snapshot, with the
date we saw it. The middle column says where FastStream appears: in the team's stack, in the
requirements, or as a "nice to have" — the last one is the weakest signal, and it is marked as such.

**If this is your organization:** same as above — move your row to the confirmed table and fill in
what you actually run, or ask us to remove it.

<!-- Please keep the table sorted alphabetically by organization. -->

| Organization | What the posting says | Posting |
| --- | --- | --- |
| Artificial Seed — Bilbao, Spain | Senior Python backend engineer: "nice to have: experience with FastStream" | [hh.ru](https://web.archive.org/web/20260918174306/https://hh.ru/vacancy/137175705), open as of 2026-09-18 |
| Bell Integrator | Python developer: "Frameworks: FastAPI, LangChain / LangGraph, FastStream" in the stack | [hh.ru](https://web.archive.org/web/20260918194157/https://hh.ru/vacancy/137010660?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| Bureau 1440 (Бюро 1440) — satellite internet | Senior data services developer (Python): "Kafka (FastStream)" in the stack | [Habr Career](https://web.archive.org/web/20260917210933/https://career.habr.com/vacancies/1000167287), open as of 2026-09-17 |
| Data World | QA fullstack (Python): "Stack: Python, FastAPI, FastStream, NATS, Kafka, PostgreSQL, OpenSearch" | [hh.ru](https://web.archive.org/web/20260918194423/https://hh.ru/vacancy/137201126?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| Etalon (ООО Эталон) | Python backend developer: "Tech stack: Python, FastAPI, FastStream, PySide/PyQt, SQLAlchemy, PostgreSQL, TimescaleDB" | [hh.ru](https://web.archive.org/web/20250216175133/https://hh.ru/vacancy/116760887), closed 2025-03-06 |
| Kuper (Купер) — grocery delivery | Python team lead, anti-fraud: FastStream among the technologies, next to FastAPI, Faust, ARQ and Kafka | [getmatch](https://web.archive.org/web/20260917212720/https://getmatch.ru/vacancies/17393-python-team-lead-antifrod), closed |
| Third Opinion (Платформа Третье Мнение) — medical AI | Python developer: "nice to have: experience with Apache Kafka in production, experience with FastStream" | [hh.ru](https://web.archive.org/web/20260918195849/https://hh.ru/vacancy/136862604?hhtmFrom=vacancy_search_list&try=2), open as of 2026-09-18 |
| RBC (РБК) — media | Python developer: "work with FastAPI, FastStream and other frameworks" | [hh.ru](https://web.archive.org/web/20260917212800/https://hh.ru/vacancy/125997975), closed 2025-10-31 |
| RocketData | Senior Python developer: "Our stack: Python 3.11+, Django + REST Framework, FastAPI, Celery, FastStream, Kubernetes; brokers: RabbitMQ, Kafka" | [hh.ru](https://web.archive.org/web/20260917212924/https://hh.ru/vacancy/119577133), closed 2025-05-16 |
| Rostelecom IT (Ростелеком ИТ) | Senior Python developer: "Stack: Python 3.12+, Docker, K8S, FastAPI, ..., Aiopika, FastStream (kafka), dishka, prefect, MLFlow" | [hh.ru](https://web.archive.org/web/20260918194052/https://hh.ru/vacancy/137356756?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| Runity (Рунити) — hosting and domains | PHP developer migrating to Python: "an advantage: knowledge of FastAPI, SQLAlchemy, FastStream" | [hh.ru](https://web.archive.org/web/20260918174305/https://hh.ru/vacancy/137089267), open as of 2026-09-18 |
| Sber IT (Сбер. IT) | Middle Python ML engineer, AI agents: "nice to have: experience with distributed task and message queues, stream processing — celery, taskiq, rabbitmq, Kafka, faststream" | [hh.ru](https://web.archive.org/web/20260918195015/https://hh.ru/vacancy/137494675?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| Solution (ООО Солюшен) | Python fullstack developer: "asynchronous data exchange between services via Kafka and RabbitMQ (using FastStream)" | [hh.ru](https://web.archive.org/web/20260918194730/https://hh.ru/vacancy/136878677?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| SPOTParking (ООО Спутник) | Python developer (middle): "FastAPI, PyMongo[beanie], AioPika[FastStream]" | [hh.ru](https://web.archive.org/web/20260917212842/https://hh.ru/vacancy/116147115), closed 2025-05-30 |
| TMGT (АО ТМГТ) — payments | Python tech lead / staff backend engineer: "a strong plus: Temporal, Debezium CDC, FastStream, AWS EKS, Helm or ArgoCD" | [hh.ru](https://web.archive.org/web/20260918195320/https://hh.ru/vacancy/136199617?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| VADAROD (ЗАО Водород) | Python developer (middle+): "message brokers (RabbitMQ, Kafka) and async processing (Celery, Faust, FastStream)" in the requirements | [hh.ru](https://web.archive.org/web/20260917212903/https://hh.ru/vacancy/117513184), closed 2025-04-19 |

The same list with a little more detail, plus tools that ship a FastStream integration of their own,
lives on the [Used By](https://faststream.ag2.ai/latest/who-uses/) page.
