# FastStream Adopters

This file records organizations using **FastStream** and public evidence of potential adoption.
The confirmed table is maintained **by the adopters themselves**. If your organization uses
FastStream, please add yourself — it takes a minute. Separate tables attribute repository evidence,
job postings and individual team members' public reports without treating them as organization confirmations.

The [Used By](https://faststream.ag2.ai/latest/who-uses/) page collects public examples and tools
with FastStream integrations. This file also lets adopters supply deployment details directly.

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

Four categories distinguish organization confirmations, repository evidence, job postings and
individual team members' public reports.

### Confirmed by the organization

Added by someone from the organization itself. This is the list that matters — if you belong here,
please move your row up from the table below, or add a new one.

<!-- Please keep the table sorted alphabetically by organization. -->

| Organization | Brokers | Use case | Environment | Scale | Contact |
| --- | --- | --- | --- | --- | --- |
| [AG2](https://github.com/ag2ai) | NATS, Kafka | Event transport for agent workloads; FastStream is maintained here | — | — | [@Lancetnik](https://github.com/Lancetnik) |
| [Puzzle Pools](https://puzzle-pools.com/) | Mosquitto (MQTT) | Automation and monitoring of swimming pool equipment | — | — | [@borisalekseev](https://github.com/borisalekseev) |
| [Raiffeisenbank](https://habr.com/ru/companies/raiffeisenbank/articles/885792/) | Kafka, RabbitMQ | Event-driven backend services; the team contributed the Prometheus middleware upstream | — | — | — |
| [Tochka Bank](https://github.com/tochka-public) | RabbitMQ | Event transport between services | — | — | — |
| [IdaProject](https://github.com/idaproject) | RabbitMQ, Kafka | Transport between microservices (EDA) | — | — | — |
| [DNS Technologies](https://dns-shop.ru) | Kafka | Federal Financial Service, exchange of internal documents and banking transactions between systems | — | — | — |
| [MTS Web Services](https://github.com/MTSWebServices) | Kafka | [Consumer](https://github.com/MTSWebServices/data-rentgen) for OpenLineage events | — | — | — |

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

These employer postings and their public mirrors name FastStream in the team's stack, development
responsibilities, requirements or desirable skills. The middle column distinguishes these signals:
a requirement or an alternative tool does not establish adoption. Postings may describe client
projects rather than the employer's own services; unidentified clients are not listed as adopters.

Every posting links to an archived snapshot with a date. Closed and historical postings document
what was stated at the time, not current hiring or confirmed production use. Locations describe
the vacancy's market or work location, not necessarily the organization's headquarters.

**If this is your organization:** same as above — move your row to the confirmed table and fill in
what you actually run, or ask us to remove it.

<!-- Please keep the table sorted alphabetically by organization. -->

| Organization | What the posting says | Posting |
| --- | --- | --- |
| AI Implementation Group — Tashkent, Uzbekistan | **Requirements:** Python backend role accepts knowledge of one or more frameworks, including Django/Flask, FastAPI or FastStream; it does not identify an established FastStream stack. | [LinkedIn](https://web.archive.org/web/20261006052446/https://uz.linkedin.com/jobs/view/python-backend-at-ai-implementation-group-4249783780), archived 2026-10-06 |
| Arnia Software — Romania | **Requirements:** Senior Python developer for energy trading and risk management; experience with Python, FastAPI and FastStream. The end client is not identified. | [LinkedIn](https://web.archive.org/web/20261006052216/https://ro.linkedin.com/jobs/view/senior-python-developer-at-arnia-software-4348649393), archived 2026-10-06 |
| Artificial Seed — Bilbao, Spain | Senior Python backend engineer: "nice to have: experience with FastStream" | [hh.ru](https://web.archive.org/web/20260918174306/https://hh.ru/vacancy/137175705), open as of 2026-09-18 |
| Bell Integrator | Python developer: "Frameworks: FastAPI, LangChain / LangGraph, FastStream" in the stack | [hh.ru](https://web.archive.org/web/20260918194157/https://hh.ru/vacancy/137010660?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| Bureau 1440 (Бюро 1440) — satellite internet | Senior data services developer (Python): "Kafka (FastStream)" in the stack | [Habr Career](https://web.archive.org/web/20260917210933/https://career.habr.com/vacancies/1000167287), open as of 2026-09-17 |
| Code Metal — United States, remote | **Nice to have:** Senior backend engineer; event-driven architecture experience with tools such as Celery, FastStream or Kafka. FastStream is an example, not a declared dependency. | [Built In Boston](https://web.archive.org/web/20261006052101/https://www.builtinboston.com/job/senior-backend-engineer/8269424), archived 2026-10-06 |
| Data World | QA fullstack (Python): "Stack: Python, FastAPI, FastStream, NATS, Kafka, PostgreSQL, OpenSearch" | [hh.ru](https://web.archive.org/web/20260918194423/https://hh.ru/vacancy/137201126?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| Etalon (ООО Эталон) | Python backend developer: "Tech stack: Python, FastAPI, FastStream, PySide/PyQt, SQLAlchemy, PostgreSQL, TimescaleDB" | [hh.ru](https://web.archive.org/web/20250216175133/https://hh.ru/vacancy/116760887), closed 2025-03-06 |
| Global Changer — Berlin, Germany | **Team stack:** Senior Ruby backend role lists Python, FastAPI and FastStream under AI & Services, with NATS for messaging. | [Built In](https://web.archive.org/web/20261006054136/https://builtin.com/job/senior-backend-engineer-ruby-rails-m-f-d/9413156), archived 2026-10-06 |
| HUDstats — Bulgaria, remote | **Development responsibilities:** Backend engineer building Python services with FastAPI/FastStream for real-time esports computer vision and data processing. | [LinkedIn](https://web.archive.org/web/20261006053751/https://bg.linkedin.com/jobs/view/backend-engineer-at-hudstats-4438640236), archived 2026-10-06 |
| Kuper (Купер) — grocery delivery | Python team lead, anti-fraud: FastStream among the technologies, next to FastAPI, Faust, ARQ and Kafka | [getmatch](https://web.archive.org/web/20260917212720/https://getmatch.ru/vacancies/17393-python-team-lead-antifrod), closed |
| mylantech GmbH — Bulgaria, remote | **Development responsibilities:** Python application engineer building event-driven microservices with FastAPI and FastStream, alongside Kafka and Azure/Kubernetes. The end client is not identified. | [LinkedIn](https://web.archive.org/web/20261006053906/https://bg.linkedin.com/jobs/view/python-application-engineer-fastapi-streamlit-azure-%E2%80%93-bulgaria-remote-first-at-mylantech-gmbh-4406063518), archived 2026-10-06 |
| Next Kraftwerke — Cologne, Germany | **Team technologies:** Python developer for balancing energy and redispatch; Django, FastAPI and FastStream, with RabbitMQ and Azure. | [Employer career site](https://web.archive.org/web/20261006051827/https://www.next-kraftwerke.de/job/python-software-developer-balancing-energy-market-de), archived 2026-10-06 |
| Octopus Energy Trading — London, UK | **Team stack:** Software engineer; FastStream for streaming microservices supporting energy trading, alongside Redis, FastAPI and Airflow. | [Work in Green](https://web.archive.org/web/20261006053547/https://workingreen.jobs/offers/software-engineer-at-octopus-energy-london-gb), archived 2026-10-06 |
| Partoo — Paris, France | **Team stack:** Senior backend developer; FastStream alongside Python, FastAPI, Celery and SQLAlchemy. | [HubMub](https://web.archive.org/web/20261006054311/https://www.hubmub.com/jobs/1045650/senior-back-end-developer-cdi-paris-hfx), archived 2026-10-06 |
| RBC (РБК) — media | Python developer: "work with FastAPI, FastStream and other frameworks" | [hh.ru](https://web.archive.org/web/20260917212800/https://hh.ru/vacancy/125997975), closed 2025-10-31 |
| RocketData | Senior Python developer: "Our stack: Python 3.11+, Django + REST Framework, FastAPI, Celery, FastStream, Kubernetes; brokers: RabbitMQ, Kafka" | [hh.ru](https://web.archive.org/web/20260917212924/https://hh.ru/vacancy/119577133), closed 2025-05-16 |
| Rostelecom IT (Ростелеком ИТ) | Senior Python developer: "Stack: Python 3.12+, Docker, K8S, FastAPI, ..., Aiopika, FastStream (kafka), dishka, prefect, MLFlow" | [hh.ru](https://web.archive.org/web/20260918194052/https://hh.ru/vacancy/137356756?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| Runity (Рунити) — hosting and domains | PHP developer migrating to Python: "an advantage: knowledge of FastAPI, SQLAlchemy, FastStream" | [hh.ru](https://web.archive.org/web/20260918174305/https://hh.ru/vacancy/137089267), open as of 2026-09-18 |
| Sber IT (Сбер. IT) | Middle Python ML engineer, AI agents: "nice to have: experience with distributed task and message queues, stream processing — celery, taskiq, rabbitmq, Kafka, faststream" | [hh.ru](https://web.archive.org/web/20260918195015/https://hh.ru/vacancy/137494675?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| Solution (ООО Солюшен) | Python fullstack developer: "asynchronous data exchange between services via Kafka and RabbitMQ (using FastStream)" | [hh.ru](https://web.archive.org/web/20260918194730/https://hh.ru/vacancy/136878677?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| SPOTParking (ООО Спутник) | Python developer (middle): "FastAPI, PyMongo[beanie], AioPika[FastStream]" | [hh.ru](https://web.archive.org/web/20260917212842/https://hh.ru/vacancy/116147115), closed 2025-05-30 |
| Third Opinion (Платформа Третье Мнение) — medical AI | Python developer: "nice to have: experience with Apache Kafka in production, experience with FastStream" | [hh.ru](https://web.archive.org/web/20260918195849/https://hh.ru/vacancy/136862604?hhtmFrom=vacancy_search_list&try=2), open as of 2026-09-18 |
| TMGT (АО ТМГТ) — payments | Python tech lead / staff backend engineer: "a strong plus: Temporal, Debezium CDC, FastStream, AWS EKS, Helm or ArgoCD" | [hh.ru](https://web.archive.org/web/20260918195320/https://hh.ru/vacancy/136199617?hhtmFrom=vacancy_search_list), open as of 2026-09-18 |
| TripleTen / Nebius Academy — remote | **Team stack and responsibilities:** Full-stack developer in the upskilling team; FastStream in the Python backend and Kafka/FastStream data-flow orchestration. | [Neo Remote Jobs](https://web.archive.org/web/20261006054406/https://neoremotejobs.com/jobs/full-stack-developer-upskilling-team-at-tripleten-1928), archived 2026-10-06 |
| Unumbio — remote LATAM | **Desirable skills:** Python PDF, scraping and ETL role; FastStream is one of the task-orchestration tools listed alongside Celery and RQ. | [LinkedIn](https://web.archive.org/web/20261006052331/https://co.linkedin.com/jobs/view/desarrolladoress-python-pdfs-scraping-etl-%E2%80%93-remoto-latam-at-unumbio-4364527617), archived 2026-10-06 |
| VADAROD (ЗАО Водород) | Python developer (middle+): "message brokers (RabbitMQ, Kafka) and async processing (Celery, Faust, FastStream)" in the requirements | [hh.ru](https://web.archive.org/web/20260917212903/https://hh.ru/vacancy/117513184), closed 2025-04-19 |
| Yolk — Tel Aviv District, Israel | **Team stack:** Senior backend engineer for an AI sales-coaching platform; RabbitMQ and FastStream for messaging. | [LinkedIn](https://web.archive.org/web/20261006053707/https://il.linkedin.com/jobs/view/senior-back-end-developer-at-yolk-4370385045), archived 2026-10-06 |

### Usage reported by individual team members — please confirm

These accounts describe work on specific teams or products. They are attributed to their authors,
not presented as statements authorized by the organization. Employment dates and project scope
limit what we can infer; current organization-wide or production use has not been independently confirmed.

**If this is your organization:** confirm the deployment details in the first table, correct the
account or ask us to remove it.

| Organization | Reported use | Source |
| --- | --- | --- |
| Magalu Cloud — Brazil | Lucas França reports working on the first Load Balancing as a Service (LBaaS) product as a senior software developer, December 2023–June 2024. His project technologies include FastStream, RabbitMQ, FastAPI and OpenStack. This is a historical account of one team. | [Lucas França’s public CV, p. 2](https://web.archive.org/web/20261006053249/https://www.lucasfrancaid.com/CV_Lucas_Franca.pdf), archived 2026-10-06 |
| Tektome — Tokyo, Japan | Pumidol Leelerdsakulvong reports designing and implementing an ingestion, conversion and extraction queue for 3D models and large PDFs using RabbitMQ and FastStream. Tektome’s team page confirms his role as Technical Product Lead. | [Technical lead’s personal site](https://web.archive.org/web/20261006053419/https://pumidol.com/), archived 2026-10-06; [official team page](https://tektome.com/leadership-and-team/) |

More public examples and tools that ship a FastStream integration of their own are collected on the
[Used By](https://faststream.ag2.ai/latest/who-uses/) page.
