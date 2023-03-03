# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/997_Test_Utils.ipynb.

# %% auto 0
__all__ = ['logger', 'kafka_server_url', 'kafka_server_port', 'aiokafka_config', 'nb_safe_seed', 'true_after',
           'create_testing_topic', 'create_and_fill_testing_topic', 'mock_AIOKafkaProducer_send', 'change_dir',
           'run_script_and_cancel', 'display_docs']

# %% ../../nbs/997_Test_Utils.ipynb 1
import asyncio
import contextlib
import functools
import glob
import hashlib
import multiprocessing
import os
import random
import shlex
import shutil
import signal
import socket
import subprocess  # nosec
import tarfile
import textwrap
import time
import unittest
import unittest.mock
from collections import namedtuple
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import *
from unittest.mock import AsyncMock, MagicMock
from IPython.display import IFrame

import asyncer
import nest_asyncio
import posix_ipc

# [B404:blacklist] Consider possible security implications associated with the subprocess module.
import requests
import typer
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic
from fastcore.foundation import patch
from fastcore.meta import delegates
from pydantic import BaseModel, Field
from tqdm import tqdm

from .._components._subprocess import terminate_asyncio_process

# from fastkafka.server import _import_from_string
from fastkafka._components.helpers import (
    _import_from_string,
    combine_params,
    filter_using_signature,
    use_parameters_of,
)
from .._components.logger import get_logger, supress_timestamps
from ..application import FastKafka
from fastkafka.helpers import (
    consumes_messages,
    create_admin_client,
    create_missing_topics,
    in_notebook,
    produce_messages,
    tqdm,
    trange,
)

# %% ../../nbs/997_Test_Utils.ipynb 2
if in_notebook():
    from tqdm.notebook import tqdm, trange
else:
    from tqdm import tqdm, trange

# %% ../../nbs/997_Test_Utils.ipynb 5
logger = get_logger(__name__)

# %% ../../nbs/997_Test_Utils.ipynb 7
kafka_server_url = (
    os.environ["KAFKA_HOSTNAME"] if "KAFKA_HOSTNAME" in os.environ else "localhost"
)
kafka_server_port = os.environ["KAFKA_PORT"] if "KAFKA_PORT" in os.environ else "9092"

aiokafka_config = {
    "bootstrap_servers": f"{kafka_server_url}:{kafka_server_port}",
}

# %% ../../nbs/997_Test_Utils.ipynb 8
def nb_safe_seed(s: str) -> Callable[[int], int]:
    """Gets a unique seed function for a notebook

    Params:
        s: name of the notebook used to initialize the seed function

    Returns:
        A unique seed function
    """
    init_seed = int(hashlib.sha256(s.encode("utf-8")).hexdigest(), 16) % (10**8)

    def _get_seed(x: int = 0, *, init_seed: int = init_seed) -> int:
        return init_seed + x

    return _get_seed

# %% ../../nbs/997_Test_Utils.ipynb 10
def true_after(seconds: float) -> Callable[[], bool]:
    """Function returning True after a given number of seconds"""
    t = datetime.now()

    def _true_after(seconds: float = seconds, t: datetime = t) -> bool:
        return (datetime.now() - t) > timedelta(seconds=seconds)

    return _true_after

# %% ../../nbs/997_Test_Utils.ipynb 12
@contextmanager
@delegates(create_missing_topics)  # type: ignore
def create_testing_topic(
    *,
    topic_prefix: str = "test_topic_",
    seed: Optional[int] = None,
    **kwargs: Dict[str, Any],
) -> Generator[str, None, None]:
    """Create testing topic

    Example:
        ```python
        from os import environ
        from fastkafka.testing import create_testing_topic
        from fastkafka.helpers import create_admin_client

        kafka_server_url = environ["KAFKA_HOSTNAME"]
        aiokafka_config = {"bootstrap_servers": f"{kafka_server_url}:9092"}

        with create_testing_topic(
            topic_prefix="my_topic_for_create_testing_topic_",
            seed=746855,
            num_partitions=1,
            **aiokafka_config
        ) as topic:
            # Check if topic is created and exists in topic list
            kafka_admin = create_admin_client(**aiokafka_config)
            existing_topics = kafka_admin.list_topics().topics.keys()
            assert topic in existing_topics

        # Check if topic is deleted after exiting context
        existing_topics = kafka_admin.list_topics().topics.keys()
        assert topic not in existing_topics
        ```

    Args:
        topic_prefix: topic name prefix which will be augumented with a randomly generated sufix
        seed: seed used to generate radnom sufix
        topic_names: a list of topic names
        num_partitions: Number of partitions to create
        replication_factor: Replication factor of partitions, or -1 if replica_assignment is used.
        replica_assignment: List of lists with the replication assignment for each new partition.
        new_topic_config: topic level config parameters as defined here: https://kafka.apache.org/documentation.html#topicconfigs
        bootstrap_servers (str, list(str)): a ``host[:port]`` string or list of
            ``host[:port]`` strings that the producer should contact to
            bootstrap initial cluster metadata. This does not have to be the
            full node list.  It just needs to have at least one broker that will
            respond to a Metadata API Request. Default port is 9092. If no
            servers are specified, will default to ``localhost:9092``.
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: ``PLAINTEXT``, ``SSL``. Default: ``PLAINTEXT``.
            Default: ``PLAINTEXT``.
        sasl_mechanism (str): Authentication mechanism when security_protocol
            is configured for ``SASL_PLAINTEXT`` or ``SASL_SSL``. Valid values
            are: ``PLAIN``, ``GSSAPI``, ``SCRAM-SHA-256``, ``SCRAM-SHA-512``,
            ``OAUTHBEARER``.
            Default: ``PLAIN``
        sasl_plain_username (str): username for SASL ``PLAIN`` authentication.
            Default: :data:`None`
        sasl_plain_password (str): password for SASL ``PLAIN`` authentication.
            Default: :data:`None`

    Returns:
        Generator returning the generated name of the created topic


    """
    # create random topic name
    random.seed(seed)
    # [B311:blacklist] Standard pseudo-random generators are not suitable for security/cryptographic purposes.
    suffix = str(random.randint(0, 10**10))  # nosec

    topic = topic_prefix + suffix.zfill(3)

    # delete topic if it already exists
    admin = create_admin_client(**kwargs)  # type: ignore
    existing_topics = admin.list_topics().topics.keys()
    if topic in existing_topics:
        logger.warning(f"topic {topic} exists, deleting it...")
        fs = admin.delete_topics(topics=[topic])
        results = {k: f.result() for k, f in fs.items()}
        while topic in admin.list_topics().topics.keys():
            time.sleep(1)
    try:
        # create topic if needed
        create_missing_topics([topic], **kwargs)
        while topic not in admin.list_topics().topics.keys():
            time.sleep(1)
        yield topic

    finally:
        pass
        # cleanup if needed again
        fs = admin.delete_topics(topics=[topic])
        while topic in admin.list_topics().topics.keys():
            time.sleep(1)

# %% ../../nbs/997_Test_Utils.ipynb 15
@asynccontextmanager
@delegates(produce_messages)  # type: ignore
@delegates(create_testing_topic, keep=True)  # type: ignore
async def create_and_fill_testing_topic(**kwargs: Dict[str, str]) -> AsyncIterator[str]:
    """Create testing topic with a random sufix in the same and fill it will messages

    Args:
        topic_names: a list of topic names
        num_partitions: Number of partitions to create
        replication_factor: Replication factor of partitions, or -1 if replica_assignment is used.
        replica_assignment: List of lists with the replication assignment for each new partition.
        new_topic_config: topic level config parameters as defined here: https://kafka.apache.org/documentation.html#topicconfigs
        bootstrap_servers (str, list(str)): a ``host[:port]`` string or list of
            ``host[:port]`` strings that the producer should contact to
            bootstrap initial cluster metadata. This does not have to be the
            full node list.  It just needs to have at least one broker that will
            respond to a Metadata API Request. Default port is 9092. If no
            servers are specified, will default to ``localhost:9092``.
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: ``PLAINTEXT``, ``SSL``. Default: ``PLAINTEXT``.
            Default: ``PLAINTEXT``.
        sasl_mechanism (str): Authentication mechanism when security_protocol
            is configured for ``SASL_PLAINTEXT`` or ``SASL_SSL``. Valid values
            are: ``PLAIN``, ``GSSAPI``, ``SCRAM-SHA-256``, ``SCRAM-SHA-512``,
            ``OAUTHBEARER``.
            Default: ``PLAIN``
        sasl_plain_username (str): username for SASL ``PLAIN`` authentication.
            Default: :data:`None`
        sasl_plain_password (str): password for SASL ``PLAIN`` authentication.
            Default: :data:`None`
        topic: Topic name
        msgs: a list of messages to produce
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client.
            Default: ``aiokafka-producer-#`` (appended with a unique number
            per instance)
        key_serializer (Callable): used to convert user-supplied keys to bytes
            If not :data:`None`, called as ``f(key),`` should return
            :class:`bytes`.
            Default: :data:`None`.
        value_serializer (Callable): used to convert user-supplied message
            values to :class:`bytes`. If not :data:`None`, called as
            ``f(value)``, should return :class:`bytes`.
            Default: :data:`None`.
        acks (Any): one of ``0``, ``1``, ``all``. The number of acknowledgments
            the producer requires the leader to have received before considering a
            request complete. This controls the durability of records that are
            sent. The following settings are common:

            * ``0``: Producer will not wait for any acknowledgment from the server
              at all. The message will immediately be added to the socket
              buffer and considered sent. No guarantee can be made that the
              server has received the record in this case, and the retries
              configuration will not take effect (as the client won't
              generally know of any failures). The offset given back for each
              record will always be set to -1.
            * ``1``: The broker leader will write the record to its local log but
              will respond without awaiting full acknowledgement from all
              followers. In this case should the leader fail immediately
              after acknowledging the record but before the followers have
              replicated it then the record will be lost.
            * ``all``: The broker leader will wait for the full set of in-sync
              replicas to acknowledge the record. This guarantees that the
              record will not be lost as long as at least one in-sync replica
              remains alive. This is the strongest available guarantee.

            If unset, defaults to ``acks=1``. If `enable_idempotence` is
            :data:`True` defaults to ``acks=all``
        compression_type (str): The compression type for all data generated by
            the producer. Valid values are ``gzip``, ``snappy``, ``lz4``, ``zstd``
            or :data:`None`.
            Compression is of full batches of data, so the efficacy of batching
            will also impact the compression ratio (more batching means better
            compression). Default: :data:`None`.
        max_batch_size (int): Maximum size of buffered data per partition.
            After this amount :meth:`send` coroutine will block until batch is
            drained.
            Default: 16384
        linger_ms (int): The producer groups together any records that arrive
            in between request transmissions into a single batched request.
            Normally this occurs only under load when records arrive faster
            than they can be sent out. However in some circumstances the client
            may want to reduce the number of requests even under moderate load.
            This setting accomplishes this by adding a small amount of
            artificial delay; that is, if first request is processed faster,
            than `linger_ms`, producer will wait ``linger_ms - process_time``.
            Default: 0 (i.e. no delay).
        partitioner (Callable): Callable used to determine which partition
            each message is assigned to. Called (after key serialization):
            ``partitioner(key_bytes, all_partitions, available_partitions)``.
            The default partitioner implementation hashes each non-None key
            using the same murmur2 algorithm as the Java client so that
            messages with the same key are assigned to the same partition.
            When a key is :data:`None`, the message is delivered to a random partition
            (filtered to partitions with available leaders only, if possible).
        max_request_size (int): The maximum size of a request. This is also
            effectively a cap on the maximum record size. Note that the server
            has its own cap on record size which may be different from this.
            This setting will limit the number of record batches the producer
            will send in a single request to avoid sending huge requests.
            Default: 1048576.
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        request_timeout_ms (int): Produce request timeout in milliseconds.
            As it's sent as part of
            :class:`~kafka.protocol.produce.ProduceRequest` (it's a blocking
            call), maximum waiting time can be up to ``2 *
            request_timeout_ms``.
            Default: 40000.
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        api_version (str): specify which kafka API version to use.
            If set to ``auto``, will attempt to infer the broker version by
            probing various APIs. Default: ``auto``
        ssl_context (ssl.SSLContext): pre-configured :class:`~ssl.SSLContext`
            for wrapping socket connections. Directly passed into asyncio's
            :meth:`~asyncio.loop.create_connection`. For more
            information see :ref:`ssl_auth`.
            Default: :data:`None`
        connections_max_idle_ms (int): Close idle connections after the number
            of milliseconds specified by this config. Specifying :data:`None` will
            disable idle checks. Default: 540000 (9 minutes).
        enable_idempotence (bool): When set to :data:`True`, the producer will
            ensure that exactly one copy of each message is written in the
            stream. If :data:`False`, producer retries due to broker failures,
            etc., may write duplicates of the retried message in the stream.
            Note that enabling idempotence acks to set to ``all``. If it is not
            explicitly set by the user it will be chosen. If incompatible
            values are set, a :exc:`ValueError` will be thrown.
            New in version 0.5.0.
        sasl_oauth_token_provider (: class:`~aiokafka.abc.AbstractTokenProvider`):
            OAuthBearer token provider instance. (See
            :mod:`kafka.oauth.abstract`).
            Default: :data:`None`
    """

    with create_testing_topic(
        **use_parameters_of(create_testing_topic, **kwargs)
    ) as topic:
        await produce_messages(
            topic=topic, **use_parameters_of(produce_messages, **kwargs)
        )

        yield topic

# %% ../../nbs/997_Test_Utils.ipynb 18
@contextmanager
def mock_AIOKafkaProducer_send() -> Generator[unittest.mock.Mock, None, None]:
    """Mocks **send** method of **AIOKafkaProducer**"""
    with unittest.mock.patch("__main__.AIOKafkaProducer.send") as mock:

        async def _f() -> None:
            pass

        mock.return_value = asyncio.create_task(_f())

        yield mock

# %% ../../nbs/997_Test_Utils.ipynb 19
@contextlib.contextmanager
def change_dir(d: str) -> Generator[None, None, None]:
    curdir = os.getcwd()
    os.chdir(d)
    try:
        yield
    finally:
        os.chdir(curdir)

# %% ../../nbs/997_Test_Utils.ipynb 21
async def run_script_and_cancel(
    script: str,
    *,
    script_file: Optional[str] = None,
    cmd: Optional[str] = None,
    cancel_after: int = 10,
    app_name: str = "app",
    kafka_app_name: str = "kafka_app",
    generate_docs: bool = False,
) -> Tuple[int, bytes]:
    """Run script and cancel after predefined time

    Args:
        script: a python source code to be executed in a separate subprocess
        script_file: name of the script where script source will be saved
        cmd: command to execute. If None, it will be set to 'python3 -m {Path(script_file).stem}'
        cancel_after: number of seconds before sending SIGTERM signal

    Returns:
        A tuple containing exit code and combined stdout and stderr as a binary string
    """
    if script_file is None:
        script_file = "script.py"

    if cmd is None:
        cmd = f"python3 -m {Path(script_file).stem}"

    with TemporaryDirectory() as d:
        consumer_script = Path(d) / script_file

        with open(consumer_script, "w") as file:
            file.write(script)

        if generate_docs:
            logger.info(
                f"Generating docs for: {Path(script_file).stem}:{kafka_app_name}"
            )
            try:
                kafka_app: FastKafka = _import_from_string(
                    f"{Path(script_file).stem}:{kafka_app_name}"
                )
                await asyncer.asyncify(kafka_app.create_docs)()
            except Exception as e:
                logger.warning(
                    f"Generating docs failed for: {Path(script_file).stem}:{kafka_app_name}, ignoring it for now."
                )

        proc = subprocess.Popen(  # nosec: [B603:subprocess_without_shell_equals_true] subprocess call - check for execution of untrusted input.
            shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=d
        )
        await asyncio.sleep(cancel_after)
        proc.terminate()
        output, _ = proc.communicate()

        return (proc.returncode, output)

# %% ../../nbs/997_Test_Utils.ipynb 26
async def display_docs(docs_path: str, port: int = 4000) -> None:
    with change_dir(docs_path):
        process = await asyncio.create_subprocess_exec(
            "python3",
            "-m",
            "http.server",
            f"{port}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            from google.colab.output import eval_js

            proxy = eval_js(f"google.colab.kernel.proxyPort({port})")
            logger.info("Google colab detected! Proxy adjusted.")
        except:
            proxy = f"http://localhost:{port}"
        finally:
            display(IFrame(f"{proxy}", 1000, 700))  # type: ignore
            await terminate_asyncio_process(process)
