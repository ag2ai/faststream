import logging
from typing import Annotated

from faststream._internal.context import ContextRepo as CR
from faststream.app import FastStream as FS
from faststream.params import Context

Logger = Annotated[logging.Logger, Context("logger")]
ContextRepo = Annotated[CR, Context("context")]
FastStream = Annotated[FS, Context("app")]
