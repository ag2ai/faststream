# AUTOGENERATED! DO NOT EDIT! File to edit: ../../../nbs/19_Json_Encode_Decoder.ipynb.

# %% auto 0
__all__ = ['logger', 'json_encoder', 'json_decoder']

# %% ../../../nbs/19_Json_Encode_Decoder.ipynb 1
import io
import json
from typing import *

from pydantic import BaseModel
from pydantic.main import ModelMetaclass

from ..logger import get_logger

# %% ../../../nbs/19_Json_Encode_Decoder.ipynb 4
logger = get_logger(__name__)

# %% ../../../nbs/19_Json_Encode_Decoder.ipynb 6
def _to_json_utf8(o: Any) -> bytes:
    """Converts to JSON and then encodes with UTF-8"""
    if hasattr(o, "json"):
        return o.json().encode("utf-8")  # type: ignore
    else:
        return json.dumps(o).encode("utf-8")

# %% ../../../nbs/19_Json_Encode_Decoder.ipynb 9
def json_encoder(msg: BaseModel) -> bytes:
    return _to_json_utf8(msg)

# %% ../../../nbs/19_Json_Encode_Decoder.ipynb 11
def json_decoder(raw_msg: bytes, cls: ModelMetaclass) -> Any:
    msg_dict = json.loads(raw_msg.decode("utf-8"))

    return cls(**msg_dict)
