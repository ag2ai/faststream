__version__ = "0.4.0dev"
# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/010_Application_export.ipynb.

# %% auto 0
__all__ = ['dummy']

# %% ../nbs/010_Application_export.ipynb 1
from ._application.app import FastKafka
from ._components.producer_decorator import KafkaEvent

__all__ = [
    "FastKafka",
    "KafkaEvent",
]

# %% ../nbs/010_Application_export.ipynb 2
def dummy() -> None:
    pass


dummy.__module__ = "_dummy"
