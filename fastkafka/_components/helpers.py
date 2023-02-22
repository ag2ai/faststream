# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/010_Internal_Helpers.ipynb.

# %% auto 0
__all__ = ['logger', 'F', 'in_notebook', 'combine_params', 'delegates_using_docstring', 'use_parameters_of', 'generate_app_src',
           'ImportFromStringError']

# %% ../../nbs/010_Internal_Helpers.ipynb 2
def in_notebook() -> bool:
    """!!! note

    Failed to generate docs

    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    try:
        from IPython import get_ipython

        if "IPKernelApp" not in get_ipython().config:
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True

# %% ../../nbs/010_Internal_Helpers.ipynb 4
import textwrap
from functools import wraps
from inspect import signature
from typing import *
from pathlib import Path
from tempfile import TemporaryDirectory
import sys
import importlib

import docstring_parser
from fastcore.meta import delegates
import typer

if in_notebook():
    from tqdm.notebook import tqdm, trange
else:
    from tqdm import tqdm, trange

import nbformat
from nbconvert import PythonExporter

from .logger import get_logger, supress_timestamps

# %% ../../nbs/010_Internal_Helpers.ipynb 6
logger = get_logger(__name__)

# %% ../../nbs/010_Internal_Helpers.ipynb 8
F = TypeVar("F", bound=Callable[..., Any])


def _format_args(xs: List[docstring_parser.DocstringParam]) -> str:
    """Format a list of arguments for a docstring.

    Args:
        xs: A list of arguments

    Returns:
        A string containing the formatted arguments

    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    return "\nArgs:\n - " + "\n - ".join(
        [f"{x.arg_name} ({x.type_name}): {x.description}" for x in xs]
    )


def combine_params(f: F, o: Union[Type, Callable[..., Any]]) -> F:
    """Combines docstring arguments of a function and another object or function

    Args:
        f: destination functions where combined arguments will end up
        o: source function from which arguments are taken from

    Returns:
        Function f with augumented docstring including arguments from both functions/objects
    """
    src_params = docstring_parser.parse_from_object(o).params
    #     logger.info(f"combine_params(): source:{_format_args(src_params)}")
    docs = docstring_parser.parse_from_object(f)
    #     logger.info(f"combine_params(): destination:{_format_args(docs.params)}")
    dst_params_names = [p.arg_name for p in docs.params]

    combined_params = docs.params + [
        x for x in src_params if not x.arg_name in dst_params_names
    ]
    #     logger.info(f"combine_params(): combined:{_format_args(combined_params)}")

    docs.meta = [
        x for x in docs.meta if not isinstance(x, docstring_parser.DocstringParam)
    ] + combined_params  # type: ignore

    f.__doc__ = docstring_parser.compose(
        docs, style=docstring_parser.DocstringStyle.GOOGLE
    )
    return f

# %% ../../nbs/010_Internal_Helpers.ipynb 10
def delegates_using_docstring(o: Union[Type, Callable[..., Any]]) -> Callable[[F], F]:
    """
    Args:
        o: Union[Type, Callable[..., Any]]

    Returns:
        Callable[[F], F]

    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """

    def _delegates_using_docstring(f: F) -> F:
        """
        Args:
            f:

        Returns:
            F:

        Raises:


        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """

        def _combine_params(o: Union[Type, Callable[..., Any]]) -> Callable[[F], F]:
            """
            Args:
                o: Union[Type, Callable[..., Any]]

            Returns:
                Callable[[F], F]:

            !!! note

                The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
            """

            def __combine_params(f: F, o: Union[Type, Callable[..., Any]] = o) -> F:
                """
                Args:
                    f:
                    o:

                Returns:


                !!! note

                    The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
                """
                return combine_params(f=f, o=o)

            return __combine_params

        @_combine_params(o)
        @delegates(o)  # type: ignore
        @wraps(f)
        def _f(*args: Any, **kwargs: Any) -> Any:
            """!!! note

            Failed to generate docs

            !!! note

                The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
            """
            return f(*args, **kwargs)

        return _f

    return _delegates_using_docstring

# %% ../../nbs/010_Internal_Helpers.ipynb 17
def use_parameters_of(
    o: Union[Type, Callable[..., Any]], **kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    """Restrict parameters passwed as keyword arguments to parameters from the signature of ``o``

    Args:
        o: object or callable which signature is used for restricting keyword arguments
        kwargs: keyword arguments

    Returns:
        restricted keyword arguments

    """
    allowed_keys = set(signature(o).parameters.keys())
    return {k: v for k, v in kwargs.items() if k in allowed_keys}

# %% ../../nbs/010_Internal_Helpers.ipynb 19
def generate_app_src(out_path: Union[Path, str]) -> None:
    """Generate the source code of the app from the notebook.

    Args:
        out_path: The path where to save the source code.

    Raises:
        ValueError: If the path does not exists.

    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    path = Path("099_Test_Service.ipynb")
    if not path.exists():
        path = Path("..") / "099_Test_Service.ipynb"
    if not path.exists():
        raise ValueError(f"Path '{path.resolve()}' does not exists.")

    with open(path, "r") as f:
        notebook = nbformat.reads(f.read(), nbformat.NO_CONVERT)
        exporter = PythonExporter()
        source, _ = exporter.from_notebook_node(notebook)

    with open(out_path, "w") as f:
        f.write(source)

# %% ../../nbs/010_Internal_Helpers.ipynb 21
class ImportFromStringError(Exception):
    """Exception raised when an import from a string fails.

    Attributes:
        message -- explanation of the error

    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """

    pass


def _import_from_string(import_str: str) -> Any:
    """Imports library from string

    Note:
        copied from https://github.com/encode/uvicorn/blob/master/uvicorn/importer.py

    Args:
        import_str: input string in form 'main:app'

    """
    sys.path.append(".")

    if not isinstance(import_str, str):
        return import_str

    module_str, _, attrs_str = import_str.partition(":")
    if not module_str or not attrs_str:
        message = (
            'Import string "{import_str}" must be in format "<module>:<attribute>".'
        )
        typer.secho(f"{message}", err=True, fg=typer.colors.RED)
        raise ImportFromStringError(message.format(import_str=import_str))

    try:
        # nosemgrep: python.lang.security.audit.non-literal-import.non-literal-import
        module = importlib.import_module(module_str)
    except ImportError as exc:
        if exc.name != module_str:
            raise exc from None
        message = 'Could not import module "{module_str}".'
        raise ImportFromStringError(message.format(module_str=module_str))

    instance = module
    try:
        for attr_str in attrs_str.split("."):
            instance = getattr(instance, attr_str)
    except AttributeError:
        message = 'Attribute "{attrs_str}" not found in module "{module_str}".'
        raise ImportFromStringError(
            message.format(attrs_str=attrs_str, module_str=module_str)
        )

    return instance
