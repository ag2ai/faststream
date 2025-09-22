import sys
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock
import pytest
import typer

from faststream import FastStream
from faststream.asgi import AsgiFastStream
from faststream.exceptions import StartupValidationError, SetupError
from faststream._internal.cli.main import (
    version_callback,
    _run,
    _run_imported_app,
    publish_message,
    run,
    publish
)


class TestVersionCallback:
    """Test cases for version_callback function."""

    def test_version_callback_with_version_true(self):
        """Test version callback when version is True."""
        with patch("platform.python_implementation", return_value="CPython"):
            with patch("platform.python_version", return_value="3.11.0"):
                with patch("platform.system", return_value="Linux"):
                    with patch("typer.echo") as mock_echo:
                        with pytest.raises(typer.Exit):
                            version_callback(True)
                        mock_echo.assert_called_once()
                        call_args = mock_echo.call_args[0][0]
                        assert "FastStream" in call_args
                        assert "CPython" in call_args
                        assert "3.11.0" in call_args
                        assert "Linux" in call_args

    def test_version_callback_with_version_false(self):
        """Test version callback when version is False."""
        with patch("typer.echo") as mock_echo:
            version_callback(False)
            mock_echo.assert_not_called()


class TestRunFunction:
    """Test cases for _run function."""

    @patch("faststream._internal.cli.main.import_from_string")
    @patch("faststream._internal.cli.main._run_imported_app")
    def test_run_with_basic_parameters(self, mock_run_imported_app, mock_import_from_string):
        """Test _run function with basic parameters."""
        mock_app_obj = MagicMock()
        mock_import_from_string.return_value = (None, mock_app_obj)
        
        _run(
            app="test:app",
            extra_options={},
            is_factory=False,
            log_config=None,
            log_level=logging.INFO,
            app_level=logging.DEBUG
        )
        
        mock_import_from_string.assert_called_once_with("test:app", is_factory=False)
        mock_run_imported_app.assert_called_once_with(
            mock_app_obj,
            extra_options={},
            log_level=logging.INFO,
            app_level=logging.DEBUG,
            log_config=None
        )


class TestRunImportedApp:
    """Test cases for _run_imported_app function."""

    def test_run_imported_app_with_invalid_app_type(self):
        """Test _run_imported_app with invalid app type."""
        invalid_app = "not_an_application"
        
        with pytest.raises(typer.BadParameter, match='Imported object "not_an_application" must be "Application" type.'):
            _run_imported_app(
                app_obj=invalid_app,
                extra_options={},
                log_config=None,
                log_level=logging.INFO
            )

    @patch("faststream._internal.cli.main.set_log_level")
    @patch("faststream._internal.cli.main.set_log_config")
    @patch("faststream._internal.cli.main.anyio.run")
    def test_run_imported_app_with_log_config(self, mock_anyio_run, mock_set_log_config, mock_set_log_level):
        """Test _run_imported_app with log configuration."""
        from faststream._internal.application import Application
        mock_app = MagicMock(spec=Application)
        mock_app.run = AsyncMock()
        log_config = Path("/tmp/log.conf")
        
        _run_imported_app(
            app_obj=mock_app,
            extra_options={},
            log_config=log_config,
            log_level=logging.DEBUG
        )
        
        mock_set_log_level.assert_called_once_with(logging.DEBUG, mock_app)
        mock_set_log_config.assert_called_once_with(log_config)
        mock_anyio_run.assert_called_once()

    @patch("faststream._internal.cli.main.anyio.run")
    @patch("faststream._internal.cli.utils.errors.draw_startup_errors")
    def test_run_imported_app_with_startup_validation_error(self, mock_draw_errors, mock_anyio_run):
        """Test _run_imported_app with StartupValidationError."""
        from faststream._internal.application import Application
        mock_app = MagicMock(spec=Application)
        mock_app.run = AsyncMock()
        mock_app.logger = MagicMock()
        mock_app.brokers = []
        startup_error = StartupValidationError(invalid_fields=[], missed_fields=[])
        mock_anyio_run.side_effect = startup_error
        
        with patch("sys.exit") as mock_exit:
            _run_imported_app(
                app_obj=mock_app,
                extra_options={},
                log_config=None,
                log_level=logging.INFO
            )
            
            mock_draw_errors.assert_called_once_with(startup_error)
            mock_exit.assert_called_once_with(1)


class TestPublishMessage:
    """Test cases for publish_message function."""

    @pytest.mark.asyncio
    async def test_publish_message_with_rpc(self):
        """Test publish_message with RPC mode."""
        mock_broker = AsyncMock()
        mock_broker.request = AsyncMock(return_value="rpc_response")
        
        result = await publish_message(
            broker=mock_broker,
            rpc=True,
            message='{"test": "data"}',
            extra={"timeout": 30}
        )
        
        assert result == "rpc_response"
        mock_broker.request.assert_called_once_with({"test": "data"}, timeout=30)

    @pytest.mark.asyncio
    async def test_publish_message_without_rpc(self):
        """Test publish_message without RPC mode."""
        mock_broker = AsyncMock()
        mock_broker.publish = AsyncMock()
        
        await publish_message(
            broker=mock_broker,
            rpc=False,
            message='{"test": "data"}',
            extra={}
        )
        
        mock_broker.publish.assert_called_once_with({"test": "data"})

    @pytest.mark.asyncio
    async def test_publish_message_with_string_message(self):
        """Test publish_message with string message (not JSON)."""
        mock_broker = AsyncMock()
        mock_broker.publish = AsyncMock()
        
        await publish_message(
            broker=mock_broker,
            rpc=False,
            message="simple_string",
            extra={}
        )
        
        mock_broker.publish.assert_called_once_with("simple_string")

    @pytest.mark.asyncio
    async def test_publish_message_with_broker_error(self):
        """Test publish_message with broker error."""
        mock_broker = AsyncMock()
        mock_broker.publish.side_effect = Exception("Broker error")
        
        with patch("typer.echo") as mock_echo:
            with patch("sys.exit") as mock_exit:
                await publish_message(
                    broker=mock_broker,
                    rpc=False,
                    message="test",
                    extra={}
                )
                
                mock_echo.assert_called_once()
                mock_exit.assert_called_once_with(1)


class TestRunCommand:
    """Test cases for run command."""

    @patch("faststream._internal.cli.main.parse_cli_args")
    @patch("faststream._internal.cli.main.import_from_string")
    @patch("faststream._internal.cli.main._run_imported_app")
    def test_run_command_basic(self, mock_run_imported_app, mock_import_from_string, mock_parse_cli_args):
        """Test run command with basic parameters."""
        mock_ctx = MagicMock()
        mock_ctx.args = []
        mock_parse_cli_args.return_value = ("test:app", {})
        mock_app_obj = MagicMock()
        mock_import_from_string.return_value = (None, mock_app_obj)
        
        run(
            ctx=mock_ctx,
            app="test:app",
            workers=1,
            app_dir="",
            is_factory=False,
            reload=False,
            watch_extensions=[],
            log_level="info",
            log_config=None
        )
        
        mock_run_imported_app.assert_called_once()

    @patch("faststream._internal.cli.main.parse_cli_args")
    @patch("faststream._internal.cli.main.import_from_string")
    def test_run_command_with_reload_and_workers_error(self, mock_import_from_string, mock_parse_cli_args):
        """Test run command with reload and workers > 1 (should raise error)."""
        mock_ctx = MagicMock()
        mock_ctx.args = []
        mock_parse_cli_args.return_value = ("test:app", {})
        mock_import_from_string.return_value = (None, MagicMock())
        
        with pytest.raises(SetupError, match="You can't use reload option with multiprocessing"):
            run(
                ctx=mock_ctx,
                app="test:app",
                workers=2,
                app_dir="",
                is_factory=False,
                reload=True,
                watch_extensions=[],
                log_level="info",
                log_config=None
            )

    @patch("faststream._internal.cli.main.parse_cli_args")
    @patch("faststream._internal.cli.main.import_from_string")
    @patch("faststream._internal.cli.supervisors.multiprocess.Multiprocess")
    def test_run_command_with_multiprocess(self, mock_multiprocess, mock_import_from_string, mock_parse_cli_args):
        """Test run command with multiprocess (workers > 1)."""
        mock_ctx = MagicMock()
        mock_ctx.args = []
        mock_parse_cli_args.return_value = ("test:app", {})
        mock_app_obj = FastStream(MagicMock())
        mock_import_from_string.return_value = (None, mock_app_obj)
        mock_multiprocess_instance = MagicMock()
        mock_multiprocess.return_value = mock_multiprocess_instance
        
        run(
            ctx=mock_ctx,
            app="test:app",
            workers=2,
            app_dir="",
            is_factory=False,
            reload=False,
            watch_extensions=[],
            log_level="info",
            log_config=None
        )
        
        mock_multiprocess.assert_called_once()
        mock_multiprocess_instance.run.assert_called_once()

    @patch("faststream._internal.cli.main.parse_cli_args")
    @patch("faststream._internal.cli.main.import_from_string")
    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.ASGIMultiprocess")
    def test_run_command_with_asgi_multiprocess(self, mock_asgi_multiprocess, mock_import_from_string, mock_parse_cli_args):
        """Test run command with ASGI multiprocess."""
        mock_ctx = MagicMock()
        mock_ctx.args = []
        mock_parse_cli_args.return_value = ("test:app", {})
        mock_app_obj = AsgiFastStream(MagicMock())
        mock_import_from_string.return_value = (None, mock_app_obj)
        mock_asgi_multiprocess_instance = MagicMock()
        mock_asgi_multiprocess.return_value = mock_asgi_multiprocess_instance
        
        run(
            ctx=mock_ctx,
            app="test:app",
            workers=2,
            app_dir="",
            is_factory=False,
            reload=False,
            watch_extensions=[],
            log_level="info",
            log_config=None
        )
        
        mock_asgi_multiprocess.assert_called_once()
        mock_asgi_multiprocess_instance.run.assert_called_once()

    @patch("faststream._internal.cli.main.parse_cli_args")
    @patch("faststream._internal.cli.main.import_from_string")
    def test_run_command_with_invalid_app_type(self, mock_import_from_string, mock_parse_cli_args):
        """Test run command with invalid app type."""
        mock_ctx = MagicMock()
        mock_ctx.args = []
        mock_parse_cli_args.return_value = ("test:app", {})
        mock_import_from_string.return_value = (None, "invalid_app")
        
        with pytest.raises(typer.BadParameter, match="Unexpected app type"):
            run(
                ctx=mock_ctx,
                app="test:app",
                workers=2,
                app_dir="",
                is_factory=False,
                reload=False,
                watch_extensions=[],
                log_level="info",
                log_config=None
            )


class TestPublishCommand:
    """Test cases for publish command."""

    @patch("faststream._internal.cli.main.parse_cli_args")
    @patch("faststream._internal.cli.main.import_from_string")
    @patch("faststream._internal.cli.main.anyio.run")
    def test_publish_command_success(self, mock_anyio_run, mock_import_from_string, mock_parse_cli_args):
        """Test publish command with success."""
        from faststream._internal.application import Application
        mock_ctx = MagicMock()
        mock_ctx.args = []
        mock_parse_cli_args.return_value = ("test:app", {})
        mock_app_obj = MagicMock(spec=Application)
        mock_broker = MagicMock()
        mock_app_obj.broker = mock_broker
        mock_import_from_string.return_value = (None, mock_app_obj)
        mock_anyio_run.return_value = "publish_result"
        
        with patch("typer.echo") as mock_echo:
            publish(
                ctx=mock_ctx,
                app="test:app",
                app_dir="",
                message='{"test": "data"}',
                rpc=False,
                is_factory=False
            )
            
            mock_anyio_run.assert_called_once()
            mock_echo.assert_not_called()  # No RPC, so no output

    @patch("faststream._internal.cli.main.parse_cli_args")
    @patch("faststream._internal.cli.main.import_from_string")
    @patch("faststream._internal.cli.main.anyio.run")
    def test_publish_command_with_rpc(self, mock_anyio_run, mock_import_from_string, mock_parse_cli_args):
        """Test publish command with RPC mode."""
        from faststream._internal.application import Application
        mock_ctx = MagicMock()
        mock_ctx.args = []
        mock_parse_cli_args.return_value = ("test:app", {})
        mock_app_obj = MagicMock(spec=Application)
        mock_broker = MagicMock()
        mock_app_obj.broker = mock_broker
        mock_import_from_string.return_value = (None, mock_app_obj)
        mock_anyio_run.return_value = "rpc_response"
        
        with patch("typer.echo") as mock_echo:
            publish(
                ctx=mock_ctx,
                app="test:app",
                app_dir="",
                message='{"test": "data"}',
                rpc=True,
                is_factory=False
            )
            
            mock_echo.assert_called_once_with("rpc_response")

    @patch("faststream._internal.cli.main.parse_cli_args")
    @patch("faststream._internal.cli.main.import_from_string")
    def test_publish_command_without_broker(self, mock_import_from_string, mock_parse_cli_args):
        """Test publish command without broker."""
        from faststream._internal.application import Application
        mock_ctx = MagicMock()
        mock_ctx.args = []
        mock_parse_cli_args.return_value = ("test:app", {})
        mock_app_obj = MagicMock(spec=Application)
        mock_app_obj.broker = None
        mock_import_from_string.return_value = (None, mock_app_obj)
        
        with patch("typer.echo") as mock_echo:
            with patch("sys.exit") as mock_exit:
                publish(
                    ctx=mock_ctx,
                    app="test:app",
                    app_dir="",
                    message='{"test": "data"}',
                    rpc=False,
                    is_factory=False
                )
                
                mock_echo.assert_called_once()
                mock_exit.assert_called_once_with(1)

    @patch("faststream._internal.cli.main.parse_cli_args")
    @patch("faststream._internal.cli.main.import_from_string")
    def test_publish_command_with_exception(self, mock_import_from_string, mock_parse_cli_args):
        """Test publish command with exception."""
        mock_ctx = MagicMock()
        mock_ctx.args = []
        mock_parse_cli_args.return_value = ("test:app", {})
        mock_import_from_string.side_effect = Exception("Import error")
        
        with patch("typer.echo") as mock_echo:
            with patch("sys.exit") as mock_exit:
                publish(
                    ctx=mock_ctx,
                    app="test:app",
                    app_dir="",
                    message='{"test": "data"}',
                    rpc=False,
                    is_factory=False
                )
                
                mock_echo.assert_called_once()
                mock_exit.assert_called_once_with(1)
