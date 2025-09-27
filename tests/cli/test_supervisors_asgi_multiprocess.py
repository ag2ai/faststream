"""Tests for ASGI multiprocess supervisor."""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from faststream.exceptions import INSTALL_UVICORN
from faststream._internal.cli.supervisors.asgi_multiprocess import ASGIMultiprocess


class TestASGIMultiprocess:
    """Test cases for ASGIMultiprocess class."""

    def test_initialization(self):
        """Test proper initialization of ASGIMultiprocess."""
        target = "test:app"
        args = (
            "test:app",
            {"host": "localhost", "port": 8000},
            False,
            None,
            20
        )
        workers = 4
        
        supervisor = ASGIMultiprocess(target, args, workers)
        
        assert supervisor._target == target
        assert supervisor._workers == workers
        assert supervisor._is_factory is False
        assert supervisor._log_level == 20
        assert supervisor._run_extra_options == {"host": "localhost", "port": 8000}

    def test_initialization_with_factory(self):
        """Test initialization with factory flag."""
        target = "test:app"
        args = (
            "test:app",
            {"host": "localhost", "port": 8000},
            True,
            None,
            20
        )
        workers = 2
        
        supervisor = ASGIMultiprocess(target, args, workers)
        
        assert supervisor._target == target
        assert supervisor._workers == workers
        assert supervisor._is_factory is True
        assert supervisor._log_level == 20

    def test_initialization_with_path(self):
        """Test initialization with Path object."""
        target = "test:app"
        app_dir = Path("/tmp/test")
        args = (
            "test:app",
            {"host": "localhost", "port": 8000},
            False,
            app_dir,
            20
        )
        workers = 1
        
        supervisor = ASGIMultiprocess(target, args, workers)
        
        assert supervisor._target == target
        assert supervisor._workers == workers
        assert supervisor._is_factory is False
        assert supervisor._log_level == 20

    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.HAS_UVICORN", False)
    def test_run_without_uvicorn_raises_import_error(self):
        """Test that run raises ImportError when uvicorn is not available."""
        target = "test:app"
        args = (
            "test:app",
            {"host": "localhost", "port": 8000},
            False,
            None,
            20
        )
        workers = 1
        
        supervisor = ASGIMultiprocess(target, args, workers)
        
        with pytest.raises(ImportError) as exc_info:
            supervisor.run()
        
        assert INSTALL_UVICORN in str(exc_info.value)

    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.HAS_UVICORN", True)
    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.uvicorn")
    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.UvicornExtraConfig")
    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.UvicornMultiprocess")
    def test_run_with_uvicorn_available(self, mock_multiprocess, mock_config, mock_uvicorn):
        """Test run method when uvicorn is available."""
        target = "test:app"
        args = (
            "test:app",
            {"host": "localhost", "port": 8000},
            False,
            None,
            20
        )
        workers = 2
        
        supervisor = ASGIMultiprocess(target, args, workers)
        
        # Mock the config and server
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance
        mock_config_instance.bind_socket.return_value = MagicMock()
        
        mock_server = MagicMock()
        mock_uvicorn.Server.return_value = mock_server
        
        mock_multiprocess_instance = MagicMock()
        mock_multiprocess.return_value = mock_multiprocess_instance
        
        # Call run
        supervisor.run()
        
        # Verify UvicornExtraConfig was created with correct parameters
        mock_config.assert_called_once()
        config_call_args = mock_config.call_args
        assert config_call_args[1]["app"] == target
        assert config_call_args[1]["factory"] is False
        assert config_call_args[1]["log_level"] == 20
        assert config_call_args[1]["workers"] == workers
        assert config_call_args[1]["run_extra_options"] == {"host": "localhost", "port": 8000}
        
        # Verify server was created and multiprocess was started
        mock_uvicorn.Server.assert_called_once_with(mock_config_instance)
        mock_config_instance.bind_socket.assert_called_once()
        mock_multiprocess.assert_called_once()
        mock_multiprocess_instance.run.assert_called_once()

    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.HAS_UVICORN", True)
    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.uvicorn")
    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.UvicornExtraConfig")
    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.UvicornMultiprocess")
    def test_run_with_factory_true(self, mock_multiprocess, mock_config, mock_uvicorn):
        """Test run method with factory=True."""
        target = "test:app"
        args = (
            "test:app",
            {"host": "localhost", "port": 8000},
            True,
            None,
            20
        )
        workers = 1
        
        supervisor = ASGIMultiprocess(target, args, workers)
        
        # Mock the config and server
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance
        mock_config_instance.bind_socket.return_value = MagicMock()
        
        mock_server = MagicMock()
        mock_uvicorn.Server.return_value = mock_server
        
        mock_multiprocess_instance = MagicMock()
        mock_multiprocess.return_value = mock_multiprocess_instance
        
        # Call run
        supervisor.run()
        
        # Verify factory parameter was set correctly
        config_call_args = mock_config.call_args
        assert config_call_args[1]["factory"] is True

    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.HAS_UVICORN", True)
    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.uvicorn")
    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.UvicornExtraConfig")
    @patch("faststream._internal.cli.supervisors.asgi_multiprocess.UvicornMultiprocess")
    def test_run_filters_uvicorn_config_parameters(self, mock_multiprocess, mock_config, mock_uvicorn):
        """Test that run filters parameters to only include valid uvicorn.Config parameters."""
        target = "test:app"
        args = (
            "test:app",
            {
                "host": "localhost",
                "port": 8000,
                "invalid_param": "should_be_filtered"
            },
            False,
            None,
            20
        )
        workers = 2
        
        supervisor = ASGIMultiprocess(target, args, workers)
        
        # Mock inspect.signature to return only valid parameters
        with patch("faststream._internal.cli.supervisors.asgi_multiprocess.inspect.signature") as mock_signature:
            mock_signature.return_value.parameters.keys.return_value = {
                "host", "port"
            }
            
            # Mock the config and server
            mock_config_instance = MagicMock()
            mock_config.return_value = mock_config_instance
            mock_config_instance.bind_socket.return_value = MagicMock()
            
            mock_server = MagicMock()
            mock_uvicorn.Server.return_value = mock_server
            
            mock_multiprocess_instance = MagicMock()
            mock_multiprocess.return_value = mock_multiprocess_instance
            
            # Call run
            supervisor.run()
            
            # Verify only valid parameters were passed to config
            config_call_args = mock_config.call_args
            config_kwargs = config_call_args[1]
            
            # Should include valid uvicorn parameters
            assert config_kwargs["host"] == "localhost"
            assert config_kwargs["port"] == 8000
            
            # Should not include invalid parameters
            assert "invalid_param" not in config_kwargs
            
            # Should include run_extra_options with all original parameters
            assert config_kwargs["run_extra_options"] == {
                "host": "localhost",
                "port": 8000,
                "invalid_param": "should_be_filtered"
            }
