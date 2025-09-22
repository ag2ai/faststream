import os
import threading
import time
from multiprocessing.context import SpawnProcess
from unittest.mock import patch, MagicMock
import pytest

from faststream._internal.cli.supervisors.basereload import BaseReload


class TestBaseReload:
    """Test cases for BaseReload class."""

    def test_init(self):
        """Test BaseReload initialization."""
        mock_target = MagicMock()
        args = ("arg1", "arg2")
        reload_delay = 1.0
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit") as mock_set_exit:
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args, reload_delay)
                
                assert reloader._target == mock_target
                assert reloader._args == args
                assert reloader.reload_delay == reload_delay
                assert reloader.pid == 12345
                assert isinstance(reloader.should_exit, threading.Event)
                assert reloader.reloader_name == ""
                mock_set_exit.assert_called_once()

    def test_init_with_default_reload_delay(self):
        """Test BaseReload initialization with default reload delay."""
        mock_target = MagicMock()
        args = ("arg1", "arg2")
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args)
                
                assert reloader.reload_delay == 0.5

    def test_startup(self):
        """Test BaseReload startup method."""
        mock_target = MagicMock()
        args = ("arg1", "arg2")
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args)
                
                with patch.object(reloader, "start_process") as mock_start_process:
                    with patch("faststream._internal.cli.supervisors.basereload.logger") as mock_logger:
                        mock_process = MagicMock()
                        mock_start_process.return_value = mock_process
                        
                        reloader.startup()
                        
                        assert reloader._process == mock_process
                        mock_start_process.assert_called_once()
                        mock_logger.info.assert_called_once_with(
                            "Started reloader process [%s] using %s",
                            12345,
                            ""
                        )

    def test_restart(self):
        """Test BaseReload restart method."""
        mock_target = MagicMock()
        args = ("arg1", "arg2")
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args)
                
                with patch.object(reloader, "_stop_process") as mock_stop_process:
                    with patch.object(reloader, "start_process") as mock_start_process:
                        with patch("faststream._internal.cli.supervisors.basereload.logger") as mock_logger:
                            mock_process = MagicMock()
                            mock_start_process.return_value = mock_process
                            
                            reloader.restart()
                            
                            mock_stop_process.assert_called_once()
                            mock_start_process.assert_called_once()
                            mock_logger.info.assert_called_once_with("Process successfully reloaded")
                            assert reloader._process == mock_process

    def test_shutdown(self):
        """Test BaseReload shutdown method."""
        mock_target = MagicMock()
        args = ("arg1", "arg2")
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args)
                
                with patch.object(reloader, "_stop_process") as mock_stop_process:
                    with patch("faststream._internal.cli.supervisors.basereload.logger") as mock_logger:
                        reloader.shutdown()
                        
                        mock_stop_process.assert_called_once()
                        mock_logger.info.assert_called_once_with("Stopping reloader process [%s]", 12345)

    def test_stop_process(self):
        """Test BaseReload _stop_process method."""
        mock_target = MagicMock()
        args = ("arg1", "arg2")
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args)
                
                mock_process = MagicMock()
                reloader._process = mock_process
                
                reloader._stop_process()
                
                mock_process.terminate.assert_called_once()
                mock_process.join.assert_called_once()

    def test_start_process(self):
        """Test BaseReload start_process method."""
        mock_target = MagicMock()
        args = ("arg1", {"extra": "data"})
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args)
                
                with patch("faststream._internal.cli.supervisors.basereload.get_subprocess") as mock_get_subprocess:
                    mock_process = MagicMock()
                    mock_get_subprocess.return_value = mock_process
                    
                    result = reloader.start_process(worker_id=42)
                    
                    assert result == mock_process
                    mock_get_subprocess.assert_called_once_with(target=mock_target, args=args)
                    mock_process.start.assert_called_once()
                    
                    # Check that worker_id was added to args
                    assert args[1]["worker_id"] == 42

    def test_start_process_with_none_worker_id(self):
        """Test BaseReload start_process method with None worker_id."""
        mock_target = MagicMock()
        args = ("arg1", {"extra": "data"})
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args)
                
                with patch("faststream._internal.cli.supervisors.basereload.get_subprocess") as mock_get_subprocess:
                    mock_process = MagicMock()
                    mock_get_subprocess.return_value = mock_process
                    
                    result = reloader.start_process(worker_id=None)
                    
                    assert result == mock_process
                    assert args[1]["worker_id"] is None

    def test_should_restart_not_implemented(self):
        """Test BaseReload should_restart method raises NotImplementedError."""
        mock_target = MagicMock()
        args = ("arg1", "arg2")
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args)
                
                with pytest.raises(NotImplementedError, match="Reload strategies should override should_restart\\(\\)"):
                    reloader.should_restart()

    def test_run_with_immediate_exit(self):
        """Test BaseReload run method with immediate exit."""
        mock_target = MagicMock()
        args = ("arg1", "arg2")
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args)
                
                # Set should_exit immediately
                reloader.should_exit.set()
                
                with patch.object(reloader, "startup") as mock_startup:
                    with patch.object(reloader, "shutdown") as mock_shutdown:
                        reloader.run()
                        
                        mock_startup.assert_called_once()
                        mock_shutdown.assert_called_once()

    def test_run_with_restart_cycle(self):
        """Test BaseReload run method with restart cycle."""
        mock_target = MagicMock()
        args = ("arg1", "arg2")
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args, reload_delay=0.01)
                
                # Mock should_restart to return True once, then False
                restart_calls = [True, False]
                
                def mock_should_restart():
                    return restart_calls.pop(0) if restart_calls else False
                
                reloader.should_restart = mock_should_restart
                
                with patch.object(reloader, "startup") as mock_startup:
                    with patch.object(reloader, "restart") as mock_restart:
                        with patch.object(reloader, "shutdown") as mock_shutdown:
                            # Set should_exit after a short delay
                            def set_exit():
                                time.sleep(0.05)
                                reloader.should_exit.set()
                            
                            import threading
                            exit_thread = threading.Thread(target=set_exit)
                            exit_thread.start()
                            
                            reloader.run()
                            
                            mock_startup.assert_called_once()
                            mock_restart.assert_called_once()
                            mock_shutdown.assert_called_once()
                            
                            exit_thread.join()

    def test_run_with_no_restart(self):
        """Test BaseReload run method with no restart needed."""
        mock_target = MagicMock()
        args = ("arg1", "arg2")
        
        with patch("faststream._internal.cli.supervisors.basereload.set_exit"):
            with patch("os.getpid", return_value=12345):
                reloader = BaseReload(mock_target, args, reload_delay=0.01)
                
                # Mock should_restart to always return False
                reloader.should_restart = lambda: False
                
                with patch.object(reloader, "startup") as mock_startup:
                    with patch.object(reloader, "restart") as mock_restart:
                        with patch.object(reloader, "shutdown") as mock_shutdown:
                            # Set should_exit after a short delay
                            def set_exit():
                                time.sleep(0.05)
                                reloader.should_exit.set()
                            
                            import threading
                            exit_thread = threading.Thread(target=set_exit)
                            exit_thread.start()
                            
                            reloader.run()
                            
                            mock_startup.assert_called_once()
                            mock_restart.assert_not_called()
                            mock_shutdown.assert_called_once()
                            
                            exit_thread.join()
