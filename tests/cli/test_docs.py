"""Tests for CLI documentation generation."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import typer

from faststream._internal.cli.docs import gen


class TestDocsCommand:
    """Test cases for docs_command function."""

    def test_docs_command_with_yaml_output(self):
        """Test docs command with YAML output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir) / "test_schema.yaml"
            
            # Mock the app object and schema
            mock_app = MagicMock()
            mock_schema = MagicMock()
            mock_app.schema = mock_schema
            
            mock_spec = {"asyncapi": "2.6.0", "info": {"title": "Test API"}}
            mock_schema.to_specification.return_value.to_yaml.return_value = "asyncapi: 2.6.0\ninfo:\n  title: Test API"
            
            with patch("faststream._internal.cli.docs.import_from_string", return_value=(None, mock_app)):
                with patch("faststream._internal.cli.docs.typer.echo") as mock_echo:
                    # Call the function
                    gen(
                        app="test:app",
                        out=str(output_file),
                        yaml=True,
                        debug=False
                    )
                    
                    # Verify schema generation was called
                    mock_schema.to_specification.assert_called_once()
                    mock_schema.to_specification.return_value.to_yaml.assert_called_once()
                    
                    # Verify file was written
                    assert output_file.exists()
                    content = output_file.read_text()
                    assert "asyncapi: 2.6.0" in content
                    assert "title: Test API" in content

    def test_docs_command_with_json_output(self):
        """Test docs command with JSON output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir) / "test_schema.json"
            
            # Mock the app object and schema
            mock_app = MagicMock()
            mock_schema = MagicMock()
            mock_app.schema = mock_schema
            
            mock_spec = {"asyncapi": "2.6.0", "info": {"title": "Test API"}}
            mock_schema.to_specification.return_value.to_jsonable.return_value = mock_spec
            
            with patch("faststream._internal.cli.docs.import_from_string", return_value=(None, mock_app)):
                with patch("faststream._internal.cli.docs.typer.echo") as mock_echo:
                    # Call the function
                    gen(
                        app="test:app",
                        out=str(output_file),
                        yaml=False,
                        debug=False
                    )
                    
                    # Verify schema generation was called
                    mock_schema.to_specification.assert_called_once()
                    mock_schema.to_specification.return_value.to_jsonable.assert_called_once()
                    
                    # Verify file was written
                    assert output_file.exists()
                    content = json.loads(output_file.read_text())
                    assert content["asyncapi"] == "2.6.0"
                    assert content["info"]["title"] == "Test API"

    def test_docs_command_debug_mode_yaml(self):
        """Test docs command in debug mode with YAML."""
        # Mock the app object and schema
        mock_app = MagicMock()
        mock_schema = MagicMock()
        mock_app.schema = mock_schema
        
        mock_yaml_output = "asyncapi: 2.6.0\ninfo:\n  title: Test API"
        mock_schema.to_specification.return_value.to_yaml.return_value = mock_yaml_output
        
        with patch("faststream._internal.cli.docs.import_from_string", return_value=(None, mock_app)):
            with patch("faststream._internal.cli.docs.typer.echo") as mock_echo:
                # Call the function in debug mode
                gen(
                    app="test:app",
                    out=None,
                    yaml=True,
                    debug=True
                )
                
                # Verify schema generation was called
                mock_schema.to_specification.assert_called_once()
                mock_schema.to_specification.return_value.to_yaml.assert_called_once()
                
                # Verify debug output was echoed
                mock_echo.assert_called()
                # Check that the schema content was echoed
                echo_calls = [call[0][0] for call in mock_echo.call_args_list]
                assert any("Generated schema:" in call for call in echo_calls)
                assert any("asyncapi: 2.6.0" in call for call in echo_calls)

    def test_docs_command_debug_mode_json(self):
        """Test docs command in debug mode with JSON."""
        # Mock the app object and schema
        mock_app = MagicMock()
        mock_schema = MagicMock()
        mock_app.schema = mock_schema
        
        mock_spec = {"asyncapi": "2.6.0", "info": {"title": "Test API"}}
        mock_schema.to_specification.return_value.to_jsonable.return_value = mock_spec
        
        with patch("faststream._internal.cli.docs.import_from_string", return_value=(None, mock_app)):
            with patch("faststream._internal.cli.docs.typer.echo") as mock_echo:
                with patch("faststream._internal.cli.docs.pformat") as mock_pformat:
                    mock_pformat.return_value = "{\n  'asyncapi': '2.6.0',\n  'info': {'title': 'Test API'}\n}"
                    
                    # Call the function in debug mode
                    gen(
                        app="test:app",
                        out=None,
                        yaml=False,
                        debug=True
                    )
                    
                    # Verify schema generation was called
                    mock_schema.to_specification.assert_called_once()
                    mock_schema.to_specification.return_value.to_jsonable.assert_called_once()
                    mock_pformat.assert_called_once_with(mock_spec)
                    
                    # Verify debug output was echoed
                    mock_echo.assert_called()
                    # Check that the formatted schema was echoed
                    echo_calls = [call[0][0] for call in mock_echo.call_args_list]
                    assert any("Generated schema:" in call for call in echo_calls)

    def test_docs_command_yaml_import_error(self):
        """Test docs command when YAML import fails."""
        # Mock the app object and schema
        mock_app = MagicMock()
        mock_schema = MagicMock()
        mock_app.schema = mock_schema
        
        mock_schema.to_specification.return_value.to_yaml.side_effect = ImportError("No module named 'yaml'")
        
        with patch("faststream._internal.cli.docs.import_from_string", return_value=(None, mock_app)):
            with patch("faststream._internal.cli.docs.typer.echo") as mock_echo:
                with patch("faststream._internal.cli.docs.typer.Exit") as mock_exit:
                    # Mock typer.Exit to raise SystemExit instead of TypeError
                    mock_exit.side_effect = SystemExit(1)
                    
                    # Call the function and expect SystemExit
                    with pytest.raises(SystemExit):
                        gen(
                            app="test:app",
                            out="test.yaml",
                            yaml=True,
                            debug=False
                        )
                    
                    # Verify that Exit was called due to ImportError
                    mock_exit.assert_called_once_with(1)
                    
                    # Verify error message was echoed
                    mock_echo.assert_called()
                    echo_calls = [call[0][0] for call in mock_echo.call_args_list]
                    assert any("pip install PyYAML" in call for call in echo_calls)

    def test_docs_command_app_without_schema(self):
        """Test docs command when app doesn't have schema attribute."""
        # Mock the app object without schema
        mock_app = MagicMock()
        del mock_app.schema  # Remove schema attribute
        
        with patch("faststream._internal.cli.docs.import_from_string", return_value=(None, mock_app)):
            with pytest.raises(ValueError) as exc_info:
                gen(
                    app="test:app",
                    out="test.yaml",
                    yaml=True,
                    debug=False
                )
            
            assert "doesn't have `schema` attribute" in str(exc_info.value)

    def test_docs_command_with_app_dir(self):
        """Test docs command with app_dir parameter."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir) / "test_schema.json"
            
            # Mock the app object and schema
            mock_app = MagicMock()
            mock_schema = MagicMock()
            mock_app.schema = mock_schema
            
            mock_spec = {"asyncapi": "2.6.0", "info": {"title": "Test API"}}
            mock_schema.to_specification.return_value.to_jsonable.return_value = mock_spec
            
            with patch("faststream._internal.cli.docs.import_from_string", return_value=(None, mock_app)):
                with patch("faststream._internal.cli.docs.sys") as mock_sys:
                    with patch("faststream._internal.cli.docs.typer.echo") as mock_echo:
                        # Call the function with app_dir
                        gen(
                            app="test:app",
                            app_dir=temp_dir,
                            out=str(output_file),
                            yaml=False,
                            debug=False
                        )
                        
                        # Verify sys.path was modified
                        mock_sys.path.insert.assert_called_once_with(0, temp_dir)
                        
                        # Verify file was written
                        assert output_file.exists()

    def test_docs_command_default_output_filenames(self):
        """Test docs command with default output filenames."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock the app object and schema
            mock_app = MagicMock()
            mock_schema = MagicMock()
            mock_app.schema = mock_schema
            
            mock_spec = {"asyncapi": "2.6.0", "info": {"title": "Test API"}}
            mock_schema.to_specification.return_value.to_yaml.return_value = "asyncapi: 2.6.0"
            mock_schema.to_specification.return_value.to_jsonable.return_value = mock_spec
            
            with patch("faststream._internal.cli.docs.import_from_string", return_value=(None, mock_app)):
                with patch("faststream._internal.cli.docs.typer.echo") as mock_echo:
                    # Test YAML default filename
                    with patch("faststream._internal.cli.docs.Path") as mock_path:
                        mock_path_instance = MagicMock()
                        mock_path.return_value = mock_path_instance
                        
                        gen(
                            app="test:app",
                            out=None,
                            yaml=True,
                            debug=False
                        )
                        
                        # Verify default filename was used
                        mock_path.assert_called_with("asyncapi.yaml")
                        mock_path_instance.write_text.assert_called_once()
                    
                    # Test JSON default filename
                    with patch("faststream._internal.cli.docs.Path") as mock_path:
                        mock_path_instance = MagicMock()
                        mock_path.return_value = mock_path_instance
                        
                        gen(
                            app="test:app",
                            out=None,
                            yaml=False,
                            debug=False
                        )
                        
                        # Verify default filename was used
                        mock_path.assert_called_with("asyncapi.json")
                        mock_path_instance.open.assert_called_once()
