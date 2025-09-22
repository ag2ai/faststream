"""Tests for CLI error handling utilities."""

import pytest
from unittest.mock import patch, MagicMock

from faststream.exceptions import StartupValidationError
from faststream._internal.cli.utils.errors import draw_startup_errors


class TestDrawStartupErrors:
    """Test cases for draw_startup_errors function."""

    def test_draw_startup_errors_with_invalid_fields(self):
        """Test drawing errors for invalid fields."""
        # Create a mock startup exception with invalid fields
        startup_exc = StartupValidationError(
            invalid_fields=["invalid_field1", "invalid_field2"],
            missed_fields=[]
        )
        
        # Mock the imports and functions
        with patch("typer.rich_utils") as mock_rich_utils:
            with patch("click.exceptions.BadParameter") as mock_bad_param:
                with patch("typer.core.TyperOption") as mock_typer_option:
                    # Call the function
                    draw_startup_errors(startup_exc)
                    
                    # Verify that BadParameter was called for each invalid field
                    assert mock_bad_param.call_count == 2

    def test_draw_startup_errors_with_missed_fields(self):
        """Test drawing errors for missed fields."""
        # Create a mock startup exception with missed fields
        startup_exc = StartupValidationError(
            invalid_fields=[],
            missed_fields=["missed_field1", "missed_field2"]
        )
        
        # Mock the imports and functions
        with patch("typer.rich_utils") as mock_rich_utils:
            with patch("click.exceptions.MissingParameter") as mock_missing_param:
                with patch("typer.core.TyperOption") as mock_typer_option:
                    # Call the function
                    draw_startup_errors(startup_exc)
                    
                    # Verify that MissingParameter was called for missed fields
                    assert mock_missing_param.call_count == 1

    def test_draw_startup_errors_with_both_invalid_and_missed_fields(self):
        """Test drawing errors for both invalid and missed fields."""
        # Create a mock startup exception with both types of errors
        startup_exc = StartupValidationError(
            invalid_fields=["invalid_field"],
            missed_fields=["missed_field"]
        )
        
        # Mock the imports and functions
        with patch("typer.rich_utils") as mock_rich_utils:
            with patch("click.exceptions.BadParameter") as mock_bad_param:
                with patch("click.exceptions.MissingParameter") as mock_missing_param:
                    with patch("typer.core.TyperOption") as mock_typer_option:
                        # Call the function
                        draw_startup_errors(startup_exc)
                        
                        # Verify both types of errors were handled
                        assert mock_bad_param.call_count == 1
                        assert mock_missing_param.call_count == 1

    def test_draw_startup_errors_without_rich_utils(self):
        """Test drawing errors when rich_utils is not available."""
        # Create a mock startup exception
        startup_exc = StartupValidationError(
            invalid_fields=["test_field"],
            missed_fields=[]
        )
        
        # Mock rich_utils to raise ImportError
        with patch("typer.rich_utils", side_effect=ImportError):
            with patch("click.exceptions.BadParameter") as mock_bad_param:
                with patch("typer.core.TyperOption") as mock_typer_option:
                    # Mock the show method
                    mock_instance = MagicMock()
                    mock_bad_param.return_value = mock_instance
                    
                    # Call the function
                    draw_startup_errors(startup_exc)
                    
                    # Verify that BadParameter was called
                    assert mock_bad_param.call_count == 1

    def test_draw_startup_errors_empty_exception(self):
        """Test drawing errors with empty exception (no fields)."""
        # Create a mock startup exception with no errors
        startup_exc = StartupValidationError(
            invalid_fields=[],
            missed_fields=[]
        )
        
        # Mock the imports and functions
        with patch("typer.rich_utils") as mock_rich_utils:
            with patch("click.exceptions.BadParameter") as mock_bad_param:
                with patch("click.exceptions.MissingParameter") as mock_missing_param:
                    # Call the function
                    draw_startup_errors(startup_exc)
                    
                    # Verify no error drawing functions were called
                    assert mock_bad_param.call_count == 0
                    assert mock_missing_param.call_count == 0