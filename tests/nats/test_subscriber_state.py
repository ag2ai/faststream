"""Tests for NATS subscriber state classes."""

import pytest
from unittest.mock import MagicMock

from faststream.exceptions import IncorrectState
from faststream.nats.subscriber.state import (
    EmptySubscriberState,
    ConnectedSubscriberState,
)


class TestEmptySubscriberState:
    """Test cases for EmptySubscriberState class."""

    def test_client_property_raises_incorrect_state(self):
        """Test that client property raises IncorrectState."""
        state = EmptySubscriberState()
        
        with pytest.raises(IncorrectState) as exc_info:
            _ = state.client
        
        assert "Connection is not available yet" in str(exc_info.value)
        assert "setup the subscriber first" in str(exc_info.value)

    def test_js_property_raises_incorrect_state(self):
        """Test that js property raises IncorrectState."""
        state = EmptySubscriberState()
        
        with pytest.raises(IncorrectState) as exc_info:
            _ = state.js
        
        assert "Stream is not available yet" in str(exc_info.value)
        assert "setup the subscriber first" in str(exc_info.value)

    def test_kv_declarer_property_raises_incorrect_state(self):
        """Test that kv_declarer property raises IncorrectState."""
        state = EmptySubscriberState()
        
        with pytest.raises(IncorrectState) as exc_info:
            _ = state.kv_declarer
        
        assert "KeyValue is not available yet" in str(exc_info.value)
        assert "setup the subscriber first" in str(exc_info.value)

    def test_kv_declarer_setter_raises_incorrect_state(self):
        """Test that kv_declarer setter raises IncorrectState."""
        state = EmptySubscriberState()
        mock_declarer = MagicMock()
        
        with pytest.raises(IncorrectState) as exc_info:
            state.kv_declarer = mock_declarer
        
        assert "Connection is not available yet" in str(exc_info.value)
        assert "setup the subscriber first" in str(exc_info.value)

    def test_os_declarer_property_raises_incorrect_state(self):
        """Test that os_declarer property raises IncorrectState."""
        state = EmptySubscriberState()
        
        with pytest.raises(IncorrectState) as exc_info:
            _ = state.os_declarer
        
        assert "ObjectStorage is not available yet" in str(exc_info.value)
        assert "setup the subscriber first" in str(exc_info.value)

    def test_os_declarer_setter_raises_incorrect_state(self):
        """Test that os_declarer setter raises IncorrectState."""
        state = EmptySubscriberState()
        mock_declarer = MagicMock()
        
        with pytest.raises(IncorrectState) as exc_info:
            state.os_declarer = mock_declarer
        
        assert "Connection is not available yet" in str(exc_info.value)
        assert "setup the subscriber first" in str(exc_info.value)


class TestConnectedSubscriberState:
    """Test cases for ConnectedSubscriberState class."""

    def test_initialization(self):
        """Test proper initialization of ConnectedSubscriberState."""
        mock_parent_state = MagicMock()
        mock_kv_declarer = MagicMock()
        mock_os_declarer = MagicMock()
        
        state = ConnectedSubscriberState(
            parent_state=mock_parent_state,
            kv_declarer=mock_kv_declarer,
            os_declarer=mock_os_declarer
        )
        
        assert state._parent_state == mock_parent_state
        assert state.kv_declarer == mock_kv_declarer
        assert state.os_declarer == mock_os_declarer

    def test_client_property_returns_parent_connection(self):
        """Test that client property returns parent state connection."""
        mock_parent_state = MagicMock()
        mock_connection = MagicMock()
        mock_parent_state.connection = mock_connection
        
        mock_kv_declarer = MagicMock()
        mock_os_declarer = MagicMock()
        
        state = ConnectedSubscriberState(
            parent_state=mock_parent_state,
            kv_declarer=mock_kv_declarer,
            os_declarer=mock_os_declarer
        )
        
        result = state.client
        assert result == mock_connection

    def test_js_property_returns_parent_stream(self):
        """Test that js property returns parent state stream."""
        mock_parent_state = MagicMock()
        mock_stream = MagicMock()
        mock_parent_state.stream = mock_stream
        
        mock_kv_declarer = MagicMock()
        mock_os_declarer = MagicMock()
        
        state = ConnectedSubscriberState(
            parent_state=mock_parent_state,
            kv_declarer=mock_kv_declarer,
            os_declarer=mock_os_declarer
        )
        
        result = state.js
        assert result == mock_stream

    def test_kv_declarer_property_access(self):
        """Test that kv_declarer property can be accessed."""
        mock_parent_state = MagicMock()
        mock_kv_declarer = MagicMock()
        mock_os_declarer = MagicMock()
        
        state = ConnectedSubscriberState(
            parent_state=mock_parent_state,
            kv_declarer=mock_kv_declarer,
            os_declarer=mock_os_declarer
        )
        
        result = state.kv_declarer
        assert result == mock_kv_declarer

    def test_os_declarer_property_access(self):
        """Test that os_declarer property can be accessed."""
        mock_parent_state = MagicMock()
        mock_kv_declarer = MagicMock()
        mock_os_declarer = MagicMock()
        
        state = ConnectedSubscriberState(
            parent_state=mock_parent_state,
            kv_declarer=mock_kv_declarer,
            os_declarer=mock_os_declarer
        )
        
        result = state.os_declarer
        assert result == mock_os_declarer

    def test_kv_declarer_property_setter(self):
        """Test that kv_declarer property can be set."""
        mock_parent_state = MagicMock()
        mock_kv_declarer = MagicMock()
        mock_os_declarer = MagicMock()
        new_kv_declarer = MagicMock()
        
        state = ConnectedSubscriberState(
            parent_state=mock_parent_state,
            kv_declarer=mock_kv_declarer,
            os_declarer=mock_os_declarer
        )
        
        state.kv_declarer = new_kv_declarer
        assert state.kv_declarer == new_kv_declarer

    def test_os_declarer_property_setter(self):
        """Test that os_declarer property can be set."""
        mock_parent_state = MagicMock()
        mock_kv_declarer = MagicMock()
        mock_os_declarer = MagicMock()
        new_os_declarer = MagicMock()
        
        state = ConnectedSubscriberState(
            parent_state=mock_parent_state,
            kv_declarer=mock_kv_declarer,
            os_declarer=mock_os_declarer
        )
        
        state.os_declarer = new_os_declarer
        assert state.os_declarer == new_os_declarer
