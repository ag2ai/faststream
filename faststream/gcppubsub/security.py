"""GCP Pub/Sub security configuration."""

import os
from typing import Any, Dict, Optional

from faststream.security import BaseSecurity


class GCPPubSubSecurity(BaseSecurity):
    """GCP Pub/Sub security configuration."""
    
    def __init__(
        self,
        service_file: Optional[str] = None,
        use_application_default: bool = False,
    ) -> None:
        """Initialize security configuration.
        
        Args:
            service_file: Path to service account JSON file
            use_application_default: Use application default credentials
        """
        self.service_file = service_file
        self.use_application_default = use_application_default
    
    def get_config(self) -> Dict[str, Any]:
        """Get security configuration.
        
        Returns:
            Security configuration dictionary
        """
        config = {}
        
        if self.service_file:
            config["service_file"] = self.service_file
        elif self.use_application_default:
            # Application default credentials will be used automatically
            pass
        
        return config


def parse_security(security: Optional[BaseSecurity]) -> Dict[str, Any]:
    """Parse security configuration.
    
    Args:
        security: Security configuration object
    
    Returns:
        Security configuration dictionary
    """
    if security is None:
        # Check for emulator
        if "PUBSUB_EMULATOR_HOST" in os.environ:
            return {"emulator_host": os.environ["PUBSUB_EMULATOR_HOST"]}
        return {}
    
    if isinstance(security, GCPPubSubSecurity):
        return security.get_config()
    
    return {}