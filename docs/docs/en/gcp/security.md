---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# GCP Pub/Sub Security

Secure your FastStream GCP Pub/Sub applications with proper authentication, authorization, and encryption. This guide covers various security configurations and best practices.

## Authentication Methods

### Default Credentials

The simplest authentication method uses Application Default Credentials (ADC):

```python
from faststream.gcp import GCPBroker

# Uses Application Default Credentials
# 1. GOOGLE_APPLICATION_CREDENTIALS environment variable
# 2. gcloud auth application-default login
# 3. GCE/GKE/Cloud Run metadata service
broker = GCPBroker(project_id="your-project-id")
```

### Service Account Key File

Use a service account JSON key file:

```python
from faststream.gcp import GCPBroker, GCPSecurity

# Using credentials file path
broker = GCPBroker(
    project_id="your-project-id",
    security=GCPSecurity(
        credentials_path="/path/to/service-account.json"
    )
)

# Or set environment variable
# export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
broker = GCPBroker(project_id="your-project-id")
```

### Service Account Credentials Object

Use credentials object directly:

```python
from google.oauth2 import service_account
from faststream.gcp import GCPBroker, GCPSecurity

# Load credentials
credentials = service_account.Credentials.from_service_account_file(
    "/path/to/service-account.json",
    scopes=["https://www.googleapis.com/auth/pubsub"]
)

broker = GCPBroker(
    project_id="your-project-id",
    security=GCPSecurity(
        credentials=credentials
    )
)
```

### Workload Identity (GKE)

For Google Kubernetes Engine with Workload Identity:

```python
from google.auth import compute_engine
from faststream.gcp import GCPBroker, GCPSecurity

# Automatic authentication in GKE with Workload Identity
credentials = compute_engine.Credentials()

broker = GCPBroker(
    project_id="your-project-id",
    security=GCPSecurity(
        credentials=credentials
    )
)
```

### Impersonation

Impersonate a service account:

```python
from google.auth import impersonated_credentials
from google.oauth2 import service_account

# Source credentials
source_credentials = service_account.Credentials.from_service_account_file(
    "/path/to/source-account.json"
)

# Impersonate target service account
target_credentials = impersonated_credentials.Credentials(
    source_credentials=source_credentials,
    target_principal="target-account@project.iam.gserviceaccount.com",
    target_scopes=["https://www.googleapis.com/auth/pubsub"],
    lifetime=3600  # Token lifetime in seconds
)

broker = GCPBroker(
    project_id="your-project-id",
    security=GCPSecurity(
        credentials=target_credentials
    )
)
```

## IAM Permissions

### Required Permissions

Minimum IAM permissions for FastStream operations:

```yaml
# Publisher permissions
- pubsub.topics.publish
- pubsub.topics.get

# Subscriber permissions
- pubsub.subscriptions.consume
- pubsub.subscriptions.get
- pubsub.topics.get

# Management permissions (optional)
- pubsub.topics.create
- pubsub.topics.delete
- pubsub.subscriptions.create
- pubsub.subscriptions.delete
- pubsub.subscriptions.update
```

### Example IAM Roles

Common predefined roles:

```python
# roles/pubsub.publisher - Can publish messages
# roles/pubsub.subscriber - Can consume messages
# roles/pubsub.viewer - Read-only access
# roles/pubsub.editor - Full control except IAM
# roles/pubsub.admin - Full control including IAM
```

### Custom IAM Role

Create a custom role with specific permissions:

```json
{
  "title": "FastStream Pub/Sub User",
  "description": "Custom role for FastStream applications",
  "stage": "GA",
  "includedPermissions": [
    "pubsub.topics.get",
    "pubsub.topics.publish",
    "pubsub.subscriptions.get",
    "pubsub.subscriptions.consume",
    "pubsub.subscriptions.create",
    "pubsub.subscriptions.update"
  ]
}
```

## Encryption

### Encryption at Rest

Pub/Sub automatically encrypts all data at rest. For additional control:

```python
# Use Customer-Managed Encryption Keys (CMEK)
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# Create topic with CMEK
topic = publisher.create_topic(
    request={
        "name": topic_path,
        "kms_key_name": "projects/PROJECT/locations/LOCATION/keyRings/RING/cryptoKeys/KEY"  # pragma: allowlist secret
    }
)
```

### Encryption in Transit

All Pub/Sub traffic is encrypted using TLS. For additional security:

```python
from faststream.gcp import GCPBroker, GCPSecurity

# Force TLS version (handled automatically by Google libraries)
broker = GCPBroker(
    project_id="your-project-id",
    security=GCPSecurity(
        credentials_path="/path/to/credentials.json",
        # Additional SSL/TLS configuration if needed
    )
)
```

## Message-Level Security

### Encrypting Message Content

Encrypt sensitive data before publishing:

```python
from cryptography.fernet import Fernet
import json
import base64

class EncryptedPublisher:
    def __init__(self, broker: GCPBroker, key: bytes):
        self.broker = broker
        self.cipher = Fernet(key)

    async def publish_encrypted(
        self,
        data: dict,
        topic: str,
        **kwargs
    ):
        # Serialize and encrypt
        json_data = json.dumps(data)
        encrypted = self.cipher.encrypt(json_data.encode())

        # Publish encrypted data
        await self.broker.publish(
            base64.b64encode(encrypted).decode(),
            topic=topic,
            attributes={"encrypted": "true", **kwargs.get("attributes", {})}
        )

# Generate encryption key
key = Fernet.generate_key()
publisher = EncryptedPublisher(broker, key)

# Publish encrypted message
await publisher.publish_encrypted(
    {"sensitive": "data"},
    topic="secure-topic"
)
```

### Decrypting Messages

Decrypt messages in subscribers:

```python
class EncryptedSubscriber:
    def __init__(self, key: bytes):
        self.cipher = Fernet(key)

    def decrypt(self, encrypted_data: str) -> dict:
        # Decode and decrypt
        encrypted = base64.b64decode(encrypted_data)
        decrypted = self.cipher.decrypt(encrypted)
        return json.loads(decrypted)

decryptor = EncryptedSubscriber(key)

@broker.subscriber("secure-sub", topic="secure-topic")
async def handle_encrypted(
    msg: str,
    attributes: MessageAttributes
):
    if attributes.get("encrypted") == "true":
        data = decryptor.decrypt(msg)
    else:
        data = json.loads(msg)

    # Process decrypted data
    await process_sensitive_data(data)
```

## Access Control

### VPC Service Controls

Restrict Pub/Sub access within VPC perimeters:

```python
# Configure VPC-SC compliant endpoint
from faststream.gcp import GCPBroker, GCPSecurity

broker = GCPBroker(
    project_id="your-project-id",
    security=GCPSecurity(
        credentials_path="/path/to/credentials.json",
        # VPC-SC uses private Google access
        api_endpoint="https://pubsub.googleapis.com"
    )
)
```

### Private Service Connect

Use Private Service Connect for private connectivity:

```python
# Use private endpoint
broker = GCPBroker(
    project_id="your-project-id",
    security=GCPSecurity(
        credentials_path="/path/to/credentials.json",
        api_endpoint="https://pubsub-psc.p.googleapis.com"
    )
)
```

## Audit Logging

### Enable Audit Logs

Configure audit logging for Pub/Sub:

```yaml
# In your project's audit log configuration
auditConfigs:
  - service: pubsub.googleapis.com
    auditLogConfigs:
      - logType: ADMIN_READ
      - logType: DATA_READ
      - logType: DATA_WRITE
```

### Log Security Events

Log security-relevant events in your application:

```python
import logging
from datetime import datetime

security_logger = logging.getLogger("security")

@broker.subscriber("audit-sub", topic="sensitive-events")
async def handle_with_audit(
    msg: dict,
    attributes: MessageAttributes,
    message_id: MessageId
):
    # Log access
    security_logger.info(
        "Message accessed",
        extra={
            "message_id": message_id,
            "user_id": attributes.get("user_id"),
            "timestamp": datetime.utcnow().isoformat(),
            "topic": "sensitive-events",
            "action": "consume"
        }
    )

    try:
        result = await process_sensitive_message(msg)

        # Log successful processing
        security_logger.info(
            "Message processed successfully",
            extra={"message_id": message_id}
        )

        return result

    except Exception as e:
        # Log failures
        security_logger.error(
            "Message processing failed",
            extra={
                "message_id": message_id,
                "error": str(e)
            }
        )
        raise
```

## Secret Management

### Using Secret Manager

Integrate with Google Secret Manager:

```python
from google.cloud import secretmanager

def get_secret(project_id: str, secret_id: str, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"

    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Get credentials from Secret Manager
api_key = get_secret("your-project", "api-key")
encryption_key = get_secret("your-project", "encryption-key")

# Use secrets in broker configuration
broker = GCPBroker(
    project_id="your-project-id",
    security=GCPSecurity(
        credentials=json.loads(get_secret("your-project", "service-account"))
    )
)
```

### Environment Variable Security

Secure environment variables:

```python
import os
from pathlib import Path

def load_secure_env():
    """Load environment variables securely."""
    # Check file permissions
    env_file = Path(".env")
    if env_file.exists():
        # Ensure file is not world-readable
        if env_file.stat().st_mode & 0o077:
            raise PermissionError(".env file has insecure permissions")

    # Load environment variables
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path:
        cred_file = Path(credentials_path)
        if not cred_file.exists():
            raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

        # Check credentials file permissions
        if cred_file.stat().st_mode & 0o077:
            raise PermissionError("Credentials file has insecure permissions")

load_secure_env()
```

## Security Best Practices

### 1. Principle of Least Privilege

```python
# Create separate service accounts for different components
publisher_broker = GCPBroker(
    project_id="your-project",
    security=GCPSecurity(
        credentials_path="/path/to/publisher-account.json"
        # Only has pubsub.topics.publish permission
    )
)

subscriber_broker = GCPBroker(
    project_id="your-project",
    security=GCPSecurity(
        credentials_path="/path/to/subscriber-account.json"
        # Only has pubsub.subscriptions.consume permission
    )
)
```

### 2. Rotate Credentials Regularly

```python
from datetime import datetime, timedelta

class CredentialRotator:
    def __init__(self, rotation_days: int = 30):
        self.rotation_days = rotation_days
        self.last_rotation = datetime.now()

    def should_rotate(self) -> bool:
        return datetime.now() - self.last_rotation > timedelta(days=self.rotation_days)

    async def rotate_if_needed(self, broker: GCPBroker):
        if self.should_rotate():
            # Fetch new credentials
            new_credentials = await fetch_new_credentials()

            # Update broker
            broker.security = GCPSecurity(credentials=new_credentials)

            self.last_rotation = datetime.now()
            logger.info("Credentials rotated successfully")
```

### 3. Validate Input Data

```python
from pydantic import BaseModel, validator

class SecureMessage(BaseModel):
    user_id: str
    data: dict

    @validator("user_id")
    def validate_user_id(cls, v):
        # Prevent injection attacks
        if not v.replace("-", "").isalnum():
            raise ValueError("Invalid user_id format")
        return v

    @validator("data")
    def validate_data(cls, v):
        # Check for sensitive data leakage
        sensitive_keys = ["password", "ssn", "credit_card"]
        for key in sensitive_keys:
            if key in v:
                raise ValueError(f"Sensitive field {key} not allowed")
        return v

@broker.subscriber("secure-sub", topic="user-events")
async def handle_secure(msg: SecureMessage):
    # Message is validated before processing
    await process_validated_message(msg)
```

### 4. Monitor Security Events

```python
from collections import defaultdict
from datetime import datetime, timedelta

class SecurityMonitor:
    def __init__(self):
        self.failed_attempts = defaultdict(int)
        self.suspicious_patterns = []

    def check_suspicious_activity(
        self,
        user_id: str,
        action: str
    ) -> bool:
        # Check for brute force attempts
        if self.failed_attempts[user_id] > 5:
            self.suspicious_patterns.append({
                "user_id": user_id,
                "pattern": "brute_force",
                "timestamp": datetime.now()
            })
            return True

        return False

    def record_failure(self, user_id: str):
        self.failed_attempts[user_id] += 1

monitor = SecurityMonitor()
```

## Testing Security

```python
import pytest
from unittest.mock import patch

@pytest.mark.asyncio
async def test_authentication():
    """Test that broker requires valid authentication."""
    with pytest.raises(Exception):
        # Should fail without credentials
        broker = GCPBroker(project_id="test-project")
        await broker.start()

@pytest.mark.asyncio
async def test_encryption():
    """Test message encryption/decryption."""
    key = Fernet.generate_key()
    publisher = EncryptedPublisher(broker, key)
    subscriber = EncryptedSubscriber(key)

    # Test encryption
    original = {"test": "data"}
    encrypted = await publisher.encrypt(original)
    decrypted = subscriber.decrypt(encrypted)

    assert decrypted == original
    assert encrypted != original
```
