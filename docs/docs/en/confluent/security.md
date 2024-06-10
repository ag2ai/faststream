---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# FastStream Kafka Security

This chapter discusses the security options available in **FastStream** and how to use them.

## Security Objects

**FastStream** allows you to enhance the security of applications by using security objects when creating brokers. These security objects encapsulate security-related configurations and mechanisms. Security objects supported in **FastStream** are (More are planned in the future such as SASL OAuth):

### 1. BaseSecurity Object

**Purpose:** The `BaseSecurity` object wraps `ssl.SSLContext` object and is used to enable SSL/TLS encryption for secure communication between **FastStream** services and external components such as message brokers.

**Usage:**

```python linenums="1" hl_lines="2 4 6"
{! docs_src/confluent/security/basic.py !}
```

### 2. SASLPlaintext Object with SSL/TLS

**Purpose:** The `SASLPlaintext` object is used for authentication in SASL (Simple Authentication and Security Layer) plaintext mode. It allows you to provide a username and password for authentication.

**Usage:**

```python linenums="1"
{! docs_src/confluent/security/plaintext.py !}
```

**Using any SASL authentication without SSL:**

The following example will log a **RuntimeWarning**:

```python linenums="1"
{! docs_src/confluent/security/ssl_warning.py [ln:8.16] !}
```

If the user does not want to use SSL encryption without the warning getting logged, they must explicitly set the `use_ssl` parameter to `False` when creating a SASL object.

```python linenums="1"
{! docs_src/confluent/security/ssl_warning.py [ln:12.5-12.72] !}
```

### 3. SASLScram256/512 Object with SSL/TLS

**Purpose:** The `SASLScram256` and `SASLScram512` objects are used for authentication using the Salted Challenge Response Authentication Mechanism (SCRAM).

**Usage:**

=== "SCRAM256"
    ```python linenums="1"
    {!> docs_src/confluent/security/sasl_scram256.py [ln:1-6.25,7-] !}
    ```

=== "SCRAM512"
    ```python linenums="1"
    {!> docs_src/confluent/security/sasl_scram512.py [ln:1-6.25,7-] !}
    ```

### 4. Other security related usecases

**Purpose**: If you want to pass additional values to `confluent-kafka-python`, you can pass a dictionary called `config` to `KafkaBroker`. For example, to pass your own certificate file:

**Usage:**

```python
from faststream.confluent import KafkaBroker
from faststream.security import SASLPlaintext

security = SASLPlaintext(
    username="admin",
    password="password", # pragma: allowlist secret
)

config = {"ssl.ca.location": "~/my_certs/CRT_cacerts.pem"}

broker = KafkaBroker("localhost:9092", security=security, config=config)
```
