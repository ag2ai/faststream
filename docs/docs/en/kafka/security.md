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

```python linenums="1" hl_lines="4 7 9"
{! docs_src/kafka/security/basic.py !}
```

### 2. SASLPlaintext Object with SSL/TLS

**Purpose:** The `SASLPlaintext` object is used for authentication in SASL (Simple Authentication and Security Layer) plaintext mode. It allows you to provide a username and password for authentication.

**Usage:**

```python linenums="1"
{! docs_src/kafka/security/plaintext.py [ln:1-10.25,11-] !}
```

### 3. SASLScram256/512 Object with SSL/TLS

**Purpose:** The `SASLScram256` and `SASLScram512` objects are used for authentication using the Salted Challenge Response Authentication Mechanism (SCRAM).

**Usage:**

=== "SCRAM256"
    ```python linenums="1"
    {!> docs_src/kafka/security/sasl_scram256.py [ln:1-10.25,11-] !}
    ```

=== "SCRAM512"
    ```python linenums="1"
    {!> docs_src/kafka/security/sasl_scram512.py [ln:1-10.25,11-] !}
    ```

### 4. SASLOAuthBearer Object with SSL/TLS

**Purpose:** The `SASLOAuthBearer` is used for authentication using the `OAUTHBEARER` sasl_mechanism. You'll likely need to provide your own `sasl_oauth_token_provider` to the KafkaBroker object in order to complete the authentication flow, such as AWS's [`aws-msk-iam-sasl-signer-python`](https://github.com/aws/aws-msk-iam-sasl-signer-python). For more information see AIOKafka's documentation on [AbstractTokenProvider](https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.abc.AbstractTokenProvider).

**Usage:**

=== "OauthBearer"
    ```python linenums="1"
    {!> docs_src/kafka/security/sasl_oauthbearer.py [ln:1-16] !}
    ```

### 5. SASLGSSAPI Object with SSL/TLS

**Purpose:** The `SASLGSSAPI` object is used for authentication using Kerberos.

**Usage:**

```python linenums="1"
{!> docs_src/kafka/security/sasl_gssapi.py [ln:1-10.25,11-] !}
```
