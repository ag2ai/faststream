---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# FastStream Redis Security

This chapter discusses the security options available in **FastStream** and how to use them.

## Security Objects

**FastStream** allows you to enhance the security of **applications** by using security objects when creating brokers. These security objects encapsulate security-related configurations and mechanisms. Security objects supported in **FastStream** for **Redis** are:

### 1. BaseSecurity Object

**Purpose:** The `BaseSecurity` object wraps `ssl.SSLContext` object and is used to enable SSL/TLS encryption for secure communication between FastStream services and external components such as message brokers.

**Usage:**

```python linenums="1" hl_lines="6-7 9"
{! docs_src/redis/security/basic.py !}
```

### 2. SASLPlaintext Object with SSL/TLS

**Purpose:** The `SASLPlaintext` object is used for authentication in SASL (Simple Authentication and Security Layer) plaintext mode. It allows you to provide a username and password for authentication.

**Usage:**

```python linenums="1" hl_lines="6-11 13"
{! docs_src/redis/security/plaintext.py [ln:1-10.25,11-] !}
```
