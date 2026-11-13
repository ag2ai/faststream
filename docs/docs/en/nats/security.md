---
# 10 - Default
search:
  boost: 10
---

# FastStream NATS security

`NatsBroker` accepts one authentication strategy through the `security` argument.
The same object can carry the TLS settings used by the NATS connection.

NATS treats the following concerns separately:

- **TLS** encrypts the connection and authenticates the server. With mutual TLS, it can also authenticate the client.
- **Authentication** establishes the identity of the connecting client.
- **Authorization** controls which subjects that identity may publish or subscribe to. Authorization is configured on the NATS server or in account and user JWT claims; it is not configured by `NatsBroker`.

Read the [NATS authentication overview](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro){.external-link target="_blank"} for the corresponding server-side concepts and configuration.

!!! warning
    A NATS server without authentication accepts every client that can reach
    its port. Configure the server as well as the FastStream client.

## Choose an authentication strategy

FastStream models mutually exclusive NATS authentication mechanisms as separate objects:

| NATS deployment model            | Client credential                              | FastStream strategy |
|----------------------------------|------------------------------------------------|---------------------|
| Centralized server configuration | Username and password                          | `NatsUserPassword`  |
| Centralized server configuration | Server-wide token                              | `NatsToken`         |
| Centralized server configuration | User NKey seed                                 | `NatsNKey`          |
| Operator mode                    | Combined `.creds` file                         | `NatsCredentials`   |
| Operator mode                    | Separate user JWT and seed files               | `NatsCredentials`   |
| Operator mode                    | Application-provided JWT and signing callbacks | `NatsJWT`           |
| TLS without another credential   | `SSLContext`                                   | `NatsSecurity`      |

Use only one authentication strategy per broker. Credentials embedded in a server URL cannot be combined with an authentication object.
Keeping secrets in the `security` object also prevents them from appearing in connection URLs, logs, or generated AsyncAPI documents.

## Username and password

Use `NatsUserPassword` when the NATS server defines a user/password pair in its centralized configuration.

```python linenums="1" hl_lines="4-7"
{!> docs_src/nats/security/user_password.py !}
```

The password is sent by the client when each connection is authenticated.
On the server, prefer a bcrypt hash instead of storing the plaintext password, and use TLS because hashing the server configuration does not encrypt the password
in transit.

NATS does not negotiate SASL. FastStream still accepts the cross-broker `SASLPlaintext` object for backward compatibility and maps its fields to the
native NATS `user` and `password` connection options. Prefer `NatsUserPassword` in new NATS applications so the configured mechanism is explicit.

## Token

A NATS authentication token is one server-wide shared secret without a username. It is not a user JWT.

Pass a string to `NatsToken` when the token is fixed for the lifetime of the application.
Pass a zero-argument callback when the token can rotate:

```python linenums="1" hl_lines="6-7 10"
{!> docs_src/nats/security/token.py !}
```

The callback must return `str`. `nats-py` invokes it when composing a CONNECT message, so a reconnect can use the current token rather than the value from the first connection.

The callback is synchronous and runs in the connection path. It should return immediately and must not be declared with `async def`. If the secret provider
requires asynchronous or slow I/O, refresh the token separately and have the callback read the cached value.

!!! note
    Server-wide tokens are convenient for small internal deployments, but they
    do not identify individual users. Prefer user credentials or operator mode
    when identities need separate permissions or independent rotation.

## NKey authentication

NKey authentication uses an Ed25519 challenge-response exchange. The server stores the user's **public** NKey. The client keeps the corresponding private
seed and signs a fresh nonce from the server; the seed itself is not transmitted.

!!! note "Optional dependency"
    `NatsNKey` and `NatsCredentials` require the optional NKey support from
    `nats-py`. Install it with `pip install "nats-py[nkeys]"` before creating
    either security object.

Use an explicit factory so a file path is never confused with raw seed content:

```python linenums="1" hl_lines="4"
{!> docs_src/nats/security/nkey.py !}
```

- `NatsNKey.from_file(path)` lets `nats-py` read a seed file.
- `NatsNKey.from_seed(seed)` uses seed content already held in memory.

Prefer `from_file` with a mounted secret. Treat every NKey seed as a password: never commit it, log it, put it in an AsyncAPI description, or copy it into the NATS server configuration. The server needs only the public key.

## JWT credentials and operator mode

Operator mode uses a signed trust chain: an operator signs accounts, and an account signs users. A normal user authenticates by presenting its user JWT and signing the server nonce with its private NKey seed. The JWT identifies the account and carries claims; the seed proves that the client owns that identity.

See the official [decentralized authentication guide](https://docs.nats.io/learn/security/decentralized-auth){.external-link target="_blank"} for the trust model, permissions, expiry, and revocation lifecycle.

### Credentials file

A standard NATS `.creds` file contains two blocks: the user JWT and the user NKey seed. Pass its path with `NatsCredentials.from_file`:

```python linenums="1" hl_lines="4"
{!> docs_src/nats/security/credentials.py !}
```

Other supported sources are:

- `NatsCredentials.from_files(jwt=..., seed=...)` for separate files;
- `NatsCredentials.from_raw(credentials)` for combined credentials already in memory.

`from_raw` validates that both required credentials blocks are present. Prefer file-based credentials in deployed applications: they integrate naturally with mounted secrets and avoid keeping the seed in application configuration.

### JWT callbacks

Use `NatsJWT` when the application must obtain the user JWT or sign the nonce dynamically instead of giving `nats-py` a credentials file.

```python linenums="1" hl_lines="12-13 16-21 24"
{!> docs_src/nats/security/jwt.py !}
```

Both callbacks are synchronous:

- `jwt_cb()` returns the encoded user JWT as `bytes` or `bytearray`;
- `signature_cb(nonce)` signs the provided nonce and returns the base64-encoded signature as `bytes`.

The two callbacks form one authentication mechanism and must be provided together. Keep signing-key access short-lived where possible and clear key
material after signing when the key provider supports it.

!!! warning
    A user JWT is not the shared token accepted by `NatsToken`. Normal user JWT
    authentication also requires proof of the private seed. NATS bearer-user
    JWTs are a separate operator-mode feature with different security tradeoffs.

## TLS

Use a `tls://` server URL when the connection must use TLS. Pass an `ssl.SSLContext` through any NATS security object to configure trusted CAs,
certificate verification, or a client certificate. When TLS is the only security setting, use `NatsSecurity` directly.

```python linenums="1" hl_lines="5 9-13"
{!> docs_src/nats/security/tls.py !}
```

The NATS-specific TLS options are:

- `ssl_context` — Python TLS configuration, including trusted CAs and an
  optional client certificate loaded with `SSLContext.load_cert_chain(...)`;
- `tls_hostname` — hostname used for certificate verification when it must
  differ from the connection address;
- `tls_handshake_first` — start TLS before the server sends the normal NATS `INFO` protocol line.

By default, NATS sends `INFO` first and upgrades the connection to TLS before credentials are exchanged.
Set `tls_handshake_first=True` only when the server is configured with `handshake_first: true`.
Clients and servers must agree on the handshake order.
See the official [NATS encryption and TLS guide](https://docs.nats.io/learn/security/encryption){.external-link target="_blank"}.

For mutual TLS, load the client certificate and private key into `ssl_context`.
Whether that certificate merely permits the TLS connection or becomes the NATS user identity depends on server-side `verify` / `verify_and_map` configuration.

TLS transport alone does not create an AsyncAPI `X509` authentication scheme.
FastStream adds an authentication scheme only when the selected strategy models an identity mechanism.
Explicit mTLS identity modeling is outside the current NATS security interface.

## Secret handling and reconnects

Authentication runs for every new connection, including reconnects.
Ensure that credential files remain readable and that token/JWT callbacks remain available for the lifetime of the broker.

Recommended operational practices:

- mount credentials and seed files from a secret store instead of committing them to the application repository;
- restrict filesystem permissions on `.creds` and seed files;
- rotate shared tokens and passwords, and use JWT expiry and revocation in operator mode;
- never put credentials in a NATS URL when the URL may be logged;
- combine password or token authentication with TLS;
- configure authorization separately so an authenticated user receives only the publish and subscribe permissions it needs.
