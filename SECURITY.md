# Security

## Scope

The defaults are Basic256Sha256 / SignAndEncrypt with an RSA client certificate/key, username/password authentication and Full server certificate validation. Configurable policies also allow unencrypted or signed channels and anonymous login. Selection must exactly match the configured policy/mode/authentication and never falls back to weaker security.

Full checks server certificate trust, validity, usage, revocation, Application URI and endpoint hostname/IP. Certificate skips only hostname/IP matching. These modes require server trust when the certificate protects the channel or an encrypted username token. None (insecure) bypasses server certificate validation and permits server impersonation even when the channel is encrypted. It leaves the configured client identity, user authentication and applicable protocol signature requirements unchanged.

A None/None channel needs no client identity. With anonymous login or an unencrypted username token, Full does not authenticate the server through a certificate. **Allow Unencrypted Password** defaults to **None channels only**: it permits plaintext passwords on None/None channels but rejects them on Sign channels. **Never** requires password encryption by the channel or the token. **Always (insecure)** permits plaintext passwords on Sign channels as well. A None-policy username token is allowed on SignAndEncrypt under every setting because the channel encrypts it.

The processor prefers supported encrypted username tokens under every setting, including Always. Full and Certificate validate the server certificate used to encrypt a token, even on a None channel. A validation failure ends the attempt without retrying a plaintext token. See the [configuration reference and migration checks](docs/configuration.md#security-and-authentication).

The legacy Milo 0.6 adapter does not compare the discovery endpoint list with the endpoints returned by CreateSession. The modern Milo 1 adapter enables that comparison. SignAndEncrypt does not add the missing legacy comparison. Certificate validation and applicable channel/session signature checks still apply, but do not replace this check.

Private keys and passwords are configuration secrets. Use NiFi sensitive parameters, restrictive filesystem permissions and trusted deployment administrators. Store and private-key passwords are independently optional. An unset password supplies an empty password, and a failed load is never retried with integrity verification disabled. JKS/PKCS12 server trust uses only trusted-certificate entries, with optional PKI issuer certificates and CRLs. The processor is a restricted component because it reads identity/trust files and maintains a configured PKI directory. Public demo credentials and generated demo identities are not production credentials.

## Reporting

Do not open a public issue containing an exploit, credentials, private keys, raw flow configuration or production data. On the GitHub repository, use **Security → Report a vulnerability** if private vulnerability reporting is enabled. If that option is unavailable, open a minimal issue asking the maintainer to enable a private reporting channel without disclosing the vulnerability.

Maintainers must enable private vulnerability reporting when publishing. No private email address or guaranteed response SLA is configured by this repository.

## Supported scope

No long-term security support promise is made for any release. Exact build targets and validation limits are in the README. Supporting a legacy JVM target does not imply that its runtime, operating system or dependencies are up to date or secure.

Dependency inventories identify what is bundled; they are not vulnerability scans. Before a release, review current advisories for the resolved versions and record the result. Updates that affect Milo/NiFi APIs must pass both build targets and lifecycle tests.

## Publication hygiene

Run the publication checks before sharing source. They detect selected credential formats, private-key blocks and forbidden file paths, but do not prove the absence of all secrets. Review the actual export. Public snapshots must not contain private source history or customer documents.
