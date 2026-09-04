# Security Policy

## Supported Versions

Security fixes are applied to the current minor release line.

| Version | Supported |
| ------- | --------- |
| 0.4.x   | Yes       |
| <= 0.3  | No        |

Users on an unsupported release should upgrade before requesting a security
backport.

## Reporting a Vulnerability

Do **not** open a public issue for a suspected vulnerability.

Use one of these private channels:

1. [Open a private GitHub security advisory](https://github.com/streamlinelabs/streamline-go-sdk/security/advisories/new).
2. Email **security@streamlinelabs.dev** if private vulnerability reporting is
   unavailable.

Do not include production credentials, private keys, access tokens, or customer
data. Provide sanitized reproductions instead.

### What to Include

- Affected SDK and Go versions
- A description of the vulnerability and potential impact
- Minimal reproduction steps or a proof of concept
- Any relevant configuration, with secrets removed
- Suggested remediation, if known

The maintainers aim to acknowledge reports within 48 hours and provide an
initial assessment within 7 days. Remediation and disclosure timing depend on
severity, affected upstream components, and release coordination. These targets
are not a contractual service-level agreement.

Reporters may be credited in an advisory or release notes with their permission.

## Scope

This policy covers code and release artifacts from this repository. Vulnerabilities
in the Streamline server should be reported through the
[server security policy](https://github.com/streamlinelabs/streamline/security/policy).

General questions, feature requests, and non-sensitive bugs belong in the
[support channels](SUPPORT.md).
