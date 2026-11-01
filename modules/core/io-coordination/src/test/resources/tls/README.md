Test-only key material, generated with keytool (password `changeit`):

- `node.p12`: key pair used by every test node.
- `trust.p12`: trusts only the `node` certificate.
- `rogue.p12`: an unrelated key pair, to check that unknown certificates are rejected.
