# Releasing

Releases go to Maven Central through [sbt-ci-release](https://github.com/sbt/sbt-ci-release) and the
[Sonatype Central Portal](https://central.sonatype.com). Scaladex picks them up from Maven Central.

## One-time setup

1. **Namespace.** Sign in to <https://central.sonatype.com> with the GitHub account `joacovela16`. The namespace
   `io.github.joacovela16` is verified automatically.
2. **Publishing token.** In the portal, *View Account → Generate User Token*. Save the username and password as the
   GitHub secrets `SONATYPE_USERNAME` and `SONATYPE_PASSWORD`.
3. **Signing key.** Maven Central requires signed artifacts:

   ```bash
   gpg --full-generate-key                                   # RSA 4096, no expiry or a long one
   gpg --list-secret-keys --keyid-format=long                # note the key id
   gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID> # the portal checks signatures against public keyservers
   gpg --armor --export-secret-keys <KEY_ID> | base64 -w0    # value of the PGP_SECRET secret
   ```

   Save the key passphrase as the GitHub secret `PGP_PASSPHRASE`.
4. **Public repository.** Scaladex only indexes public GitHub repositories, matched through the `scm` URL of the POMs
   (`https://github.com/joacovela16/x-actor-platform`).

## Publishing a version

```bash
git tag -a v0.1.0 -m "v0.1.0"
git push origin v0.1.0
```

The [Release workflow](.github/workflows/release.yml) builds, signs and uploads every module except `root` and
`examples`. The version comes from the tag. After the portal validates the bundle, the artifacts reach Maven Central in
about 30 minutes, and Scaladex lists them within a few hours. To edit the Scaladex page (topics, description,
documentation link), sign in to <https://index.scala-lang.org> with GitHub and open the project settings.

## Checking locally

```bash
sbt publishLocal   # publishes to ~/.ivy2/local with a snapshot version derived from git
sbt makePom        # writes the POMs under each module's target/scala-2.13
```
