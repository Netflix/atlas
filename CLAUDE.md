# Atlas

Netflix Atlas is a metrics platform for managing dimensional time series data. It uses a
stack-based query language (Atlas Stack Language / ASL).

## Contributing

Follow the guidelines in [CONTRIBUTING.md](CONTRIBUTING.md).

## Build and Test

Atlas uses SBT. Always use the project launcher, not a globally installed sbt:

```
project/sbt test                          # build and run all tests
project/sbt atlas-core/test               # test a single module
project/sbt 'atlas-core/testOnly *Suite'  # run a specific test suite
```

PR validation (license headers + formatting + tests):
```
make
```

## Code Style

- **Scala 2.13** with `-Xsource:3` cross-compilation flag
- Compiler flags include `-Werror` and `-Wunused` — unused imports are fatal
- Formatting enforced by [scalafmt](.scalafmt.conf) (maxColumn = 100, Scala213Source3 dialect)
- Check format: `project/sbt scalafmtCheckAll`
- Fix format: `project/sbt scalafmtAll`
- License headers (Apache 2.0, Copyright 2014-2026 Netflix) required on all source files
- Check: `project/sbt checkLicenseHeaders`
- Fix: `project/sbt formatLicenseHeaders`

## Project Structure

- `atlas-core` — core library: stack language interpreter, model types, data types
- `atlas-eval` — expression evaluation engine
- `atlas-chart` — graph rendering
- `atlas-json3` — JSON serialization (Jackson 3)
- `atlas-lsp` — Language Server Protocol for Atlas Stack Language
- `atlas-pekko` / `atlas-spring-*` — HTTP server and Spring integration
- `atlas-webapi` — web API endpoints
- `atlas-standalone` — standalone server assembly
- Dependencies: `project/Dependencies.scala`, modules: `build.sbt`

## LSP for Scala Metals

To use Scala Metals (IDE LSP support) with this SBT project:

1. Import the build via Metals "Import build" or run `project/sbt bloopInstall`
2. Metals uses Bloop under the hood — SBT must export the build to Bloop first
3. If Metals doesn't auto-detect, set the workspace root to the directory containing `build.sbt`

## Key Conventions

- One commit per PR (squash and merge is fine)
- Test cases should be small, focused, and quick to execute
- LSP4j interfaces must be implemented in Java (not Scala) due to annotation interop issues