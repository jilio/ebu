# Working conventions

These instructions apply to every contributor and coding harness.

## Releases

- Use one shared version for all four public modules: `github.com/jilio/ebu`,
  `github.com/jilio/ebu/stores/sqlite`, `github.com/jilio/ebu/stores/durablestream`,
  and `github.com/jilio/ebu/otel`. Never release a module independently.
- Publish the root tag `vX.Y.Z` and the corresponding `stores/sqlite/vX.Y.Z`,
  `stores/durablestream/vX.Y.Z`, and `otel/vX.Y.Z` tags as one project-wide
  release. Publish the root dependency before validating and publishing modules
  that depend on its new version.
- Keep direct dependencies between these modules and the example modules'
  dependency pins aligned with the shared release version.
- Compatible changes during `0.x` use a patch release. A minor release requires
  a breaking change or an explicit owner decision; an additive API alone does
  not justify a minor bump.
- Never rewrite or remove a published tag.
- Record changes in `CHANGELOG.md` and complete the formatting, tests, and
  coverage checks required by [CLAUDE.md](CLAUDE.md) before opening a pull
  request. Test each affected module; a root `go test ./...` does not cross
  module boundaries.
