# Claude Code Instructions

## Before Creating Pull Requests

Before creating any pull request, you MUST:

1. Run `go fmt ./...` to format all Go code
2. Run tests with `go test ./...` to ensure all tests pass
3. Check test coverage with `go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out`

## Code Quality Standards

- Maintain 100% test coverage for core functionality
- Follow Go idioms and best practices
- Use the options pattern for configuration where appropriate
- Keep implementations simple and focused