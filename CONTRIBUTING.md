# Contributing to nats-s3

Thanks for your interest in improving nats-s3! This project is in an early stage, so the process is intentionally lightweight.

## Getting Started
- Prerequisites:
  - Go 1.22+
  - Optional: a local NATS server with JetStream enabled for manual testing (`nats-server -js`)
- Build: `go build ./cmd/nats-s3`
- Run: `./nats-s3 --listen 0.0.0.0:5222 --natsServers nats://127.0.0.1:4222`
- Test: `go test ./...`

## How We Work
- Prefer opening an issue or discussion before large changes to align on direction.
- Keep pull requests small, focused, and easy to review.
- Include tests for new behavior or bug fixes when reasonable.
- Update docs when you change user-facing behavior.

## Branching & Commits
- Branch naming suggestions:
  - Features: `feat/<short-description>`
  - Fixes: `fix/<short-description>`
  - Chores/Docs: `chore/<short-description>`, `docs/<short-description>`
- Commit messages: prefer conventional-style prefixes, for example:
  - `feat: add DELETE object route`
  - `fix: correct ETag and Last-Modified headers`
  - `docs: improve README usage examples`
  - `test: add unit tests for object handlers`

## Coding Style
- Run the formatter before committing: `gofmt -s -w .`
- Keep changes minimal and focused on the task at hand.
- Be careful with logging; avoid logging full payloads or sensitive data.
- Favor clear, concise comments and Go-doc comments for exported items.

## Testing
- Run all tests: `go test ./...`
- For NATS-dependent tests, use `github.com/nats-io/nats-server/v2/test` helpers to start an in-process server with JetStream enabled.
- Aim for table-driven tests where practical and prefer deterministic behavior.

## Pull Request Checklist
- [ ] Code is formatted (`gofmt -s`)
- [ ] Unit tests added/updated and passing (`go test ./...`)
- [ ] Behavior and public APIs documented (README or comments)
- [ ] PR description explains the problem and the solution briefly

We appreciate your contributions and feedback! If something is unclear, please open an issue and we’ll help clarify and improve the docs and process.
