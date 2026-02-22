# Overhop

> Overhop is an **LLM-Assisted Experimental Queue Orchestrator**

A server-side queue orchestration system built in Rust — exploring strict concurrency enforcement and what it actually looks like to develop software architecture collaboratively with LLMs.

This repository is both a technical project and a research log.

## Why This Exists

I've worked extensively with [Bull](https://github.com/OptimalBits/bull) and [BullMQ](https://docs.bullmq.io/). I genuinely like them:

- Clean API
- Good observability
- Clear state inspection
- Straightforward debugging
- Simple mental model

But two recurring limitations kept surfacing. First, **strict concurrency enforcement isn't truly enforceable in BullMQ** — the worker side can always misbehave, and the system won't stop it. Second, in real production work we genuinely lacked BullMQ-equivalent ports in Go and Rust. Early ideas to build those ports gradually evolved into something more ambitious: rather than porting an existing design, why not rethink the architecture from scratch in a language that forces the right tradeoffs?

That alone wouldn't justify rewriting a mature queue system for production use. But this project isn't about production — it's an architectural experiment, a Rust learning exercise, and an attempt to understand what LLM-assisted development actually looks like when the human stays in control.

## Core Idea

A standalone TCP-based queue orchestration server that:

- Enforces strict concurrency limits centrally — workers can't exceed them
- Owns scheduling and dispatch logic server-side
- Delegates work via subscription rather than polling
- Maintains explicit worker identity and metadata
- Uses a custom TCP wire protocol designed for this system specifically
- Supports pluggable storage backends via proxy abstraction — the system is deliberately not coupled to any single storage engine

No HTTP. No gRPC. A purpose-built protocol, designed incrementally alongside the architecture.

## Objectives

This is an experimental project, but it's not aimed at undermining LLMs or proving they can't be trusted. The main objective is the opposite: to use LLMs in a disciplined, structured way such that Overhop could become production-ready software one day.

That shapes everything about how the project is developed. It isn't a quick prototype or a throwaway research spike. It's built with proper documentation, changelogs, and CI/CD tooling from the start — because the experiment only means something if the output is something worth shipping.

## Changelog and Commit Discipline

The repository uses a strict, commit-coupled changelog workflow:

- `CHANGELOG.md` is the canonical release history.
- Unreleased work is tracked as fragment files in `.changelog/unreleased/`.
- Every meaningful commit should stage at least one fragment.
- Commit messages must follow readable Conventional Commit format.

Git hooks are versioned in `.githooks/`:

- `pre-commit` enforces changelog fragments for behavior/code changes.
- `commit-msg` enforces commit message quality and structure.

Hook activation for this clone:

`git config core.hooksPath .githooks`

Release flow:

1. Add fragments while implementing changes.
2. Optional helper: `scripts/new_changelog_fragment.sh <type> <slug> [message]`.
3. Run `scripts/release_changelog.sh <version> <YYYY-MM-DD>`.
4. Commit the release changelog update.

## Branch and PR Flow

Development should happen on topic branches and land on `main` via pull requests.

- Allowed branch prefixes: `feat/*`, `quickfix/*`, `chore/*`
- Direct pushes to `main` are disallowed by local git hook policy.
- Open a PR from topic branch to `main` for review and merge.

Typical flow:

1. Create branch: `git checkout -b feat/<short-topic>` (or `quickfix/...`, `chore/...`).
2. Implement changes and commit with changelog fragment.
3. Push branch and open PR to `main`.
4. Merge via PR after review.

## Architectural Philosophy

This is not "AI, build me a queue system."

The goal is conscious architectural development, where I use LLMs as a thinking partner — not as an autopilot. This isn't a discovery project either. I came into it with existing habits and observations from working with LLM assistants on real codebases, and those inform how I structure the collaboration.

### How I Work With LLMs

My primary coding assistant is **OpenAI Codex**. I also use a **custom ChatGPT model** and **Claude** (mostly Sonnet 4.6), depending on the task. The exact reasoning behind which tool gets which job is my sweet mystery — but the general rule is clear: **I don't write code or documentation myself**. Everything is delegated to the LLMs. My role is architectural decision-making, directing, reviewing, and course-correcting.

### Where AI helps

- Exploring design options and tradeoffs
- Scaffolding modules
- Drafting partial implementations
- Researching embedded storage backends
- Stress-testing ideas before committing to them
- Writing all documentation and changelogs

### What stays human

- Architectural decisions
- Module boundaries
- Directory structure
- Wire protocol evolution

One clear pattern has emerged: when I stay engaged, the AI output is excellent. When I step back and let it run ahead, subtle architectural problems accumulate — ones that are easy to miss and annoying to untangle. This repository documents both sides.

## How The System Works

### Strict Concurrency Enforcement

Concurrency limits are enforced by the server, not by convention. Workers cannot exceed declared limits — the orchestrator is the authority. There's no way to accidentally spawn too many concurrent jobs.

### Server-Owned Scheduling

Workers don't poll for jobs. Instead, they subscribe to channels and receive assignments from the orchestrator. Delays, scheduling decisions, and dispatch logic all live server-side. This centralizes control and removes a common source of drift in distributed job systems.

### Worker Identity

Workers aren't anonymous consumers. Each one registers explicitly, provides metadata, and is tracked in a worker pool. This makes concurrency control, diagnostics, and observability considerably more tractable.

### The Wire Protocol

The protocol is custom, TCP-based, and designed specifically for this system. It's defined incrementally — not written in one pass — and that's not arbitrary caution. It's a direct consequence of how deeply each message type is coupled to the rest of the system.

A `REGISTER` message isn't just a handshake — it needs to interact with the worker pool, the queue pool, and concurrency accounting. An `INFO` message has to reach into diagnostics and aggregate state from multiple subsystems. Every message type is effectively a small cross-cutting operation over the orchestrator's internals.

This has an important implication for LLM-assisted development: asking an LLM to implement the full wire protocol in one shot would be roughly equivalent to asking it to build the entire application. The protocol isn't a layer sitting on top of the architecture — it's threaded through it. Without a solid architectural plan and clear module boundaries already in place, a generated protocol implementation would either make implicit decisions about your architecture for you, or produce something that looks complete but falls apart the moment you try to integrate it.

And that's where the human role becomes non-negotiable. LLMs can genuinely help with drafting an architectural plan — exploring tradeoffs, sketching module responsibilities, stress-testing ideas before you commit to them. But doing that well requires someone with enough senior engineering experience to recognize when a proposed structure is actually sound versus when it's plausible-looking but subtly wrong. The LLM won't tell you it's guessing. It will produce confident output either way.

So the protocol evolves the same way the rest of the system does: one piece at a time, with a clear sense of what each addition touches and why.

### Storage Backend

Rather than coupling Overhop to a specific storage engine, the architecture uses **backend storage proxies** — an abstraction layer that lets the system work across different underlying engines without being rewritten around them. This is a deliberate design choice, not a deferral.

For Phase 1, the plan leans toward **native Rust embedded storage engines** — keeping everything in-process, without external dependencies. [Sled](https://docs.rs/sled) is a concrete candidate worth exploring. That said, I'm aware of the challenges this introduces: sled's design makes clustering more involved, though Raft-based replication is workable. I'm also mindful of the single-writer/many-readers constraints that come with mmap-based databases. Navigating those tradeoffs honestly — rather than defaulting to "just use Redis" — is part of what this project is here to investigate.

LLMs are useful here for comparative analysis, though they tend to reach for Redis fairly quickly. Pushing past that default is part of the experiment.

### Clients

The plan is to write four client implementations: TypeScript, Python, Go, and Rust. This will test protocol clarity, cross-language ergonomics, and API surface consistency. The server remains single-node for now — no clustering.

## Why Rust?

TypeScript is my primary language. I picked Rust deliberately:

- Its type system forces architectural decisions to be explicit
- It prevents certain classes of vague design from compiling at all
- It removes the familiarity bias I'd have in TypeScript
- Working in a language I'm less fluent in makes it clearer where AI scaffolding genuinely helps and where I still need to think carefully myself

One thing worth noting: as much as I'm not proficient in *writing* Rust, I find it quite readable and understandable — especially coming from TypeScript. The mental models transfer reasonably well. This is notably different from Go, which I've spent meaningful time with and found more foreign at the grammar level than its reputation as a simple language might suggest. Rust's explicitness, paradoxically, makes it easier to follow what's happening even before you're comfortable writing it yourself.

## What This Is (and Isn't)

This is a protocol design experiment, a concurrency control research project, a study in LLM-assisted modular development, and a Rust systems programming exercise.

It is not a production replacement for BullMQ, a drop-in distributed job system, or a cluster-ready solution.

If you're here to explore how architecture emerges under AI supervision, or how much control the human actually needs to retain — welcome.

## Contributions

Comments and criticism are highly appreciated — they're genuinely useful input for the research side of this project.

However, due to the LLM experimentation goal, **PRs will not be accepted until the experimentation phase is complete**. At that point, when the shape of the system is stable enough to collaborate on without disrupting the research, that will change. Until then, the most valuable thing you can offer is a sharp observation or a well-placed question.

## About the Author

Senior software engineer with versatile, cross-domain knowledge, flexibility, and a strong bias toward curiosity. Fullstack but backend-oriented. Primary stack: TypeScript and Python. Go at an early-mid level. Rust: actively learning.

Inclined toward functional software architecture. Generally the person in the room who says "yes, I can do it" — and actually does.
