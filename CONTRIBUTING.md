# Contributing

Thanks for looking. Most of what a contributor needs to know is in [`spec/`](spec/). This document is only about how changes get made.

## Before you start

The project is at M0. There is a lot of design written down and very little code, so the most useful contribution right now is reading a spec document and saying where it is wrong. The open questions are issues labelled `kind/open-question`, and an argument against one of the current answers is worth more than a patch.

If you want to write code, take a milestone issue or a piece of one, and say so on the issue first. The milestones are ordered so that the single node core exists before the cluster, and the cluster exists before the density work. Work on M2 before M1 is standing is work that gets thrown away.

## Running the checks

```
cargo xtask ci
```

That runs what the per-commit workflow runs, cheapest first, so a formatting mistake costs seconds rather than a full test run. The pieces:

```
cargo xtask layers      # the dependency graph against xtask/layers.toml
cargo xtask style       # prose against the house rules
cargo xtask msrv        # the workspace still builds on the oldest Rust the manifest claims
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features
cargo nextest run --workspace --all-features
typos
```

The msrv check needs that toolchain installed, which is `rustup toolchain install 1.88.0 --profile minimal`. Without it the task says so and carries on, because CI runs it either way.

Most of the node side only runs on Linux, and parts of it need root, KVM or a recent kernel. Tests that need those are gated on the environment and say why they skipped. A test that silently passes on a laptop because it could not run is worse than no test.

## What a change has to come with

**A change to behavior comes with a test that fails without it.** Not a test that exercises the code, a test that fails. If you cannot write one, say so in the pull request and explain why.

**A change to the lifecycle, placement or routing comes with a simulation scenario.** These are the parts where a bug is an interleaving, and an interleaving is only found reliably by `hive-sim` running it from a seed. Once the harness exists, the seed that found a bug is committed with the fix.

**A performance claim comes with the command that reproduces it.** Median of several runs with the spread, the machine described, and the losses next to the wins. Anything that is quoted outside a pull request comes from [hivebox-bench](https://github.com/tamnd/hivebox-bench) and not from a laptop.

**A change that touches the isolation boundary gets a second reviewer.** That is the guest agent protocol, the drivers, seccomp and Landlock profiles, the jailer setup, the eBPF programs and anything that parses input from inside a cell. Assume the code in the cell is hostile, because in RL it is being optimized against you.

**A new dependency is a reviewable decision.** Say why it is needed and what it pulls in. `cargo deny` enforces licenses, advisories and sources.

**A new `#[ignore]` comes with an issue number.** No test is deleted to make CI green.

## The layer rule

Each crate has a rank in `xtask/layers.toml`, and a crate may depend only on crates of strictly lower rank. `cargo xtask layers` is a required check. If a change needs an edge that goes the wrong way, that is a design conversation and not a rank edit. Usually it means a type belongs further down.

## Style

**Rust.** `cargo fmt` decides layout. Comments explain why, not what. Public items get documentation. Anything that can panic gets a `# Panics` section, and every `unsafe` block gets a `SAFETY:` comment saying which invariant makes it sound. Unsafe is forbidden at the crate root everywhere it is not needed, and where it is needed the crate says so at the top. Where a decision follows from the spec, cite it: `// spec/08_node_agent.md section 3` costs one line and saves the next person an afternoon.

**Prose.** README, spec documents, commit messages, issue and pull request text. Plain English, written the way you would explain it to a colleague. No em dashes and no en dashes: a comma, a colon, a full stop, parentheses or the word "to" always works. No horizontal rules, use a heading. Do not hard-wrap sentences across lines, because one line per paragraph makes diffs readable. `cargo xtask style` checks all three.

## Commits and pull requests

One logical change per commit. The subject line is imperative and under about seventy characters. The body says why, because what is in the diff.

Pull requests describe the problem, then the change, then how it was verified. Rebase rather than merge, so the history stays bisectable.

## Versions

The minor version is the number of milestones finished. Work inside M0 is 0.0.x, the release where M0's exit criterion passes is 0.1.0, and so on. Tagging is the whole release process: push a tag that matches the version in `Cargo.toml` and has a section in `CHANGELOG.md`, and the release workflow does the rest or refuses.

## Security

See [SECURITY.md](SECURITY.md). A way out of a cell is never a public issue.

## License

Apache-2.0. A contribution is offered under the same terms, which is what section 5 of the license says. There is no separate agreement to sign.
