# Repository guide for agents

Guide for agentic coding agents working in the `inkless` repository.

This document uses these requirement levels:

| Keyword            | How to treat it          |
| ------------------ | ------------------------ |
| MUST/REQUIRED      | mandatory                |
| SHOULD/RECOMMENDED | deviate only with reason |
| MAY/OPTIONAL       | use judgment             |

## Repository structure

`inkless` is a fork of Apache Kafka that adds Diskless Topics (KIP-1150). Most
of the tree is upstream Kafka. [`INKLESS_OWNERSHIP`](INKLESS_OWNERSHIP) records
which paths Inkless owns and which upstream files carry Inkless edits. If the
task is Inkless-related, work in the `OWNED` paths first (`@aiven/inkless`).

Some diskless logic is interleaved into upstream broker classes. It isn't
confined to an Inkless package. For example, `DisklessFetchOffsetRouter.scala`
and `InitDisklessLog*.scala` are new under `core/src/main/scala/kafka/server/`.
`ReplicaManager.scala`, `KafkaConfig.scala`, and `BrokerServer.scala` carry
heavy Inkless edits. Those files are the dual-owner `INTERLEAVED` entries in
the manifest. Diskless Scala isn't limited to `io/aiven/inkless/`.

Everything else is upstream Kafka. Follow upstream conventions when you edit
those files. Keep Inkless changes minimal and isolated.

## On-demand documentation

### Inkless documentation

If a task touches the relevant area, read the matching doc. All docs live under
[`docs/inkless/`](docs/inkless/). This table maps each doc to the question it
answers:

| Doc                                               | Read it when you need…                                                       |
| ------------------------------------------------- | ---------------------------------------------------------------------------- |
| `README.md`                                       | The documentation index and entry point.                                     |
| `ARCHITECTURE.md`                                 | The system design and how components fit together.                           |
| `GLOSSARY.md`                                     | Definitions of Inkless-specific terms.                                       |
| `FEATURES.md`                                     | Supported features, API compatibility, managed replicas, controller metrics. |
| `QUICKSTART.md`                                   | To run Inkless via Docker or locally.                                        |
| `PERFORMANCE.md`                                  | Producer/consumer tuning guidance.                                           |
| `CLASSIC_TO_DISKLESS_SWITCH.md`                   | To work on switching classic topics to diskless.                             |
| `CLIENT-BROKER-AZ-ALIGNMENT.md`                   | Multi-AZ deployment and cost optimization.                                   |
| `CREATE-TOPICS-INTERCEPTORS.md`                   | The interceptors on the `CREATE_TOPIC` path.                                 |
| `VERSIONING-STRATEGY.md`                          | The version format and release workflow.                                     |
| `RELEASES.md`                                     | Released artifacts (binaries, Docker images).                                |
| `SYSTEM_TESTS.md`                                 | To write or run system tests.                                                |
| `FAQ.md`                                          | Common questions.                                                            |
| `configs.rst`, `topic_configs.rst`, `metrics.rst` | Auto-generated config/metrics reference.                                     |

For upstream Kafka build, test, and tooling commands, see the root
[README.md](README.md). For upstream sync procedures, see
[`inkless-sync/`](inkless-sync/).

## Engineering rules

You MUST follow these rules and guidelines during development.

### Style

These rules apply to Inkless-owned docs, PR titles and bodies, commit messages,
and comments you write. Upstream Kafka files keep upstream style. Do not
rewrite existing comments or docs only to match this section.

Follow project conventions first, then this section. It is adapted from the
[Google developer documentation style guide](https://developers.google.com/style).
If something is not covered here, look it up there. Prefer a clear sentence
over a strictly compliant one. When you depart from the guide, stay consistent
in that file or PR.

#### Tone

Write like a knowledgeable teammate: conversational and direct, not stiff and
not cute. The job is to give the reader the fact they came for. Follow these
tone rules:

- MUST use active voice. Name the actor. "The broker retries the job." not
  "The job is retried."
- MUST use present tense for current behavior. "The server sends an
  acknowledgment." not "The server will send an acknowledgment." Use `will`
  only when the action is actually later (for example, a deferred archive).
- MUST NOT use `please` in instructions or PR bodies. MUST NOT call a change
  `simple`, `easy`, or `just`. Those words hide the real cost.
- SHOULD use common two-word contractions (`don't`, `isn't`, `can't`,
  `you're`). Negation contractions are easier to see while scanning than a
  lone `not`.
- SHOULD address the reader as `you` in docs and instructions. Use the
  imperative when telling them to do something ("Run the test."). In code
  comments, describe what the system does in third person ("The broker
  retries the job.").
- SHOULD NOT use first-person plural for the code (`we then update the
  offset`). `we` is fine only when it means the Inkless project.
- MUST NOT use slang, memes, exclamation points, or `tl;dr`.
- SHOULD put the condition or goal before the instruction, so the reader can
  skip it: "If the partition is diskless, skip ISR expansion." not "Skip ISR
  expansion if the partition is diskless."

#### Grammar and punctuation

Follow these grammar and punctuation rules:

- MUST use American spelling (`canceled`, not `cancelled`).
- MUST use the serial comma: "zones, regions, and multi-regions."
- SHOULD write short sentences and one idea per sentence when the alternative
  is a pile of clauses. If a second thought is spliced into the middle
  with em dashes or parentheses, give it its own sentence, or introduce it
  with a colon. An em dash is fine for a brief restatement or interruption
  that is not a new claim. Parentheses are fine for a bare label that has no
  thought of its own, such as an abbreviation gloss or a citation.
- SHOULD make pronoun antecedents obvious. Prefer "this value" over "this."
  Use singular `they`. Use `that` for restrictive clauses and `which` (with a
  comma) for nonrestrictive ones.
- MUST use a colon, not a dash, in a list of term/description pairs:
  `Term: description.` not `Term - description.`
- MUST NOT use `e.g.` or `i.e.`; write `for example` or `that is`.
- SHOULD NOT end a list with `etc.`; introduce the list as incomplete
  (`such as`) or name the items.
- MUST use one space between sentences.
- MUST NOT put a period on a heading.

#### Formatting

Follow these formatting rules:

- MUST use sentence case for headings and PR section titles: "Test plan" not
  "Test Plan." Task headings start with a bare infinitive ("Add the metric").
  Concept headings are noun phrases ("Retention metrics"). Avoid starting a
  heading with an `-ing` verb.
- MUST put identifiers, filenames, flags, class names, methods, config keys,
  status codes, and other code in backticks. Do not inflect a code token;
  add an English noun and inflect that: "`Intent` objects" not "`Intents`."
- MUST use descriptive link text. Write "For more information, see
  [ARCHITECTURE.md](docs/inkless/ARCHITECTURE.md)." not "click here" or
  "see this document."
- SHOULD number a list only when order matters. Use bullets otherwise.
  Introduce a list with a complete sentence, usually ending in a colon.
  Capitalize each item. End the item with a period if it contains a verb;
  skip the period for a single word, a code-only item, or a document title.
- SHOULD spell out an uncommon abbreviation on first use (`Border Gateway
  Protocol (BGP)`), then use the abbreviation. Do not spell out `API`,
  `HTTP`, `URL`, `SQL`, or file formats the audience already knows. Do not
  use an acronym as a verb ("Use SSH to log in," not "ssh into").
- MUST NOT use `&` as a substitute for `and`.

#### Word choices

Prefer the precise word. If a term is established Kafka vocabulary (`replica`,
`ISR`, `leader`, `follower`, `log`, `offset`), keep it.

| Avoid | Prefer |
| ----- | ------ |
| `allows you to` | `lets you` |
| `utilize`, `leverage` | `use` |
| `currently`, `at this time`, `as of this writing` | omit; state the fact |
| `will` for current behavior | present tense |
| `please note` | the note, with no preamble |
| `whitelist` / `blacklist` | `allowlist` / `denylist`, or the action ("deny requests from") |
| `dummy` (for a stand-in value) | `placeholder` |
| `above` / `below` for a position in a doc | `preceding` / `following` |
| `abort` in general prose about stopping work | `stop`, `cancel`, or `end` |
| `sanity check` | `check` |

Keep a term from the Avoid column when it names an existing identifier in Kafka or
Inkless rather than describing one. Write that name in backticks, and use the
preferred word only in prose that is not naming it.

#### Commits and pull requests

PR title and body become the squashed commit. Match existing Inkless
conventional-commit titles:

```text
type(scope): imperative description
```

Example: `fix(inkless:controller): skip switched partitions in unfence ISR
expansion`. After the colon, use a lowercase bare verb (`skip`, `add`,
`retry`). Not past tense (`skipped`) or future (`will skip`).

The body MUST say why the change exists, not only what files moved. Use
present tense for the new behavior. Put testing under a sentence-case
heading. Do not pad the body with "please review" or a recap of the diff.

#### Comments

Write self-documenting code. Comments SHOULD be rare and explain **why** the
code is this way, not **what** it does. Typical reasons are a non-obvious
constraint, a workaround, or a deliberate deviation. Match the surrounding
comment density and never restate the code.

When you do write a comment or a Javadoc/Scaladoc sentence:

- Use present tense and active voice.
- Do not start with "This class" or "This method."
- For a method that returns something, start with a verb: `Returns`, `Gets`,
  `Checks whether`, `Sets`, `Deletes`.
- Boolean returns: `True if ...; false otherwise.`
- Keep the first sentence able to stand alone; some generators take only that
  sentence as the summary.
