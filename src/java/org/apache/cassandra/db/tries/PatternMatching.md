<!---
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Pattern matching on tries

`Trie.patternMatched(TriePattern)` returns a view of a trie that only presents the content whose keys match a pattern.
It is used to serve SQL `LIKE` queries from the terms dictionaries of storage-attached indexes, whose keys are the raw
bytes of the indexed values.

The obvious way to do this is to walk the whole trie and check every key against the pattern. The approach described
here matches the pattern while the trie is walked, one byte at a time, which lets the walk skip every branch that cannot
contain a match. For example, matching `alpha/*/report????.csv` only visits the branch starting with `alpha/`, and below
it only the branches that can still be followed by `report`, four bytes and `.csv`.

## Patterns

A `TriePattern` is a sequence of elements, each of which matches
- a literal byte,
- any single byte (`?` in glob syntax),
- any single UTF-8 encoded character (`_` in `LIKE` syntax), or
- any sequence of bytes, including the empty one (`*` in glob syntax, `%` in `LIKE` syntax).

A key matches if all of it matches the pattern. Literal bytes are never interpreted as wildcards, so a pattern can
contain any byte; parsing a syntax with escapes is up to the creator of the pattern. `TriePattern.glob` parses glob
patterns without escapes, and `LikePatterns.triePatternForPatternOperator` translates `LIKE` patterns, with their `%`
and `_` wildcards and `\` escapes.

## Matching a key with a set of positions

Number the elements of a pattern of length `n` from `0` to `n-1`, and call position `i` the point in the pattern
before element `i`. Position `n` is the end of the pattern. While a key is read, the state of the match is the set of
positions that the bytes read so far can reach. The key matches if, after its last byte, the set contains position `n`.

The state starts as `{0}`. To read a byte `b`, every position `i` in the current state whose element accepts `b`
contributes position `i + 1` to the next state. Some elements, the _loops_, can be repeated any number of times,
including zero:
- if element `i` is a loop that accepts `b`, position `i` also stays in the state (the loop takes another byte);
- whenever position `j` of a loop is added to the state, position `j + 1` is added too (the loop is skipped).

For example, for the pattern `a*b` the positions are `0` (before `a`), `1` (before `*`), `2` (before `b`) and `3` (the
end). Reading the key `axb`:

| byte | state      | notes                                                      |
|------|------------|------------------------------------------------------------|
|      | `{0}`      |                                                            |
| `a`  | `{1, 2}`   | `a` takes `0` to `1`, `1` is a loop so `2` is added        |
| `x`  | `{1, 2}`   | `*` accepts `x` and stays at `1`, also advancing to `2`    |
| `b`  | `{1, 2, 3}`| `*` stays and advances, `b` takes `2` to `3`               |

The final state contains `3`, so the key matches. Note that the state holds all the ways the bytes read so far can be
matched at the same time, so the matcher never backtracks: reading a byte takes time proportional to the length of the
pattern, regardless of how many stars it has. (This is the classic simulation of a nondeterministic automaton, whose
states are the positions of the pattern.)

### Elements

The elements are classes of bytes, some of which are loops:

| Element             | Accepts                                       | Loop |
|---------------------|-----------------------------------------------|------|
| literal `b`         | `b`                                           | no   |
| any byte            | all bytes                                     | no   |
| any string          | all bytes                                     | yes  |
| character start     | `0x00`-`0x7F` and `0xC0`-`0xFF`               | no   |
| continuation bytes  | `0x80`-`0xBF`                                 | yes  |
| none                | no byte                                       | no   |

A UTF-8 character is one byte that is not a continuation byte followed by its continuation bytes, so the any-character
wildcard is expanded to a character start followed by a continuation loop. In valid UTF-8 this matches exactly one
character, whatever the length of its encoding, provided that the loop takes all the continuation bytes of the
character (see below).

### Normalization

`TriePattern.Builder` normalizes the pattern so that the matchers can rely on a few properties:
- Runs of any-string wildcards are collapsed, and any-byte wildcards are moved before them (`*?` becomes `?*`, which
  matches the same keys). An any-string wildcard is therefore always followed by a literal, a character start, or the
  end of the pattern.
- A character that is followed by an any-string does not need its continuation loop, as the any-string takes the
  continuation bytes.

As a result two loops are never adjacent, so skipping a loop never lands on another loop: adding `j + 1` for a loop at
`j` never has to be repeated.

Any-character wildcards are not moved before any-string ones like any-byte wildcards are. After an any-byte wildcard,
which can take part of a character, the position may be in the middle of a character, and there `*_` and `_*` match
different keys.

The continuation loop of a character must take all of the character's continuation bytes, otherwise the element after
it could take some of them and the any-character wildcard would match part of a character. The builder makes sure that
the element after a continuation loop never accepts continuation bytes: an any-byte becomes a character start (a single
byte that is not a continuation byte) and a literal continuation byte becomes the `none` element (as nothing valid can
follow).

## Matching a trie

The state reached by a key prefix only depends on that prefix, so when the trie is walked, the state at a node can be
computed from the state at its parent and the byte that leads to the node. A `PatternMatcher` keeps the state at every
depth of the current path, and `descend(depth, byte)` computes the state at a depth from the state at the previous
one. The walk can move on to a sibling or to any ancestor's sibling, because their parents' states are still in the
history.

`PatternCursor` wraps the cursor of the source trie and uses the states as follows:
- If the state at a node is empty, no key in the node's branch can match, and the cursor skips the whole branch.
- If the state contains the end position, the node's content is presented; otherwise it is hidden.
- If the state at a node contains only literal positions, only their bytes can follow. If there is just one, the cursor
  descends to the child with that byte directly (using `skipTo`) instead of going to the first child. When a child is
  rejected, the cursor skips to the next byte that any position in the parent's state accepts, instead of visiting each
  sibling in turn.
- When the pattern ends in an any-string wildcard and its position is reached, every key in the branch matches. The
  cursor stops tracking the state until the walk leaves the branch, and a tail cursor taken in the branch is simply the
  source's tail cursor.
- While the state contains an any-string wildcard, no branch can be ruled out, and the walk is as expensive as a full
  walk of the source. The cursor then uses the source's `advanceMultiple`, which can move over chains of single-child
  nodes in one step, and receives the bytes of the chain through `TransitionsReceiver` to keep the states up to date.
  Without an any-string, the cursor advances one step at a time, so that it can return to skipping as soon as only
  literal positions are active.
- In reverse direction, ordered tries present the content of prefixes on the return path, after the node's children.
  The cursor does not descend from such a position, as the children have already been visited.

A tail cursor (`Cursor.tailCursor`) continues matching the pattern from the state of the node it starts at: the
matcher's `tail(depth)` creates a new matcher whose initial state is the state at that depth.

The matcher does not depend on cursors, so that it can be used to walk other trie representations in the same way.

## Representing the state

The set of positions is a bit set, and the transitions above can be computed for all positions at once with bitwise
operations (the "shift-and" algorithm). For each byte `b`, `patternMatch[b]` is the set of positions whose element
accepts `b`, and `isLoop` is the set of loop positions. Then

```
match = state & patternMatch[b]
next  = match & isLoop | (match | (match << 1) & isLoop) << 1
```

where
- `match & isLoop` keeps the loops that accept `b`,
- `match << 1` advances every position that accepts `b`,
- `(match << 1) & isLoop` finds the loops that were just entered, which are shifted once more to skip them.

Two implementations differ in how they store the set:
- `PatternMatcher64` uses a single `long`, which serves patterns of up to 63 positions (bit `n` is the end).
- `PatternMatcherLong` uses an array of `long`s for longer patterns, carrying the top bit of each word into the next
  word when shifting.

When the pattern ends in an any-string wildcard, its position is used as the end position: reaching it means the rest
of the pattern matches anything. The state at that point holds the any-string bit, from which the matcher can tell that
every extension of the path matches (the `MATCHES_ALL_EXTENSIONS` flag).

The cursor asks the matcher for the flags of each state once (whether it is a match, matches all extensions, or
contains an any-string or other wildcards), and the bytes of chains received through `advanceMultiple` are processed in
a single call, so that the matcher's state can stay in a local variable while they are processed.

`PatternMatcher.create` chooses `PatternMatcher64` when the pattern fits, as it is the faster of the two.
