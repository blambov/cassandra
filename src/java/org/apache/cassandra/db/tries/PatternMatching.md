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
It is used to serve SQL `LIKE` queries from the terms dictionaries of storage-attached indexes, whose keys are the
UTF-8 encoded bytes of the indexed `text` and `ascii` values.

The obvious way to do this is to walk the whole trie and check every key against the pattern. The approach described
here matches the pattern while the trie is walked, one byte at a time, which lets the walk skip every branch that cannot
contain a match. For example, matching `alpha/*/report????.csv` only visits the branch starting with `alpha/`, and below
it only the branches that can still be followed by `report`, four characters and `.csv`.

## Patterns

A `TriePattern` is a sequence of elements, each of which matches
- a literal character,
- any single character (`?` in glob syntax, `_` in `LIKE` syntax), or
- any sequence of characters, including the empty one (`*` in glob syntax, `%` in `LIKE` syntax).

A key matches if all of it matches the pattern. Patterns and keys are UTF-8 encoded: the builder rejects literals that
are not valid UTF-8, and the matchers rely on the keys being valid UTF-8, without checking it. Literals are never
interpreted as wildcards; parsing a syntax with escapes is up to the creator of the pattern. `TriePattern.glob` parses
glob patterns without escapes, and `LikePatterns.forPatternOperator` translates `LIKE` patterns, with their `%` and `_`
wildcards and `\` escapes.

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
| any string          | all bytes                                     | yes  |
| character start     | `0x00`-`0x7F` and `0xC0`-`0xFF`               | no   |
| continuation bytes  | `0x80`-`0xBF`                                 | yes  |

A literal character is the sequence of literal bytes of its encoding. A UTF-8 character is one byte that is not a
continuation byte followed by its continuation bytes, so the any-character wildcard is expanded to a character start
followed by a continuation loop. This matches exactly one character, whatever the length of its encoding, provided that
the loop takes all the continuation bytes of the character (see below).

### Normalization

As the literals are valid UTF-8, the wildcards are on character boundaries: each one follows whole characters, and is
followed by the start of a character or the end of the key. `TriePattern.Builder` normalizes the pattern so that the
matchers can rely on a few properties:
- Runs of any-string wildcards are collapsed, and any-character wildcards are moved before them: `*?` becomes `?*`, as
  both match one or more characters. An any-string wildcard is therefore always followed by a literal or the end of
  the pattern.
- A character that is followed by an any-string does not need its continuation loop, as the any-string takes the
  continuation bytes.

As a result two loops are never adjacent, so skipping a loop never lands on another loop: adding `j + 1` for a loop at
`j` never has to be repeated.

The continuation loop of a character must take all of the character's continuation bytes, otherwise the element after
it could take some of them and the any-character wildcard would match part of a character. It does, because the
element after the loop is a literal that starts a character, another character start or the end of the pattern, none
of which accepts a continuation byte.

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

## Matching an on-disk trie

The matcher does not depend on cursors, and `org.apache.cassandra.io.tries.PatternValueIterator` uses it in the same
way to list the payloaded nodes of an on-disk trie whose keys match a pattern, in forward or reverse order. It walks
the nodes with a stack of positions, each with the index of the child being visited, and the matcher's state history
follows the depth of the stack:
- A child is only entered if the state for its transition byte is not empty. When a child is rejected, the walk
  continues from the child that holds the next byte the parent's state can accept, found with the node's `search`
  instead of examining each child in turn.
- When the state of a node is a single literal position, the only child to visit is found with `search`.
- Below the point where a trailing any-string wildcard matches, the state is not tracked and every payload is
  listed.
- In forward order the payload of a node is listed when the walk descends to it, and in reverse order when it ascends
  from it, after its children.

SAI uses it through `PatternTermsDictionaryReader` to serve `LIKE` queries from the on-disk terms dictionaries, as
`Trie.patternMatched` does for the memtable index.

## Matching single values

`TriePattern.matches` checks a single value with the same state machine: it starts from the initial state and reads
the value's bytes in turn, stopping early when the state becomes empty or a trailing any-string wildcard is reached. It
does not use the matcher's state history, so one pattern can check values concurrently. `Operator.LIKE_MATCHES` uses
it when rows are filtered with a `RowFilter` (e.g. by the coordinator's replica filtering protection). SAI uses it to
check every row it reads, as it does for all its restrictions: an index can return rows whose current value no longer
matches (the index of an sstable keeps listing a row under the value it had in that sstable), and the query planner
can choose to check a restriction on the rows instead of using its index.

`TriePattern.literalPrefix` returns the bytes of the leading literal elements, which every matching key starts with.
SAI uses the range of terms with that prefix to estimate how many rows a pattern selects and to skip the sstable
indexes whose terms cannot match.

## Representing the state

The set of positions is a bit set in which bit `i` stands for position `i`, so shifting the set left by one moves
every position past its element. The transitions above can then be computed for all positions at once with a few
bitwise operations (the "shift-and" algorithm), using two masks prepared from the pattern:
- `isLoop`, the set of loop positions, and
- `patternMatch`, a table with an entry for each of the 256 byte values, where `patternMatch[b]` is the set of
  positions whose element accepts `b`.

`patternMatch` is filled by going over the elements and setting bit `i` in the entry of every byte that element `i`
accepts (see the table of elements above):
- a literal `b` sets its bit in `patternMatch[b]` only;
- an any-string sets its bit in all 256 entries;
- a character start sets its bit in the entries of `0x00`-`0x7F` and `0xC0`-`0xFF`, and a continuation loop in the
  entries of `0x80`-`0xBF`.

For `a*b`, the pattern of the example above, this gives (bits written from position 3, the end, down to position 0):

| mask                                | bits   | positions               |
|-------------------------------------|--------|-------------------------|
| `patternMatch['a']`                 | `0011` | `0` (`a`) and `1` (`*`) |
| `patternMatch['b']`                 | `0110` | `1` (`*`) and `2` (`b`) |
| `patternMatch[x]` for any other `x` | `0010` | `1` (`*`)               |
| `isLoop`                            | `0010` | `1` (`*`)               |

A byte `b` is read with

```
match = state & patternMatch[b]
next  = match & isLoop | (match | (match << 1) & isLoop) << 1
```

where
- `match` is the set of positions in the state whose element accepts `b`,
- `match & isLoop` keeps the loops that accept `b`,
- `match << 1` advances every position that accepts `b`,
- `(match << 1) & isLoop` finds the loops that were just entered, which are shifted once more to skip them.

Reading the key `axb` with `a*b` again:

| byte | `match` | `next` | state       |
|------|---------|--------|-------------|
|      |         | `0001` | `{0}`       |
| `a`  | `0001`  | `0110` | `{1, 2}`    |
| `x`  | `0010`  | `0110` | `{1, 2}`    |
| `b`  | `0110`  | `1110` | `{1, 2, 3}` |

The operations do not know what the elements are; all of that is in the table, whose construction gives the matching
these properties:
- Literals and classes of bytes are treated alike: they only differ in how many entries have their bit. Reading a byte
  costs the same whatever wildcards the pattern has.
- Several positions can accept the same byte, as `*` and `b` do for `b`, and they all advance at once. This is how the
  state follows all the ways the key can be matched.
- The end position has no element, so its bit is in no entry, and reading any byte after it is reached clears it
  (reading `y` after `axb` gives `0110`). The end is only in the final state if the key ends where the pattern does.
- A loop stays in the state only for the bytes the loop accepts. For example, a continuation loop is left at the first
  byte that is not a continuation byte, which the element after the loop must then take.
- If no position in the state accepts the byte, `match` and the next state are empty, which is how a walk finds out
  that no key in a branch can match. Many entries are zero: for a pattern of literals, all but the entries of its
  bytes.
- An any-string bit is in every entry and in `isLoop`, so once an any-string position is reached, it stays in the
  state whatever bytes follow. When the pattern ends in an any-string wildcard, the matchers use its position as the
  end position: a key that reaches it matches, and so does every extension of the key (the `MATCHES_ALL_EXTENSIONS`
  flag). The end position after it is not used.
- The initial state is `{0}`, plus position `1` if element `0` is a loop, which is skipped like any loop that is
  entered.

The table also gives the bytes that can follow a path: `b` can if `patternMatch[b] & state` is not empty. When the
state has a wildcard position, `nextAcceptedByte` uses this to find the next byte the walk has to visit; when it only
has literal positions, it looks at their bytes instead.

Two implementations differ in how they store the set:
- `PatternMatcher64` uses a single `long`, which serves patterns of up to 63 positions (bit `n` is the end). Its table
  is 256 `long`s.
- `PatternMatcherLong` uses an array of `long`s for longer patterns, carrying the top bit of each word into the next
  word when shifting. Its table has an array of `long`s for each byte, and the bytes that no element accepts share one
  array of zeros. As a trailing any-string wildcard is the end position, a pattern of 64 positions that ends in one
  still fits in a single word.

`descend` returns the flags of the new state (whether it is a match, matches all extensions, or contains an any-string
or other wildcards), which the walks keep for the current node instead of querying the state again. The bytes of chains
received through `advanceMultiple` are processed in a single call, so that the matcher's state can stay in a local
variable while they are processed.

`PatternMatcher.create` chooses `PatternMatcher64` when the pattern fits, as it is the faster of the two.
