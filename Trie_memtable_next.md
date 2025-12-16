# Trie memtable next


## Embrace point tombstones

The complexity comes from knowing what to switch to when exiting a branch. 

For example,
```
abc -> START(555)
 cc -> POINT_WITHIN(777, 555)
 de -> END(555)
```

`precedingState` works forward and back around "acc", but how about when we enter the branch? Cursor will be positioned
ahead (otherwise we don't know there's substructure), and `precedingState` will be 555. How to know when to switch 777
to 555?

Other examples:

```
abc -> start(555)
 cc -> end(555), point(666), start(777)
   dd -> end(666), start(888)
   ee -> end(888), start(666)
 ee -> end(777)
```
to represent `[abc,acc)@555, [acc,accdd)@666, [accdd,accee)@888, [accee,acc]@555, (acc,aee)@777`

```
a -> point_start(666)  (precedingState:null in both directions, switches to 666 only if going forward)
 bc -> end(666)
```
to represent `[a, abc)@666`

```
a -> start(111)
 a -> end(111), point_start(222), point_end(333), start(444)
  a -> end(222), start(333)
b -> end(444)
```
to represent `[a, aa)@111, [aa, aaa)@222, [aaa,aa]@333, (aa, ab)@444`


We kind of need a "point" to be a pair of boundaries applicable at the positions just before point and just after point.

Something like `concat(key, -1)` and `concat(key, 256)` switchpoints.

### The current way

We currently do have to handle this for inclusivity at the boundaries. This relies on knowing boundaries have no
children and doing some special advancing for them.

### POINT vs POINT_WITH_SUBSTRUCTURE?

In other words, have a "has_children" flag on the state? Then we could use the same magic as we do for end inclusivity


### Make use of incomingTransition space

The idea here is to use/return `3*x + 1` for `incomingTransition` instead of `x`, and map points to `3*x + 0` and `3*x + 2` boundaries for range cursors. Then we have a full separation between, on one hand, data keys and branches, and, on the other, deletion boundaries. Remainder 1 cannot have content in range branches (what about metadata?). Remainders 0 and 2 cannot have children and must have boundary content.

E.g. point(aaa) must become start(aaa_before), end(aaa_after), where aaa maps the original 414141 to C4C4C4 and aaa_before is C4C4C3 and aaa_after is C4C4C5.

This increases the `incomingTransition` space but is otherwise very clean and should need some trivial modifications to merge/intersection.

We will likely use a multiplier of 4 to avoid the division by 3.

Can we do it with `2*x` for before and `2*x+1` for after? 
- If we attach branches to the +1 head for reverse direction. A little harder to grasp, but doable. Benefit: don't stop at before, at, after but rather at before+at, after. Perhaps do the 3x option first, and add this in separate commit as optimization.
- If we make after(aa) the same as before(ab). We need 0 to 2\*256 inclusive for this. Here the problem is that we don't know how to map back before(ab) to after(aa) for point(aa) esp if there's point(ab) too.

One direct simplification that comes from this is that we know we have to use 

The complication is that `InMemoryRangeTrie` and the on-disk version should translate to and from this encoding as it must store the original key bytes to avoid space blowup.

In the in-memory trie we will have to store up to four-segment states (end + point_start + point_end + start)



### Add explicit before/at/after modifier to use in cursor comparisons on match?

E.g. `RangeCursor.pointOffset()` returning -1 to 1. This parameter should also be added to `skipTo`.

Simple tries will always have 0 here.

This may be a precursor to moving to the choice above.

In fact, maybe we can define the whole thing with `pointOffset` and eventually combine -depth, incomingTransition and pointOffset into a single int (20 bits for depth should be enough, fallback to `depth()` can be implemented) or long to be returned from `advance` etc. (incomingTransition == -1 or depth == -1 are not really necessary)


### What if we further split `content` (not on child-bearing position) from `metadata` (only on child-bearing position)?

E.g. -2 for before, 1 for content, 0 for branch and metadata, 1 for after

TBH I don't quite see the value.

## Random unedited/unclassified


How do we present the switch back to 555 on exit from "acc"? We actually have it in precedingState(ade).



In set definition points need to be duplicated, and any substructure must fall within them. E.g.
`[a, abc, acd, a]`
for
```
a -> POINT
 bc -> END
 cd -> START
```



One possibility for cursor is to stop on interesting prefixes on the way back

## Prefix support in ranges

The main problem is that the prefix point must be seen when going back through

Range covering `a -> abc`

[a, abc]?


## Prefixes in `RangeCursor`

onReturnPath bit:
  - Always 0 for non-boundary nodes
  - 0 for left indexes (&1 == 0) and 1 for right (&1 == 1) (subtries with prefixes and descendants) both fwd/rev
  - 0 for inclusive-left and exclusive-right (slices) fwd, and 1 rev

Right side of onReturnPath=0 applies to branch. Left side of onReturnPath=1 applies to branch.

[a, aaa]:
  - a with false->true
  - aaa^ with true->false
    - a's combined state is false->true->false->false

[a, aa, ac, a]:
- a with false->true
- aa^ with true->false
- ac with false->true
- a^ with true->false
    - a's combined state is false->true->true->false

[a, aa, ac, e]
- a with false->true
- aa^ with true->false
- ac with false->true
- e^ with true->false
    - a's combined state is false->true->true->true




[a, aa), (a, b]

    - a: false->true->false->true

[aa, aaaa, aaac, aa]
- a START_END_PREFIX (0 (no left) to 4e (no right))
- aa START (0 (no left) to 1e (right) applies only, 1-3 advanced but not processed)
- aaa END_START_PREFIX (1 (left) to 3e (right)), 1-2 advanced
- aaaa^ END (1 (left) to 2e (no right))
- aaac START (2 (no left) to 3e (right))
- aa^ END (3 (left) to 4e (no right))


[a, b, bbb, c]
- invalid (b^ is after bbb in iteration order, should be [a, bbb, b, c])

### Alternative
 - 1/2 for path (fwd/rev)
 - 0/3 for before
 - 3/0 for after
 - Can be ^ -1'd to change direction safely


# Slices

Forward path is easy, add0 works fine.

However, on reverse we have the content() presented on the reverse-side boundary.

Example

(null, a) fine

(null, a] maps to (null, a0) which in turn is

```
Forward
-> START
a ->
  0 -> END

Reverse
->
a ->
  0^ -> START
a^ -> END
```

a's content is presented on "a" (descent-side) in either direction. This means we see NOT_CONTAINED in the reverse
direction.

## Different trail bits for content vs children/metadata?

In the four-state transition trail approach, use:
- 0 (-) for boundary before
- 1 (*) for content
- 2 (=) for metadata and children
- 3 (+) for boundary after

Above would look like (0/1/2/3 shown as -/*/=/+ here, without remapping for reverse).

```
Forward
- -> START
= ->
a* walked
a= ->
 0- -> END

Reverse
= ->
a= ->
 0- -> START
a* walked
- -> END
```

Too complex? Is it a performance hit?

get() should use = for all bytes except the last where it should be *.

Dump should only list non-= positions when they have content (e.g. -> / *> / => / +> )

In-memory thing takes all the pain. Now a node can have up to 4 different content values (if we want metadata in range
tries) plus alternate branch.

Perhaps include trail bits in the content id encoding? Use prefix nodes when there's more than one.


This approach is developed in CNDB-15669-four-state-adjustment branch. There are two problem points:
- That the root does not start in a "branch" state (easily fixed by adding a `skipToRootBranch` method)
- That tails are taken at the branch position, which may lose information. In particular, RangesCursor tails are pretty
  difficult to get right (we lose either leading bound or root branch position).


## Represent as the proper set?

Not easy at all.

for [bb, bb] that would be something like [bb, bb00, bbff^, bb^], i.e.

```
bb -> START  (w content)
  00 -> END
  FF^ -> START
bb^ -> END
```
so that reverse can be
```
bb -> START  (w content)
  FF -> END
  00^ -> START
bb^ -> END
```

(aa, bb) would be [aa00, aaff^, aa^, bb]

```
aa ->  (no content presented)
  00 -> START
  FF^ -> END
aa^ -> START
bb -> END
```
reverse
```
bb^ -> START
aa -> END
  FF -> START
  00^ -> END
```

and

(aa, bb] as [aa00, aaff^, aa^, bb00, bbff^, bb^]

```
aa ->  (no content presented)
  00 -> START
  FF^ -> END
aa^ -> START
bb ->  (w content)
  00 -> END
  FF^ -> START
bb^ -> END
```
reverse
```
bb -> START  (w content)
  FF -> END
  00^ -> START
aa -> END
  FF -> START
  00^ -> END
```


Content on root and prefixes when we have open sides:

(null, aa) same as [empty, aa)

```
-> START
aa -> END

->
aa^ -> START
^ -> END
```


[aa, null)



[aa, aa00) as [aa, aa00, aaFF^, aa^]

[aa, aabb) as [aa, aabb, aaFF^, aa^]

```
aa -> START
  bb -> END
  FF^ -> START
aa^ -> END

aa -> START
  FF -> END
  bb^ -> START
aa^ -> END
```


[aa, aabb00) as [aa, aabb00, aaFF^, aa^]?
aabb needs to be included.



This does not work.


## Write a slice cursor implementation to deal with this mess specifically for SAI?

Resurrect the code we had before CNDB-10302?

Making it as set would still need a different Slice intersection for the prefixes.


## Present content on the return path in reverse direction?

This is basically the effect of the four-state adjustment without the extra states, where we still start and can take
tails on the root branch.


# Trie-backed rows

## TTL and expiration

Definition: An expiring cell has `localDeletionTime` defined. When `nowInSec` is below the deletion time, the cell is
live, i.e. has a value. When `nowInSec` is above the deletion time, the cell is deleted, i.e. its timestamp and
local deletion time now for a DeletionTime for it.

One idea was to split expiring cells into tombstone and value, but that's a lot of extra data for no real benefit, and
it doesn't help the main issue.

Main issue: We need to walk over dead cells to find the next live cell when data is read. We can't rely on compaction
alone to avoid it.

Secondary problem: We need to convert data branch to tombstone during or after merges to move expired cells to deletion
branch. Skipping this makes main issue worse.

Main solution is branch metadata: 
- If we store max local deletion time:
  - we know if branch has any non-expired data and can fully skip it
  - we can fully purge tombstone branches
- If we store min local deletion time:
  - we know if we there's anything in the branch that needs to be converted to tombstone
  - we know if we need to apply any tombstone purging
- With max timestamp, we can drop branches that are fully deleted during merges when there are no deletion path children.


We can also perform compaction on the merged tries, writing out data and tombstones to separate sections of the file.

This avoids having to go back and reread branches in order to apply
- deletions to data and deletions to deletions
- data to data and expired data to deletions


## Column IDs

Use column index as cell key -- note that this changes because it is ordered by name, not by addition time.

If there's a mismatch, switch to materialized rows and using legacy iterator merging etc.

In later iterations we can assign fixed indexes to columns and reorder in coordinator/client. 


# Done

- Include direction bit/byte in the encoding

- (partially) Express depth limits as position limits (e.g. `positionForSkippingBranch` and `<` instead of `depth` and `>`)

- Make RangesCursor work with encoded positions instead of next/depth arrays.

- Use root on return path for set/range end state instead of at exhausted

- Test presenting content on the return path in reverse direction (i.e. singleLevelIntTrie support for content-to-the-left)

- Implement negation

- SingletonCursor option to present on the return path.


- inMemoryReadTrie support to report content on the return path and putSingleton versions for:
    - content strictly to the left of the branch (lower range bound or ordered content):
        - forward: with branch
        - reverse: return path
    - content always with the branch (metadata, i.e. content to always be presented on prefixes)
        - forward: with branch
        - reverse: with branch
    - content strictly to the right of the branch (upper range bound)
        - forward: return path
        - reverse: with branch

  Done with two content slots and implementation-specific coding:
    - normal tries only have one content slot
    - InMemoryTrie has an option to be "ordered" and presented content on the return path in reverse direction
    - range tries have two slots, one strictly before and one strictly after the branch
    - deletion-aware can have normal content (ordered or not) and alternate branches
    - alternate branches are range tries, with their two slots and logic

- Implement proper return path treatment in InMemoryTrie delete.

- Implement everything needed for deletion-aware.

- Adjust TrieBackedPartition, including change partition deletion to be on partition root.

- Change SAI's usages to use orderer trie.

- Test return path seeks.

- Implement `mapValues` throughout hierarchy. `mapValuesAndDeletions` for deletion-aware.

- `TrieBackedRow`:
    - RowData is liveness info (with maybe stats later)
    - Markers for complex column roots
    - `TrieBackedComplexColumn` implementation
    - Cell<?> without path (but with column reference) at leaves
    - Don't move deleted cells to deletion branch for now

- Make deletion-aware `tailTrie` include deletion branch.
- Make deletion-aware `tailTrieIterator` include deletion branches.
- Make deletion-aware `tailTrieIterator` switchably ignore all deletion branches.


# TODOs

- Implement cell-level trie with pojo content.

- Test `mapValues`.

- Implement specialized `DeletionAwareTrie.mergeWithDeletion(RangeTrie)` and `InMemoryDeletionAwareTrie.delete(RangeTrie)`.
  Test.

- Test deletion-aware `tailTrie` including deletion branch.

- Test deletion-aware `tailTrieIterator` including deletion branches.

- Add tests for prefixed ranges throughout (subtrie, ranges, intersection, range merge, range intersection, deletion-aware).

- `hasContent` flag
- `hasDeletionBranch` flag on deletion-aware

- Implement directly-stored content and adjust cell-level trie to make it fully off-heap.

Maybe:
- `hasPrecedingState`/`hasSucceedingState` flag on range cursors (including sets)

- `hasChildren` flag

- (Not necessary) Multiple children flag. Perhaps two variations:
  - `HAS_MULTIPLE_CHILDREN` only true if known, merges use set|source, don't add even if they may result in multiple
    children. 
    Cleared by intersection.
  - `HAS_AT_MOST_ONE_CHILD` only true if known. Intersections set at set|source but don't add on mismatch. 
    Cleared by merge.

  Merges clear the flag.

Difficult:
- Change InMemoryRangeTrie cursor's skip not lose nearest content when skip acts as advance.
- Make InMemoryRangeTrie cursor's `getNearestContent` directly walk trie nodes.


## CollectionMergeCursor

        // TODO: Keep track of deletion state to avoid repeated calls to `relevantDeletions.precedingState`
        // TODO: Consider not applying deletions to live and making a `Shadowable` deletion-aware variation, delaying
        // the deleted data removal to after transformations have been applied.

## InMemoryDeletionAwareTrie.apply

            // TODO: Consider walking both data and deletion branches in parallel.

## TrieBackedPartition

putInTrie:

        // TODO: Direct insertion methods (singleton known to not be deleted, deletion known to not delete anything)


putMarkerInTrie:

        // TODO: Standalone partitions would not delete their own data, we could tell the trie not to go over the live path.

RowData:

        // TODO track minTimestamp to avoid applying deletions that do not do anything

