# Trie memtable next

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

- Implement cell-level trie with pojo content.

- TrieTombstoneMarker point + boundary combination

- LivenessInfo for row header

- Put the upserters etc. in InMemoryTrie's mutator class
- Synthetic marker identification machinery for InMemoryTrie, i.e. code to drop content if branch becomes empty.
- Complex columns should not have a marker if they don't have cells.

- `mappingMergeWith` to apply resolvers on null values.

- Extract the object management code from InMemoryTrie to make it pluggable.

- Implement object management replacement that distributes memory from the allocator.
- Implement new trie cell type for directly stored payloads of up to 32 bytes and use whenever data would fit.

- Reuse TriePartitionUpdater now that it doesn't use cloner

- Move `danglingMetadataCleaner` to content manager to avoid materializing objects

- Figure out index handling.

- Check if TrieSet.range/slice can have better names

- Add tests for prefixed ranges throughout (subtrie, ranges, intersection, range merge, range intersection, deletion-aware).

- Test `mapValues`.

- Test dangling metadata cleaning

- Test `TrieTombstoneMarker`

- Test deletion-aware `tailTrie` including deletion branch.

- Test deletion-aware `tailTrieIterator` including deletion branches.

- Add `mappingMergeWith` to rest of trie hierarchy and test all.

- Test `!includeCoveringDeletions`, also add it to tailTrie.

- Add documentation for TrieMemtable:
    - Structure of the trie, markers
    - Handling of deletion types
    - Handling of cells (deleted or not)
    - Substructure as tail tries
    - etc.

# TODOs

- Add a commit adding indexing support after first Cell-level trie implementation

- Test range prefixes with content at EMPTY.

- Restore `path != null` checks in `BufferCell` etc after switching to `CellData`

- Test `Mutator.getXXXTailTrie`

- Test `DeletionAwareTrie.prefixedBySeparately`

- Change FlexibleMergeCursor.WithMappedContent to take a direction argument in the resolver (with direction-less version)

- Implement specialized `DeletionAwareTrie.mergeWithDeletion(RangeTrie)` and `InMemoryDeletionAwareTrie.delete(RangeTrie)`.
  Test.


- `hasContent` flag
- `hasDeletionBranch` flag on deletion-aware

- `RangeApplyCursor` could save range state after advancing to avoid repeated calls


- Figure out what to do about PartitionUpdate and Memtable data size not including keys.

- What if the columns set changes and moves column indexes? Reject updates that don't fit our initial metadata?
  (This only applies to the race between changing schema and flush completes.)

### Maybe:
- More flexible tombstone intersection so that partition or row tombstones remain at that level when intersected. This
  is not easy as we need stop on the return path when skipping.

- Make InMemoryRangeTrie cursor's `getNearestContent` directly walk trie nodes (needs directed getFirstChild method).

- Map value only for intersection

- Implement user-defined handling of combining ascent and descent path content.

- Implement storing data directly in prefixes, option 2.

- `hasPrecedingState`/`hasSucceedingState` flag on range cursors (including sets)

- `hasChildren` flag

- Reduce size of empty long-lived in-memory tries? (Currently at ~6k because of large reuse blocks)

- (Not necessary) Multiple children flag. Perhaps two variations:
  - `HAS_MULTIPLE_CHILDREN` only true if known, merges use set|source, don't add even if they may result in multiple
    children. 
    Cleared by intersection.
  - `HAS_AT_MOST_ONE_CHILD` only true if known. Intersections set at set|source but don't add on mismatch. 
    Cleared by merge.

  Merges clear the flag.

- TrieTombstoneMarker hashsets to reuse the same addresses in allocator memory for matching markers?

### Difficult:
- Change InMemoryRangeTrie cursor's skip not lose nearest content when skip acts as advance.

### Not:
- Row data liveness info methods
- Change TrieTombstone marker to be able to indicate row/complex-column deletion level


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

# Ideas/unedited thoughts about options

## Embrace point/branch tombstones

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

E.g. -2 for before, -1 for content, 0 for branch and metadata, 1 for after

TBH I don't quite see the value.
*Discussed in Trie.md*

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
- invalid (b^ is after bbb in iteration order; [a, bbb, b, c] is also invalid)

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


## Represent points and slices as the proper set, i.e. add explicit data in trie?

Not easy at all.

for [bb, bb] (the point at "bb" without the branch) that would be something like [bb, bb00, bbff^, bb^], i.e.

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


## Deletion-only rows

We need to be able to identify the root of a deletion-only row for iterating rows and unfiltereds. We can't rely on the
row deletion for this, because we can have a deletion-only row for column- or cell-level tombstones.

So a row needs two markers:
- RowData with liveness info on the live path, if there is liveness info or any data on the live path.
- TrieTombstoneMarker.Point on the deletion path, if there is any deletion present.

The point deletion should disappear when a covering deletion deletes all of its children; easiest way to do this is to
give it the superseding deletion time of all children deletions (this should also work with purging). This means that
the superseding deletion needs to be collected on insertion (merger/upserter can do this).

Note that this means we need to be able to have boundary + point TrieTombstoneMarker for row deletions.


# Memtable updates

## Dead (fully deleted) rows and columns and their markers

Rows have level markers on both the live and deleted side. The deleted side marker should take care of itself (by
expiration/purging or higher covering deletion) at the same time as the covering content disappears. We don't have the
same thing for the live side, and we likely can't, because cells may be deleted on a lower lever (e.g. by a column
delete).

The same problem is the for the complex column markers, which may become orphaned.

This maybe is something we can live with in the memtable.

Options:
- Let the trie mutator know that certain content needs to be removed if a branch ends up empty after a modification.
  Easy to do and efficient.
- Collect cell counts and make the trie stop on the return path.
- Something else?

## Indexer updates

Completely unknown how to implement at this point. Probably change the interfaces to let cells be passed individually.


The biggest problem is that currently we present rows to the indexer. This doesn't appear to be what indexers (at least
SAI) actually need, but this is the interface we currently have.

Some considerations:
- Merge of data happens separately from deletion and merge of deletion. There are three points where we can see row
  markers:
    - Deletion of current data
    - Merge of deletions
    - Data with data merge
- We can get all three in a single update.
- We probably only care about the two "on data" modifications, and we can send them separately with deletion first.
- Note that the update trie has existing deletions applied when we get it.

One option is to provide access to tail tries via some variation of the KeyProducer interface (possibly in the Mutator?)
but this cannot have access to the deletion/other branch. I.e. we will see each of the three types of updates of the
previous paragraph separately. This may not be what the indexer expects, but it should still work fine.

In theory, we can just as well send all individual cell updates wrapped in separate rows.

Neither of these is a good long-term solution as it creates complex intermediate objects we don't want to have.

A better option is to make a wrapper over the current indexer that takes:
- same partition-level calls as before
- startRow(liveness), deletedRow(deltime)
- startComplexColumn(), deletedComplexColumn(deltime)
- upsertCell(existing,new) where existing can be empty, deleteCell(existing,deltime)
  with trie keys (i.e. keyproducer)

and collect updates in in-memory trie-backed rows to pass on to indexer. Make that a subtype of current indexer so e.g.
SAI can implement it. Possibly POC an implementation for SAI folks to build on.

The final alternative is to disable this memtable implementation when indexer is in use.

# Non-pojo content

To be able to make the trie fully off-heap, we need to be able to store bytes directly in the trie. At the same time we
need adaptors to make the trie return pojos when needed (for stage 1-3 and SAI for now).

To make the latter efficient, we have to support storing ~30-bit ints directly as we currently do. On the other hand,
we need to be able to store payloads of at least 24 (`2 * sizeof(DeletionTime)`) bytes.

Perhaps the initial implementation could be:
- Keep the current leaf encoding when the given data is up to 30 bits (sign + return path bits reserved),
- Otherwise use new `payload` cell type to use full 32-byte cell.

Upserter will always be given and must return a pointer. If its data fits in the 30 bits, a leaf-encoded one. If not, it
should ask to be allocated space and be given a buffer to fill.

## Bytes in prefix nodes, option 1

It makes sense to try to use the free space in a prefix cell too. For example, the flag byte could encode:
- 5 bits: embedded prefix offset, 0 if prefix is not embedded
- 1 bit: main content is a pointer/leaf-encoded vs suppled as bytes
- 1 bit: extra content is a pointer/leaf-encoded vs suppled as bytes
- 1 bit unused for now

This needs a length calculator supplied by the user, so that we can find the start of the second buffer if it is in
the same cell.

If any of these is a pointer, it immediately follows the encoding byte (i.e. flags + content1 + content2,
flags + content2 + bytes1 etc.). If content is not present, we encode it as a NONE pointer as before.

This is complex, so we shouldn't start with it.

## Bytes in prefix nodes, option 2

What if we instead store 2 bits for:
- 00 both are pointers
- 01 main content is a pointer, extra content is bytes
- 10 main content is bytes, extra content is a pointer

and don't permit second byte array in the cell, requiring it to be a payload pointer?
Or even fix that the only content that can be bytes is the main?

This may be good enough and simplifies things a lot.

Especially as the main use for this should be attaching metadata (aggregate min/max timestamp/localdeltime).

## Option 3: Let user combine descent and ascent content in range tries

This avoids storing the same deletion multiple times and could be:
- before/after for range tombstones
- covering, point, branch for row tombstones
- branch only for partition and complex column tombstones

Combine with option 2 (main can be bytes, extra is pointer) for deletion-aware trie.

## When we need to store more than 32 bytes for memtables

This may be fully external to the trie code and should use and reference the allocator. Maybe use a bit to also allow
an allocator address to be given in leaf encoding.

We could just as well start with this...




## Sizes

Skip list
```
Memtable in unslabbed_heap_buffers mode: 215000 ops, 10.743MiB serialized bytes, 81.348MiB (4%) on-heap, 0B (0%) off-heap
Memtable in heap_buffers mode: 215000 ops, 10.743MiB serialized bytes, 75.169MiB (4%) on-heap, 0B (0%) off-heap
Memtable in offheap_buffers mode: 215000 ops, 10.743MiB serialized bytes, 78.754MiB (4%) on-heap, 3.586MiB (0%) off-heap
Memtable in offheap_objects mode: 215000 ops, 10.743MiB serialized bytes, 42.386MiB (2%) on-heap, 9.208MiB (0%) off-heap

Memtable in offheap_objects mode: 4150000 ops, 208.521MiB serialized bytes, 675.011MiB (34%) on-heap, 176.001MiB (9%) off-heap
55.7s
```

Stage 1
```
Memtable in unslabbed_heap_buffers mode: 215000 ops, 10.247MiB serialized bytes, 73.831MiB (4%) on-heap, 0B (0%) off-heap
Memtable in heap_buffers mode: 215000 ops, 10.247MiB serialized bytes, 67.651MiB (3%) on-heap, 0B (0%) off-heap
Memtable in offheap_buffers mode: 215000 ops, 10.247MiB serialized bytes, 67.928MiB (3%) on-heap, 5.902MiB (0%) off-heap
Memtable in offheap_objects mode: 215000 ops, 10.247MiB serialized bytes, 35.465MiB (2%) on-heap, 11.276MiB (1%) off-heap

Memtable in offheap_objects mode: 4150000 ops, 203.562MiB serialized bytes, 605.671MiB (31%) on-heap, 214.907MiB (11%) off-heap
46.7s
```

Stage 2
```
Memtable in unslabbed_heap_buffers mode: 215000 ops, 8.488MiB serialized bytes, 53.854MiB (3%) on-heap, 0B (0%) off-heap
Memtable in heap_buffers mode: 215000 ops, 8.488MiB serialized bytes, 50.802MiB (3%) on-heap, 0B (0%) off-heap
Memtable in offheap_buffers mode: 215000 ops, 8.488MiB serialized bytes, 38.691MiB (2%) on-heap, 15.163MiB (1%) off-heap
Memtable in offheap_objects mode: 215000 ops, 8.488MiB serialized bytes, 23.432MiB (1%) on-heap, 19.169MiB (1%) off-heap

Memtable in offheap_objects mode: 4150000 ops, 168.800MiB serialized bytes, 439.544MiB (22%) on-heap, 355.132MiB (18%) off-heap
44.2s
```

Stage 3
```
Memtable in unslabbed_heap_buffers mode: 200000 ops, 6.104MiB serialized bytes, 53.097MiB (3%) on-heap, 0B (0%) off-heap
Memtable in heap_buffers mode: 200000 ops, 6.104MiB serialized bytes, 50.045MiB (3%) on-heap, 0B (0%) off-heap
Memtable in offheap_buffers mode: 200000 ops, 6.104MiB serialized bytes, 37.171MiB (2%) on-heap, 15.926MiB (1%) off-heap
Memtable in offheap_objects mode: 200000 ops, 6.104MiB serialized bytes, 21.912MiB (1%) on-heap, 19.931MiB (1%) off-heap

Memtable in offheap_objects mode: 4250000 ops, 122.070MiB serialized bytes, 409.032MiB (21%) on-heap, 362.761MiB (18%) off-heap
45.0s
```

Stage 4
```
Memtable in unslabbed_heap_buffers mode: 215000 ops, 6.142MiB serialized bytes, 51.342MiB (3%) on-heap, 0B (0%) off-heap
Memtable in heap_buffers mode: 215000 ops, 6.142MiB serialized bytes, 48.291MiB (2%) on-heap, 0B (0%) off-heap
Memtable in offheap_buffers mode: 215000 ops, 6.142MiB serialized bytes, 29.160MiB (1%) on-heap, 22.182MiB (1%) off-heap
Memtable in offheap_objects mode: 215000 ops, 6.142MiB serialized bytes, 13.901MiB (1%) on-heap, 26.188MiB (1%) off-heap
```


Fully off-heap
```
Memtable in unslabbed_heap_buffers mode: 230000 ops, 6.104MiB serialized bytes, 36.156MiB (2%) on-heap, 0B (0%) off-heap
Memtable in heap_buffers mode: 230000 ops, 6.104MiB serialized bytes, 36.156MiB (2%) on-heap, 0B (0%) off-heap
Memtable in offheap_buffers mode: 230000 ops, 6.104MiB serialized bytes, 90.750KiB (0%) on-heap, 36.068MiB (2%) off-heap
Memtable in offheap_objects mode: 230000 ops, 3.052MiB serialized bytes, 90.500KiB (0%) on-heap, 35.762MiB (2%) off-heap

Memtable in offheap_objects mode: 4300000 ops, 61.035MiB serialized bytes, 92.500KiB (0%) on-heap, 648.863MiB (33%) off-heap
39.2s
```



```
Skip list: Memtable in offheap_objects mode: 675.011MiB (34%) on-heap, 176.001MiB (9%) off-heap
Stage 1:   Memtable in offheap_objects mode: 605.671MiB (31%) on-heap, 214.907MiB (11%) off-heap
Stage 2:   Memtable in offheap_objects mode: 439.544MiB (22%) on-heap, 355.132MiB (18%) off-heap
Stage 3:   Memtable in offheap_objects mode: 409.032MiB (21%) on-heap, 362.761MiB (18%) off-heap
Stage 4:   Memtable in offheap_objects mode:  92.500KiB (0%) on-heap, 648.863MiB (33%) off-heap

Skip list: Memtable in offheap_objects mode: 42.386MiB (2%) on-heap, 9.208MiB (0%) off-heap
Stage 1:   Memtable in offheap_objects mode: 35.465MiB (2%) on-heap, 11.276MiB (1%) off-heap
Stage 2:   Memtable in offheap_objects mode: 23.432MiB (1%) on-heap, 19.169MiB (1%) off-heap
Stage 3:   Memtable in offheap_objects mode: 21.912MiB (1%) on-heap, 19.931MiB (1%) off-heap
Stage 4:   Memtable in offheap_objects mode: 13.901MiB (1%) on-heap, 26.188MiB (1%) off-heap
Stage 5:   Memtable in offheap_objects mode: 90.500KiB (0%) on-heap, 36.068MiB (2%) off-heap
```





```
Benchmark                                (BATCH)  (count)  (deletionPattern)  (deletionSpec)  (deletionsRatio)  (flush)     (memtableClass)  (partitions)  (threadCount)  (useNet)  Mode  Cnt   Score   Error  Units
ReadTestWidePartitions.readGreaterMatch     1000  1000000             RANDOM           EQUAL                 0    INMEM        TrieMemtable          1000              1     false  avgt   10   9.752 ± 0.150  ms/op
... done in 10.485 s.
TrieMemtable in offheap_objects mode: 1000000 ops, 15.259MiB serialized bytes, 485.375KiB (0%) on-heap, 139.615MiB (7%) off-heap
ReadTestWidePartitions.readGreaterMatch     1000  1000000             RANDOM           EQUAL                 0    INMEM        TrieMemtable             4              1     false  avgt   10  11.020 ± 1.305  ms/op
... done in 10.733 s.
TrieMemtable in offheap_objects mode: 1000000 ops, 15.259MiB serialized bytes, 22.688KiB (0%) on-heap, 139.784MiB (7%) off-heap

ReadTestWidePartitions.readGreaterMatch     1000  1000000             RANDOM           EQUAL                 0    INMEM  TrieMemtableStage2          1000              1     false  avgt   10   9.226 ± 0.856  ms/op
... done in 8.810 s.
TrieMemtableStage2 in offheap_objects mode: 1000000 ops, 41.962MiB serialized bytes, 88.229MiB (4%) on-heap, 75.688MiB (4%) off-heap
ReadTestWidePartitions.readGreaterMatch     1000  1000000             RANDOM           EQUAL                 0    INMEM  TrieMemtableStage2             4              1     false  avgt   10   8.984 ± 0.931  ms/op
... done in 8.755 s.
TrieMemtableStage2 in offheap_objects mode: 1000000 ops, 41.962MiB serialized bytes, 87.760MiB (4%) on-heap, 75.887MiB (4%) off-heap

ReadTestWidePartitions.readGreaterMatch     1000  1000000             RANDOM           EQUAL                 0    INMEM  TrieMemtableStage1          1000              1     false  avgt   10   9.331 ± 0.790  ms/op
... done in 9.859 s.
TrieMemtableStage1 in offheap_objects mode: 1000000 ops, 50.545MiB serialized bytes, 116.019MiB (6%) on-heap, 42.013MiB (2%) off-heap
ReadTestWidePartitions.readGreaterMatch     1000  1000000             RANDOM           EQUAL                 0    INMEM  TrieMemtableStage1             4              1     false  avgt   10   9.494 ± 0.528  ms/op
... done in 9.542 s.
TrieMemtableStage1 in offheap_objects mode: 1000000 ops, 50.545MiB serialized bytes, 115.888MiB (6%) on-heap, 41.962MiB (2%) off-heap

ReadTestWidePartitions.readGreaterMatch     1000  1000000             RANDOM           EQUAL                 0    INMEM    SkipListMemtable          1000              1     false  avgt   10   9.222 ± 0.238  ms/op
... done in 6.290 s.
SkipListMemtable in offheap_objects mode: 1000000 ops, 50.552MiB serialized bytes, 115.704MiB (6%) on-heap, 41.973MiB (2%) off-heap
ReadTestWidePartitions.readGreaterMatch     1000  1000000             RANDOM           EQUAL                 0    INMEM    SkipListMemtable             4              1     false  avgt   10  10.191 ± 0.831  ms/op
... done in 8.772 s.
SkipListMemtable in offheap_objects mode: 1000000 ops, 50.545MiB serialized bytes, 115.870MiB (6%) on-heap, 41.962MiB (2%) off-heap




Benchmark                                (BATCH)  (count)  (deletionPattern)  (deletionSpec)  (deletionsRatio)  (flush)  (memtableClass)  (partitions)  (threadCount)  (useNet)  Mode  Cnt   Score   Error  Units
ReadTestWidePartitions.readGreaterMatch     1000  1000000         FROM_START           EQUAL             0.997    INMEM     TrieMemtable             3              1     false  avgt   10  17.937 ± 0.047  ms/op
... done in 20.109 s.
TrieMemtable in offheap_objects mode: 2994000 ops, 7.652MiB serialized bytes, 17.016KiB (0%) on-heap, 135.356MiB (7%) off-heap


... done in 14.132 s.
TrieMemtableStage2 in offheap_objects mode: 1997000 ops, 19.142MiB serialized bytes, 64.935MiB (3%) on-heap, 71.459MiB (4%) off-heap
... done in 15.255 s.
TrieMemtableStage1 in offheap_objects mode: 1997000 ops, 27.725MiB serialized bytes, 93.064MiB (5%) on-heap, 41.962MiB (2%) off-heap


Benchmark                                 (BATCH)  (count)  (deletionPattern)  (deletionSpec)  (deletionsRatio)  (flush)     (memtableClass)  (threadCount)  (useNet)  Mode  Cnt  Score   Error  Units
ReadTestSmallPartitions.readRandomInside     1000  1000000             RANDOM           EQUAL                 0    INMEM        TrieMemtable              1     false  avgt   10  9.313 ± 1.177  ms/op
... done in 9.915 s.
TrieMemtable in offheap_objects mode: 1000000 ops, 15.259MiB serialized bytes, 488.125KiB (0%) on-heap, 218.252MiB (11%) off-heap
ReadTestSmallPartitions.readRandomInside     1000  1000000             RANDOM           EQUAL                 0    INMEM  TrieMemtableStage2              1     false  avgt   10  7.634 ± 0.706  ms/op
... done in 8.409 s.
TrieMemtableStage2 in offheap_objects mode: 1000000 ops, 41.962MiB serialized bytes, 114.912MiB (6%) on-heap, 123.838MiB (6%) off-heap
ReadTestSmallPartitions.readRandomInside     1000  1000000             RANDOM           EQUAL                 0    INMEM  TrieMemtableStage1              1     false  avgt   10  6.915 ± 0.485  ms/op
... done in 6.637 s.
TrieMemtableStage1 in offheap_objects mode: 1000000 ops, 50.545MiB serialized bytes, 240.791MiB (12%) on-heap, 109.629MiB (6%) off-heap
ReadTestSmallPartitions.readRandomInside     1000  1000000             RANDOM           EQUAL                 0    INMEM    SkipListMemtable              1     false  avgt   10  8.946 ± 0.245  ms/op
... done in 6.252 s.
SkipListMemtable in offheap_objects mode: 1000000 ops, 58.174MiB serialized bytes, 339.508MiB (17%) on-heap, 53.406MiB (3%) off-heap

```


