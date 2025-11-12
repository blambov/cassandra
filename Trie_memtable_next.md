# Trie memtable next


## Embrace point tombstones

The complexity comes from knowing what to switch to when exiting a branch. 

For example,
```
abc -> START(555)
 cc -> POINT_WITHIN(777, 555)
 de -> END(555)
```

`precedingState` works forward and back around "acc", but how about when we enter the branch? Cursor will be positioned ahead (otherwise we don't know there's substructure), and `precedingState` will be 555. How to know when to switch 777 to 555?

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

Something like concat(key, -1) and concat(key, 256) switchpoints.

### The current way

We currently do have to handle this for inclusivity at the boundaries. This relies on knowing boundaries have no children and doing some special advancing for them.

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




# TODOs

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

