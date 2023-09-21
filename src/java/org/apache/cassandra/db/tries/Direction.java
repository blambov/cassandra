/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.tries;

/**
 * Class used to specify the direction of iteration. Provides methods used to replace comparisons and values in typical
 * loops and allow code to be written without explicit direction checks.
 *
 * For example, iterating between l and r inclusive in forward direction is usually done as
 * for (int i = l; i <= r; ++i) ...
 *
 * To loop over them in the specified direction dir, the loop above would change to
 * for (int i = dir.start(l, r); dir.le(i, dir.end(l, r)); i += dir.increase) ...
 */
public enum Direction
{
    FORWARD(1)
    {
        public int start(int left, int right)
        {
            return left;
        }

        public int end(int left, int right)
        {
            return right;
        }

        public boolean lt(int left, int right)
        {
            return left < right;
        }

        public boolean le(int left, int right)
        {
            return left <= right;
        }

        public int min(int left, int right)
        {
            return Math.min(left, right);
        }

        public int max(int left, int right)
        {
            return Math.max(left, right);
        }

        public int binarySearchRange(int p) {
            if (p < 0)
                return -1 - p;
            return p;
        }

        public boolean isForward()
        {
            return true;
        }

        public Direction opposite()
        {
            return REVERSE;
        }
    },
    REVERSE(-1)
    {
        public int start(int left, int right)
        {
            return right;
        }

        public int end(int left, int right)
        {
            return left;
        }

        public boolean lt(int left, int right)
        {
            return left > right;
        }

        public boolean le(int left, int right)
        {
            return left >= right;
        }

        public int min(int left, int right)
        {
            return Math.max(left, right);
        }

        public int max(int left, int right)
        {
            return Math.min(left, right);
        }

        public int binarySearchRange(int p) {
            if (p < 0)
                return -1 - p;
            return p + 1;
        }

        public boolean isForward()
        {
            return false;
        }

        public Direction opposite()
        {
            return FORWARD;
        }
    };

    /** Value that needs to be added to advance the iteration, i.e. value corresponding to 1 */
    public final int increase;

    Direction(int increase)
    {
        this.increase = increase;
    }

    /** Returns the value to start iteration with, i.e. the bound corresponding to l for the forward direction */
    public abstract int start(int l, int r);
    /** Returns the value to end iteration with, i.e. the bound corresponding to r for the forward direction */
    public abstract int end(int l, int r);
    /** Returns the result of the operation corresponding to a<b for the forward direction */
    public abstract boolean lt(int a, int b);
    /** Returns the result of the operation corresponding to a<=b for the forward direction */
    public abstract boolean le(int a, int b);
    /** Returns the result of the operation corresponding to min(a, b) for the forward direction */
    public abstract int min(int a, int b);
    /** Returns the result of the operation corresponding to max(a, b) for the forward direction */
    public abstract int max(int a, int b);
    /**
     * Make an adjustment for a binary search result so that the index reflects the range that leads to the key in
     * the given direction, e.g. for the array [0, 5, 10]:
     * - 1 meaning range 0-5 for binary search of 3 in both directions
     * - 0 (-0) for -1 in any direction
     * - 3 (10-) for 12 in any direction
     * - 1 (0-5) for 5 forward
     * - 2 (5-10) for 5 reverse
     * - 0 (-0) for 0 forward
     * - 3 (10-) for 10 backward
     */
    public abstract int binarySearchRange(int index);

    public static Direction fromBoolean(boolean reversed)
    {
        return reversed ? REVERSE : FORWARD;
    }

    public abstract boolean isForward();

    public abstract Direction opposite();
}
