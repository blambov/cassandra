/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

/// A range marker interface used for range tries.
///
/// Range tries require information about the coverage of ranges for positions before and after any prefix of a range.
/// To make this work, they use range markers, which basically combine information about three things:
/// - Whether this is the precise boundary point, and if so, what must be reported as `content()` for that point.
/// - Whether there is a range that applies to positions to the left of this point, and what that range is.
/// - Whether there is a range that applies to positions to the right of this point, and what that range it.
interface RangeMarker<M extends RangeMarker<M>>
{
    /// Called to convert this to a reportable state. Normally, if a range marker is not a boundary point, it does not
    /// need to be reported as content, and this method will return null.
    M toContent();
    /// Returns the range that applies to the positions preceding this marker in the given iteration order.
    M precedingState(Direction direction);

    /// Returns an intersected version of this marker, which may drop parts of the marker that are not covered by the
    /// intersecting range.
    /// If `convertCoveringToReported` is true, the restriction must also turn this marker as a reportable boundary
    /// point even if it is not currently one.
    M restrict(boolean applicableBefore, boolean applicableAfter, boolean convertCoveringToReported);
}
