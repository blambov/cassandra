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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/// A byte-level pattern that selects keys of a trie, see [Trie#patternMatched(TriePattern)].
///
/// A pattern is a sequence of elements, each of which matches
///   - a literal byte,
///   - any single byte ([Builder#anyByte], `?` in [#glob] syntax),
///   - any single UTF-8 encoded character ([Builder#anyChar], `_` in SQL `LIKE` syntax), or
///   - any sequence of bytes, including the empty one ([Builder#anyString], `*` in [#glob] syntax and `%` in SQL
///     `LIKE` syntax).
///
/// A key is selected if all of it matches the pattern. [Builder#anyChar] relies on the keys being valid UTF-8.
///
/// Internally, the pattern is a sequence of positions, each accepting a class of bytes, some of which (the loops) can
/// be repeated any number of times, including zero. A UTF-8 character is matched as one byte that is not a
/// continuation byte, followed by a loop of continuation bytes (`0x80`-`0xBF`); in valid UTF-8 this is exactly one
/// character. The pattern is normalized so that no two loops are adjacent, which the matching cursors rely on.
public final class TriePattern
{
    // Element codes. Values 0-255 are literal bytes.
    /// Any single byte.
    static final int ANY_BYTE = 0x100;
    /// Any sequence of bytes (a loop accepting all bytes).
    static final int ANY_STRING = 0x101;
    /// The first byte of a UTF-8 character, i.e. any byte that is not a continuation byte.
    static final int CHAR_START = 0x102;
    /// Any number of UTF-8 continuation bytes (a loop accepting `0x80`-`0xBF`).
    static final int CHAR_CONTINUATION = 0x103;
    /// No byte; a pattern containing this element matches nothing.
    static final int NONE = 0x104;

    /// The elements of the normalized pattern.
    final int[] elements;

    /// The matcher used by [#matches], created on first use. It is never used to walk a trie, so its state history is
    /// not used and it can be shared by concurrent calls.
    private volatile PatternMatcher valueMatcher;

    private TriePattern(int[] elements)
    {
        this.elements = elements;
    }

    /// Creates a pattern from glob syntax, where `*` matches any sequence of bytes and `?` matches any single byte.
    /// All other characters are matched literally, using their UTF-8 encoding. There is no way to escape `*` or `?`.
    public static TriePattern glob(String pattern)
    {
        Builder builder = builder();
        // '*' and '?' are ASCII, they can never be part of a multi-byte UTF-8 character.
        for (byte b : pattern.getBytes(StandardCharsets.UTF_8))
        {
            if (b == '*')
                builder.anyString();
            else if (b == '?')
                builder.anyByte();
            else
                builder.literal(b);
        }
        return builder.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /// The number of positions in the pattern.
    int length()
    {
        return elements.length;
    }

    /// Whether the given value matches the pattern. Thread-safe.
    public boolean matches(ByteBuffer value)
    {
        PatternMatcher matcher = valueMatcher;
        if (matcher == null)
            valueMatcher = matcher = PatternMatcher.create(this);
        return matcher.matches(value);
    }

    /// Returns the literal bytes at the start of the pattern, which every matching key starts with.
    public byte[] literalPrefix()
    {
        int length = 0;
        while (length < elements.length && isLiteral(elements[length]))
            ++length;
        byte[] prefix = new byte[length];
        for (int i = 0; i < length; ++i)
            prefix[i] = (byte) elements[i];
        return prefix;
    }

    static boolean isLiteral(int element)
    {
        return element < 0x100;
    }

    /// Whether the element can be repeated any number of times, including zero.
    static boolean isLoop(int element)
    {
        return element == ANY_STRING || element == CHAR_CONTINUATION;
    }

    /// Whether the element accepts the given byte.
    static boolean accepts(int element, int b)
    {
        switch (element)
        {
            case ANY_BYTE:
            case ANY_STRING:
                return true;
            case CHAR_START:
                return !isContinuation(b);
            case CHAR_CONTINUATION:
                return isContinuation(b);
            default:
                return element == b; // never true for NONE
        }
    }

    private static boolean isContinuation(int b)
    {
        return (b & 0xC0) == 0x80;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof TriePattern && Arrays.equals(elements, ((TriePattern) o).elements);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(elements);
    }

    /// Returns a glob-like representation of the pattern, with `_` standing for a UTF-8 character and non-printable
    /// or wildcard-like literal bytes shown as `\xNN`.
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (int element : elements)
        {
            switch (element)
            {
                case ANY_BYTE:
                    sb.append('?');
                    break;
                case ANY_STRING:
                    sb.append('*');
                    break;
                case CHAR_START:
                    sb.append('_');
                    break;
                case CHAR_CONTINUATION:
                    break;
                case NONE:
                    sb.append("[]");
                    break;
                default:
                    if (element >= 0x20 && element < 0x7F && element != '*' && element != '?' && element != '_' && element != '\\')
                        sb.append((char) element);
                    else
                        sb.append(String.format("\\x%02x", element));
            }
        }
        return sb.toString();
    }

    /// Builds a [TriePattern] from a sequence of literals and wildcards.
    public static class Builder
    {
        /// Token for [#anyChar], expanded into [#CHAR_START] and [#CHAR_CONTINUATION] by [#build].
        private static final int ANY_CHAR = 0x110;

        private int[] tokens = new int[16];
        private int size = 0;

        private Builder()
        {
        }

        private Builder add(int token)
        {
            if (size == tokens.length)
                tokens = Arrays.copyOf(tokens, size * 2);
            tokens[size++] = token;
            return this;
        }

        /// Adds a literal byte.
        public Builder literal(byte b)
        {
            return add(b & 0xFF);
        }

        /// Adds the given bytes as literals.
        public Builder literal(byte[] bytes)
        {
            for (byte b : bytes)
                literal(b);
            return this;
        }

        /// Adds the remaining bytes of the given buffer as literals. The buffer's position is not changed.
        public Builder literal(ByteBuffer bytes)
        {
            for (int i = bytes.position(); i < bytes.limit(); ++i)
                literal(bytes.get(i));
            return this;
        }

        /// Adds the UTF-8 encoding of the given string as literals.
        public Builder literal(String s)
        {
            return literal(s.getBytes(StandardCharsets.UTF_8));
        }

        /// Adds a wildcard matching any single byte.
        public Builder anyByte()
        {
            return add(ANY_BYTE);
        }

        /// Adds a wildcard matching any single UTF-8 encoded character.
        public Builder anyChar()
        {
            return add(ANY_CHAR);
        }

        /// Adds a wildcard matching any sequence of bytes, including the empty one.
        public Builder anyString()
        {
            return add(ANY_STRING);
        }

        public TriePattern build()
        {
            // Normalize: collapse runs of any-string wildcards, and move any-byte wildcards before them (i.e. `*?`
            // becomes `?*`, which matches the same keys). Any-character wildcards can't be moved the same way: after an
            // any-byte wildcard the position may be inside a character, where `*_` and `_*` match different keys.
            int[] normalized = new int[size];
            int count = 0;
            boolean anyStringPending = false;
            for (int i = 0; i < size; ++i)
            {
                int token = tokens[i];
                if (token == ANY_STRING)
                    anyStringPending = true;
                else if (token == ANY_BYTE)
                    normalized[count++] = token;
                else
                {
                    if (anyStringPending)
                        normalized[count++] = ANY_STRING;
                    anyStringPending = false;
                    normalized[count++] = token;
                }
            }
            if (anyStringPending)
                normalized[count++] = ANY_STRING;

            // Expand characters into a start byte and a loop of continuation bytes. If the character is followed by
            // an any-string, the latter will match the continuation bytes, and having two adjacent loops is not
            // permitted. An any-string can't be followed by a loop, as it is followed by a start byte or a literal.
            int[] elements = new int[count * 2];
            int length = 0;
            for (int i = 0; i < count; ++i)
            {
                int token = normalized[i];
                if (token != ANY_CHAR)
                {
                    elements[length++] = token;
                    continue;
                }

                elements[length++] = CHAR_START;
                if (i + 1 < count && normalized[i + 1] == ANY_STRING)
                    continue;
                elements[length++] = CHAR_CONTINUATION;
                // The continuation loop must take all the continuation bytes of the character, so the next element
                // must not accept any: an any-byte becomes a single non-continuation byte, and a continuation literal
                // can never match.
                if (i + 1 < count)
                {
                    int next = normalized[i + 1];
                    if (next == ANY_BYTE)
                        normalized[i + 1] = CHAR_START;
                    else if (isLiteral(next) && isContinuation(next))
                        normalized[i + 1] = NONE;
                }
            }
            return new TriePattern(Arrays.copyOf(elements, length));
        }
    }
}
