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

package org.apache.paimon.utils;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

/* This file is based on source code from the RoaringBitmap Project (http://roaringbitmap.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A bit slice index compressed bitmap. */
public class BitSliceIndexRoaringBitmap {

    public static final byte VERSION_1 = 1;

    public static final BitSliceIndexRoaringBitmap EMPTY =
            new BitSliceIndexRoaringBitmap(0, new RoaringBitmap32(), new RoaringBitmap32[] {});

    private final long min;
    private final RoaringBitmap32 ebm;
    private final RoaringBitmap32[] slices;

    private BitSliceIndexRoaringBitmap(long min, RoaringBitmap32 ebm, RoaringBitmap32[] slices) {
        this.min = min;
        this.ebm = ebm;
        this.slices = slices;
    }

    public RoaringBitmap32 eq(long predicate) {
        return oNeilCompare(Operation.EQ, predicate - min, null);
    }

    public RoaringBitmap32 lt(long predicate) {
        return oNeilCompare(Operation.LT, predicate - min, null);
    }

    public RoaringBitmap32 lte(long predicate) {
        return oNeilCompare(Operation.LTE, predicate - min, null);
    }

    public RoaringBitmap32 gt(long predicate) {
        return oNeilCompare(Operation.GT, predicate - min, null);
    }

    public RoaringBitmap32 gte(long predicate) {
        return oNeilCompare(Operation.GTE, predicate - min, null);
    }

    public RoaringBitmap32 isNotNull() {
        return ebm.clone();
    }

    public Long min(RoaringBitmap32 foundSet) {
        if (ebm.isEmpty()) {
            return null;
        }
        RoaringBitmap32 bitmap = bottomK(foundSet, 1);
        if (bitmap.isEmpty()) {
            return null;
        }
        return valueAt(bitmap.first());
    }

    public Long min() {
        if (ebm.isEmpty()) {
            return null;
        }
        return min;
    }

    public Long max(RoaringBitmap32 foundSet) {
        if (ebm.isEmpty()) {
            return null;
        }
        RoaringBitmap32 bitmap = topK(foundSet, 1);
        if (bitmap.isEmpty()) {
            return null;
        }
        return valueAt(bitmap.first());
    }

    public Long max() {
        return max(ebm);
    }

    public Long valueAt(int rid) {
        if (!ebm.contains(rid)) {
            return null;
        }
        long value = 0;
        for (int i = 0; i < slices.length; i++) {
            if (slices[i].contains(rid)) {
                value |= (1L << i);
            }
        }
        return value + min;
    }

    /**
     * Base the O'Neil bit-sliced index sum algorithm.
     *
     * <p>See <a href="https://dl.acm.org/doi/10.1145/253262.253268">Improved query performance with
     * variant indexes</a>
     */
    public Long sum(RoaringBitmap32 foundSet) {
        if (foundSet == null || foundSet.isEmpty()) {
            return null;
        }

        return IntStream.range(0, slices.length)
                        .mapToLong(
                                x ->
                                        (1L << x)
                                                * RoaringBitmap32.andCardinality(
                                                        slices[x], foundSet))
                        .sum()
                + (min * RoaringBitmap32.andCardinality(ebm, foundSet));
    }

    public Long sum() {
        return sum(ebm);
    }

    /**
     * Base the O'Neil bit-sliced index TopK algorithm.
     *
     * <p>See <a href="https://dl.acm.org/doi/10.1145/376284.375669">Bit-Sliced Index Arithmetic</a>
     */
    public RoaringBitmap32 topK(RoaringBitmap32 foundSet, int k) {
        if (foundSet == null || foundSet.isEmpty() || k == 0) {
            return new RoaringBitmap32();
        }

        if (k < 0) {
            throw new IllegalArgumentException("the k param can not be negative in topK, k=" + k);
        }

        RoaringBitmap32 g = new RoaringBitmap32();
        RoaringBitmap32 e = RoaringBitmap32.and(ebm, foundSet);

        for (int i = slices.length - 1; i >= 0; i--) {
            RoaringBitmap32 x = RoaringBitmap32.or(g, RoaringBitmap32.and(e, slices[i]));
            long n = x.getCardinality();
            if (n > k) {
                e = RoaringBitmap32.and(e, slices[i]);
            } else if (n < k) {
                g = x;
                e = RoaringBitmap32.andNot(e, slices[i]);
            } else {
                e = RoaringBitmap32.and(e, slices[i]);
                break;
            }
        }

        // only k results should be returned
        RoaringBitmap32 f = RoaringBitmap32.or(g, e);
        long n = f.getCardinality() - k;
        if (n > 0) {
            PeekableIntIterator iterator = e.getIntIterator();
            while (iterator.hasNext() && n > 0) {
                f.remove(iterator.next());
                n--;
            }
        }
        return f;
    }

    public RoaringBitmap32 topK(int k) {
        return topK(ebm, k);
    }

    public RoaringBitmap32 bottomK(RoaringBitmap32 foundSet, int k) {
        if (foundSet == null || foundSet.isEmpty() || k == 0) {
            return new RoaringBitmap32();
        }

        if (k < 0) {
            throw new IllegalArgumentException(
                    "the k param can not be negative in bottomK, k=" + k);
        }

        RoaringBitmap32 g = new RoaringBitmap32();
        RoaringBitmap32 e = RoaringBitmap32.and(ebm, foundSet);

        for (int i = slices.length - 1; i >= 0; i--) {
            RoaringBitmap32 x = RoaringBitmap32.or(g, RoaringBitmap32.andNot(e, slices[i]));
            long n = x.getCardinality();
            if (n > k) {
                e = RoaringBitmap32.andNot(e, slices[i]);
            } else if (n < k) {
                g = x;
                e = RoaringBitmap32.and(e, slices[i]);
            } else {
                e = RoaringBitmap32.andNot(e, slices[i]);
                break;
            }
        }

        // only k results should be returned
        RoaringBitmap32 f = RoaringBitmap32.or(g, e);
        long n = f.getCardinality() - k;
        if (n > 0) {
            IntIterator iterator = e.getIntIterator();
            while (iterator.hasNext() && n > 0) {
                f.remove(iterator.next());
                n--;
            }
        }
        return f;
    }

    public RoaringBitmap32 bottomK(int k) {
        return bottomK(ebm, k);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BitSliceIndexRoaringBitmap that = (BitSliceIndexRoaringBitmap) o;
        return min == that.min
                && Objects.equals(ebm, that.ebm)
                && Arrays.equals(slices, that.slices);
    }

    /**
     * O'Neil bit-sliced index compare algorithm.
     *
     * <p>See <a href="https://dl.acm.org/doi/10.1145/253262.253268">Improved query performance with
     * variant indexes</a>
     *
     * @param operation compare operation
     * @param predicate the value we found filter
     * @param foundSet rid set we want compare, using RoaringBitmap to express
     * @return rid set we found in this bsi with giving conditions, using RoaringBitmap to express
     */
    private RoaringBitmap32 oNeilCompare(
            Operation operation, long predicate, RoaringBitmap32 foundSet) {
        RoaringBitmap32 fixedFoundSet = foundSet == null ? ebm : foundSet;
        RoaringBitmap32 gt = new RoaringBitmap32();
        RoaringBitmap32 lt = new RoaringBitmap32();
        RoaringBitmap32 eq = ebm;

        for (int i = slices.length - 1; i >= 0; i--) {
            long bit = (predicate >> i) & 1;
            if (bit == 1) {
                lt = RoaringBitmap32.or(lt, RoaringBitmap32.andNot(eq, slices[i]));
                eq = RoaringBitmap32.and(eq, slices[i]);
            } else {
                gt = RoaringBitmap32.or(gt, RoaringBitmap32.and(eq, slices[i]));
                eq = RoaringBitmap32.andNot(eq, slices[i]);
            }
        }

        eq = RoaringBitmap32.and(fixedFoundSet, eq);
        switch (operation) {
            case EQ:
                return eq;
            case NEQ:
                return RoaringBitmap32.andNot(fixedFoundSet, eq);
            case GT:
                return RoaringBitmap32.and(gt, fixedFoundSet);
            case LT:
                return RoaringBitmap32.and(lt, fixedFoundSet);
            case LTE:
                return RoaringBitmap32.and(RoaringBitmap32.or(lt, eq), fixedFoundSet);
            case GTE:
                return RoaringBitmap32.and(RoaringBitmap32.or(gt, eq), fixedFoundSet);
            default:
                throw new IllegalArgumentException("not support operation: " + operation);
        }
    }

    /** Specifies O'Neil compare algorithm operation. */
    private enum Operation {
        EQ,
        NEQ,
        LTE,
        LT,
        GTE,
        GT
    }

    public static BitSliceIndexRoaringBitmap map(DataInput in) throws IOException {
        int version = in.readByte();
        if (version > VERSION_1) {
            throw new RuntimeException(
                    String.format(
                            "deserialize bsi index fail, " + "your plugin version is lower than %d",
                            version));
        }

        // deserialize min
        long min = in.readLong();

        // deserialize ebm
        RoaringBitmap32 ebm = new RoaringBitmap32();
        ebm.deserialize(in);

        // deserialize slices
        RoaringBitmap32[] slices = new RoaringBitmap32[in.readInt()];
        for (int i = 0; i < slices.length; i++) {
            RoaringBitmap32 rb = new RoaringBitmap32();
            rb.deserialize(in);
            slices[i] = rb;
        }

        return new BitSliceIndexRoaringBitmap(min, ebm, slices);
    }

    /** A Builder for {@link BitSliceIndexRoaringBitmap}. */
    public static class Appender {
        private final long min;
        private final long max;
        private final RoaringBitmap32 ebm;
        private final RoaringBitmap32[] slices;

        public Appender(long min, long max) {
            if (min < 0) {
                throw new IllegalArgumentException("values should be non-negative");
            }
            if (min > max) {
                throw new IllegalArgumentException("min should be less than max");
            }

            this.min = min;
            this.max = max;
            this.ebm = new RoaringBitmap32();
            this.slices = new RoaringBitmap32[64 - Long.numberOfLeadingZeros(max - min)];
            for (int i = 0; i < slices.length; i++) {
                slices[i] = new RoaringBitmap32();
            }
        }

        public void append(int rid, long value) {
            if (value > max) {
                throw new IllegalArgumentException(String.format("value %s is too large", value));
            }

            if (ebm.contains(rid)) {
                throw new IllegalArgumentException(String.format("rid=%s is already exists", rid));
            }

            // reduce the number of slices
            value = value - min;

            // only bit=1 need to set
            while (value != 0) {
                slices[Long.numberOfTrailingZeros(value)].add(rid);
                value &= (value - 1);
            }
            ebm.add(rid);
        }

        public boolean isNotEmpty() {
            return !ebm.isEmpty();
        }

        public void serialize(DataOutput out) throws IOException {
            out.writeByte(VERSION_1);
            out.writeLong(min);
            ebm.serialize(out);
            out.writeInt(slices.length);
            for (RoaringBitmap32 slice : slices) {
                slice.serialize(out);
            }
        }

        public BitSliceIndexRoaringBitmap build() throws IOException {
            return new BitSliceIndexRoaringBitmap(min, ebm, slices);
        }
    }
}
