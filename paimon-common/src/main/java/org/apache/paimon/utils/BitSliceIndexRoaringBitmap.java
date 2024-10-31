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

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/* This file is based on source code from the RoaringBitmap Project (http://roaringbitmap.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A bit slice index compressed bitmap.
 */
public class BitSliceIndexRoaringBitmap {

    public static final byte VERSION_1 = 1;

    private final long min;
    private final RoaringBitmap ebm;
    private final RoaringBitmap[] slices;

    private BitSliceIndexRoaringBitmap(long min, RoaringBitmap ebm, RoaringBitmap[] slices) {
        this.min = min;
        this.ebm = ebm;
        this.slices = slices;
    }

    public RoaringBitmap32 eq(long predicate) {
        return new RoaringBitmap32(oNeilCompare(Operation.EQ, predicate - min, null));
    }

    public RoaringBitmap32 lt(long predicate) {
        return new RoaringBitmap32(oNeilCompare(Operation.LT, predicate - min, null));
    }

    public RoaringBitmap32 lte(long predicate) {
        return new RoaringBitmap32(oNeilCompare(Operation.LTE, predicate - min, null));
    }

    public RoaringBitmap32 gt(long predicate) {
        return new RoaringBitmap32(oNeilCompare(Operation.GT, predicate - min, null));
    }

    public RoaringBitmap32 gte(long predicate) {
        return new RoaringBitmap32(oNeilCompare(Operation.GTE, predicate - min, null));
    }

    public RoaringBitmap32 isNotNull() {
        return new RoaringBitmap32(ebm.clone());
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
        return min == that.min && Objects.equals(ebm, that.ebm) && Arrays.equals(slices, that.slices);
    }

    /**
     * O'Neil bit-sliced index compare algorithm.
     *
     * @param operation compare operation
     * @param predicate the value we found filter
     * @param foundSet  columnId set we want compare, using RoaringBitmap to express
     * @return columnId set we found in this bsi with giving conditions, using RoaringBitmap to
     * express
     */
    private RoaringBitmap oNeilCompare(
            Operation operation, long predicate, RoaringBitmap foundSet) {
        RoaringBitmap fixedFoundSet = foundSet == null ? ebm : foundSet;
        RoaringBitmap gt = new RoaringBitmap();
        RoaringBitmap lt = new RoaringBitmap();
        RoaringBitmap eq = ebm;

        for (int i = slices.length - 1; i >= 0; i--) {
            long bit = (predicate >> i) & 1;
            if (bit == 1) {
                lt = RoaringBitmap.or(lt, RoaringBitmap.andNot(eq, slices[i]));
                eq = RoaringBitmap.and(eq, slices[i]);
            } else {
                gt = RoaringBitmap.or(gt, RoaringBitmap.and(eq, slices[i]));
                eq = RoaringBitmap.andNot(eq, slices[i]);
            }
        }

        eq = RoaringBitmap.and(fixedFoundSet, eq);
        switch (operation) {
            case EQ:
                return eq;
            case NEQ:
                return RoaringBitmap.andNot(fixedFoundSet, eq);
            case GT:
                return RoaringBitmap.and(gt, fixedFoundSet);
            case LT:
                return RoaringBitmap.and(lt, fixedFoundSet);
            case LTE:
                return RoaringBitmap.and(RoaringBitmap.or(lt, eq), fixedFoundSet);
            case GTE:
                return RoaringBitmap.and(RoaringBitmap.or(gt, eq), fixedFoundSet);
            default:
                throw new IllegalArgumentException("not support operation: " + operation);
        }
    }

    /**
     * Specifies O'Neil compare algorithm operation.
     */
    private enum Operation {
        EQ,
        NEQ,
        LTE,
        LT,
        GTE,
        GT
    }

    public static BitSliceIndexRoaringBitmap map(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte version = buffer.get();
        if (version > VERSION_1) {
            throw new RuntimeException(
                    String.format(
                            "deserialize bsi index fail, " + "your plugin version is lower than %d",
                            version));
        }

        // deserialize min
        long min = buffer.getLong();

        // deserialize ebm
        RoaringBitmap ebm = new RoaringBitmap();
        ebm.deserialize(buffer);

        // deserialize slices
        buffer.position(buffer.position() + ebm.serializedSizeInBytes());
        RoaringBitmap[] slices = new RoaringBitmap[buffer.getInt()];
        for (int i = 0; i < slices.length; i++) {
            RoaringBitmap rb = new RoaringBitmap();
            rb.deserialize(buffer);
            slices[i] = rb;
            buffer.position(buffer.position() + rb.serializedSizeInBytes());
        }

        return new BitSliceIndexRoaringBitmap(min, ebm, slices);
    }

    /**
     * A Builder for {@link BitSliceIndexRoaringBitmap}.
     */
    public static class Builder {
        private final long min;
        private final long max;
        private final RoaringBitmap ebm;
        private final RoaringBitmap[] slices;

        public Builder(long min, long max) {
            if (min < 0) {
                throw new IllegalArgumentException("values should be non-negative");
            }
            if (min > max) {
                throw new IllegalArgumentException("min should be less than max");
            }

            this.min = min;
            this.max = max;
            this.ebm = new RoaringBitmap();
            this.slices = new RoaringBitmap[64 - Long.numberOfLeadingZeros(max - min)];
            for (int i = 0; i < slices.length; i++) {
                slices[i] = new RoaringBitmap();
            }
        }

        public void set(int rid, long value) {
            if (value > max) {
                throw new IllegalArgumentException(
                        String.format("value %s is too large", value));
            }

            if (ebm.contains(rid)) {
                throw new IllegalArgumentException(
                        String.format("rid=%s is already exists", rid));
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

        public BitSliceIndexRoaringBitmap build() throws IOException {
            return map(serialize());
        }

        public byte[] serialize() {
            // runOptimize
            ebm.runOptimize();
            for (RoaringBitmap slice : slices) {
                slice.runOptimize();
            }

            // serialize
            ByteBuffer buffer = ByteBuffer.allocate(serializedSizeInBytes());
            buffer.put(VERSION_1);
            buffer.putLong(min);
            ebm.serialize(buffer);
            buffer.putInt(slices.length);
            for (RoaringBitmap rb : slices) {
                rb.serialize(buffer);
            }
            return buffer.array();
        }

        private int serializedSizeInBytes() {
            int versionSizeInBytes = Byte.BYTES;
            int minSizeInBytes = Long.BYTES;
            int depth = Integer.BYTES;
            int ebmSizeInBytes = ebm.serializedSizeInBytes();
            int slicesSizeInBytes = 0;
            for (RoaringBitmap rb : slices) {
                slicesSizeInBytes += rb.serializedSizeInBytes();
            }
            return versionSizeInBytes + minSizeInBytes + depth + ebmSizeInBytes + slicesSizeInBytes;
        }
    }
}
