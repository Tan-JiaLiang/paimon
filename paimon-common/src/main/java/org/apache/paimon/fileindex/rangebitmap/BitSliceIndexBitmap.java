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

package org.apache.paimon.fileindex.rangebitmap;

import org.apache.paimon.fs.SeekableInputStream;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BitSliceIndexBitmap {

    public static final byte VERSION_1 = 1;
    public static final byte CURRENT_VERSION = VERSION_1;

    private final byte version;
    private long min;
    private long max;
    private int[] offsets;

    private ByteBuffer body;
    private RoaringBitmap ebm;
    private RoaringBitmap[] slices;

    private int baseOffset;
    private int length;
    private SeekableInputStream stream;

    public BitSliceIndexBitmap(long min, long max) {
        this.version = CURRENT_VERSION;
        this.min = min;
        this.max = max;
        this.ebm = new RoaringBitmap();
        this.slices =
                max == Long.MIN_VALUE
                        ? new RoaringBitmap[] {}
                        : new RoaringBitmap[64 - Long.numberOfLeadingZeros(max)];
        for (int i = 0; i < bitCount(); i++) {
            slices[i] = new RoaringBitmap();
        }
    }

    public BitSliceIndexBitmap(ByteBuffer buffer) {
        version = buffer.get();
        if (version > CURRENT_VERSION) {
            throw new RuntimeException(
                    String.format(
                            "deserialize bsi index fail, " + "current version is lower than %d",
                            version));
        }

        // header size
        int headerSerializedSizeInBytes = buffer.getInt();

        // deserialize min & max
        min = buffer.getLong();
        max = buffer.getLong();

        // read offsets
        byte length = buffer.get();
        offsets = new int[length];
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = buffer.getInt();
        }

        // byte buffer
        body = buffer.slice();
        slices = new RoaringBitmap[offsets.length];
    }

    public BitSliceIndexBitmap(
            byte version,
            long min,
            long max,
            int[] offsets,
            SeekableInputStream stream,
            int baseOffset,
            int length) {
        this.version = version;
        this.min = min;
        this.max = max;
        this.offsets = offsets;
        this.slices = new RoaringBitmap[offsets.length];
        this.stream = stream;
        this.baseOffset = baseOffset;
        this.length = length;
    }

    public void set(int key, long value) {
        if (value < 0) {
            throw new UnsupportedOperationException("value can not be negative");
        }

        if (min < 0 || min > value) {
            min = value;
        }

        if (max < 0 || max < value) {
            max = value;

            // grow the slices
            int capacity = Long.toBinaryString(max).length();
            int old = bitCount();
            if (old < capacity) {
                RoaringBitmap[] newSlices = new RoaringBitmap[capacity];
                for (int i = 0; i < capacity; i++) {
                    if (i < old) {
                        newSlices[i] = slices[i];
                    } else {
                        newSlices[i] = new RoaringBitmap();
                    }
                }
                slices = newSlices;
            }
        }

        long bits = value;

        // only bit=1 need to set
        while (bits != 0) {
            getSlice(Long.numberOfTrailingZeros(bits)).add(key);
            bits &= (bits - 1);
        }
        getExistenceBitmap().add(key);
    }

    public Long get(int key) {
        if (!exists(key)) {
            return null;
        }
        long value = 0;
        for (int i = 0; i < bitCount(); i++) {
            if (getSlice(i).contains(key)) {
                value |= (1L << i);
            }
        }
        return value;
    }

    public boolean exists(int key) {
        return getExistenceBitmap().contains(key);
    }

    public int bitCount() {
        return slices.length;
    }
    
    public ByteBuffer serialize() {
        if (ebm.isEmpty()) {
            return ByteBuffer.allocate(0);
        }

        int header = 0;
        header += Byte.BYTES; // version
        header += Integer.BYTES; // header length
        header += Long.BYTES; // min
        header += Long.BYTES; // max
        header += Byte.BYTES; // offset size
        header += bitCount() * Integer.BYTES; // offsets

        int body = 0;
        ebm.runOptimize();
        body += ebm.serializedSizeInBytes();

        int[] offsets = new int[bitCount()];
        for (int i = 0; i < bitCount(); i++) {
            slices[i].runOptimize();
            body += slices[i].serializedSizeInBytes();
            if (i == 0) {
                offsets[i] = ebm.serializedSizeInBytes();
            } else {
                offsets[i] = offsets[i - 1] + slices[i - 1].serializedSizeInBytes();
            }
        }

        int sizeInBytes = header + body;
        ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes);

        buffer.put(CURRENT_VERSION);
        buffer.putInt(header - Byte.BYTES - Integer.BYTES);
        buffer.putLong(min);
        buffer.putLong(max);
        buffer.put((byte) bitCount());
        for (int offset : offsets) {
            buffer.putInt(offset);
        }
        ebm.serialize(buffer);
        for (RoaringBitmap slice : slices) {
            slice.serialize(buffer);
        }

        return buffer;
    }
    
    public RoaringBitmap eq(long predicate, @Nullable RoaringBitmap foundSet) {
        if (min == max && min == predicate) {
            return foundSet == null
                    ? getExistenceBitmap()
                    : RoaringBitmap.and(getExistenceBitmap(), foundSet);
        } else if (predicate < min || predicate > max) {
            return new RoaringBitmap();
        }

        RoaringBitmap state = isNotNull(foundSet).clone();
        if (state.isEmpty()) {
            return new RoaringBitmap();
        }

        for (int i = bitCount() - 1; i >= 0; i--) {
            long bit = (predicate >> i) & 1;
            if (bit == 1) {
                state.and(getSlice(i));
            } else {
                state.andNot(getSlice(i));
            }
        }
        return state;
    }
    
    public RoaringBitmap lte(long predicate, @Nullable RoaringBitmap foundSet) {
        if (predicate >= max) {
            return foundSet == null
                    ? getExistenceBitmap()
                    : RoaringBitmap.and(getExistenceBitmap(), foundSet);
        } else if (predicate < min) {
            return new RoaringBitmap();
        }

        RoaringBitmap state = isNotNull(foundSet);
        if (state.isEmpty()) {
            return new RoaringBitmap();
        }

        long start = state.first();
        long end = state.last() + 1;
        RoaringBitmap gt = gt(predicate, state);
        return RoaringBitmap.and(RoaringBitmap.flip(gt, start, end), state);
    }

    public RoaringBitmap gt(long predicate, @Nullable RoaringBitmap foundSet) {
        if (predicate < min) {
            if (foundSet == null) {
                return isNotNull();
            } else {
                return RoaringBitmap.and(foundSet, ebm);
            }
        } else if (predicate >= max) {
            return new RoaringBitmap();
        }

        RoaringBitmap fixedFoundSet = isNotNull(foundSet);
        if (fixedFoundSet.isEmpty()) {
            return new RoaringBitmap();
        }

        // the state is always start from the empty bitmap
        RoaringBitmap state = null;

        // if there is a run of k set bits starting from 0, [0, k] operations can be eliminated.
        int start = Long.numberOfTrailingZeros(~predicate);
        for (int i = start; i < bitCount(); i++) {
            if (state == null) {
                state = getSlice(i).clone();
                continue;
            }

            long bit = (predicate >> i) & 1;
            if (bit == 1) {
                state.and(getSlice(i));
            } else {
                state.or(getSlice(i));
            }
        }

        if (state == null) {
            return new RoaringBitmap();
        }

        state.and(fixedFoundSet);
        return state;
    }

    
    public RoaringBitmap isNotNull(@Nullable RoaringBitmap foundSet) {
        return foundSet == null
                ? getExistenceBitmap()
                : RoaringBitmap.and(getExistenceBitmap(), foundSet);
    }

    
    public RoaringBitmap topK(int k, @Nullable RoaringBitmap foundSet) {
        if (k == 0 || (foundSet != null && foundSet.isEmpty())) {
            return new RoaringBitmap();
        }

        if (k < 0) {
            throw new IllegalArgumentException(
                    "the k param can not be negative in bottomK, k=" + k);
        }

        RoaringBitmap g = new RoaringBitmap();
        RoaringBitmap e = isNotNull(foundSet);
        if (e.getCardinality() <= k) {
            return e;
        }

        for (int i = bitCount() - 1; i >= 0; i--) {
            RoaringBitmap x = RoaringBitmap.or(g, RoaringBitmap.and(e, getSlice(i)));
            long n = x.getLongCardinality();
            if (n > k) {
                e = RoaringBitmap.and(e, getSlice(i));
            } else if (n < k) {
                g = x;
                e = RoaringBitmap.andNot(e, getSlice(i));
            } else {
                e = RoaringBitmap.and(e, getSlice(i));
                break;
            }
        }

        // only k results should be returned
        RoaringBitmap f = RoaringBitmap.or(g, e);
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

    
    public RoaringBitmap bottomK(int k, @Nullable RoaringBitmap foundSet) {
        if (k == 0 || (foundSet != null && foundSet.isEmpty())) {
            return new RoaringBitmap();
        }

        if (k < 0) {
            throw new IllegalArgumentException("the k param can not be negative in topK, k=" + k);
        }

        RoaringBitmap g = new RoaringBitmap();
        RoaringBitmap e = isNotNull(foundSet);
        if (e.getCardinality() <= k) {
            return e;
        }

        for (int i = bitCount() - 1; i >= 0; i--) {
            RoaringBitmap x = RoaringBitmap.or(g, RoaringBitmap.andNot(e, getSlice(i)));
            long n = x.getCardinality();
            if (n > k) {
                e = RoaringBitmap.andNot(e, getSlice(i));
            } else if (n < k) {
                g = x;
                e = RoaringBitmap.and(e, getSlice(i));
            } else {
                e = RoaringBitmap.andNot(e, getSlice(i));
                break;
            }
        }

        // only k results should be returned
        RoaringBitmap f = RoaringBitmap.or(g, e);
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

    RoaringBitmap lt(long predicate, @Nullable RoaringBitmap foundSet) {
        return lte(predicate - 1, foundSet);
    }

    RoaringBitmap gte(long predicate, @Nullable RoaringBitmap foundSet) {
        return gt(predicate - 1, foundSet);
    }

    RoaringBitmap lte(long predicate) {
        return lte(predicate, null);
    }

    RoaringBitmap gte(long predicate) {
        return gte(predicate, null);
    }

    RoaringBitmap eq(long predicate) {
        return eq(predicate, null);
    }

    RoaringBitmap lt(long predicate) {
        return lt(predicate, null);
    }

    RoaringBitmap gt(long predicate) {
        return gt(predicate, null);
    }

    RoaringBitmap isNotNull() {
        return isNotNull(null);
    }

    RoaringBitmap topK(int k) {
        return topK(k, null);
    }

    RoaringBitmap bottomK(int k) {
        return bottomK(k, null);
    }

    private RoaringBitmap getSlice(int index) {
        try {
            if (slices[index] == null) {
                if (stream == null) {
                    body.position(offsets[index]);
                    RoaringBitmap bitmap = new RoaringBitmap();
                    bitmap.deserialize(body);
                    slices[index] = bitmap;
                } else {
                    stream.seek(baseOffset + offsets[index]);
                    int size =
                            index == offsets.length - 1
                                    ? length - offsets[index]
                                    : offsets[index + 1] - offsets[index];
                    byte[] bytes = new byte[size];
                    stream.read(bytes);
                    RoaringBitmap bitmap = new RoaringBitmap();
                    bitmap.deserialize(ByteBuffer.wrap(bytes));
                    slices[index] = bitmap;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return slices[index];
    }

    private RoaringBitmap getExistenceBitmap() {
        try {
            if (ebm == null) {
                if (stream == null) {
                    body.position(0);
                    RoaringBitmap bitmap = new RoaringBitmap();
                    bitmap.deserialize(body);
                    ebm = bitmap;
                } else {
                    stream.seek(baseOffset);
                    byte[] bytes = new byte[offsets[0]];
                    stream.read(bytes);
                    RoaringBitmap bitmap = new RoaringBitmap();
                    bitmap.deserialize(ByteBuffer.wrap(bytes));
                    ebm = bitmap;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ebm;
    }

    public static BitSliceIndexBitmap map(SeekableInputStream stream, int offset, int length)
            throws IOException {
        stream.seek(offset);

        byte[] bytes = new byte[Byte.BYTES + Integer.BYTES];
        stream.read(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte version = buffer.get();
        if (version > CURRENT_VERSION) {
            throw new RuntimeException(
                    String.format(
                            "read index file fail, " + "your plugin version is lower than %d",
                            version));
        }
        int headerSerializedSizeInBytes = buffer.getInt();

        bytes = new byte[headerSerializedSizeInBytes];
        stream.read(bytes);
        buffer = ByteBuffer.wrap(bytes);

        // deserialize min & max
        long min = buffer.getLong();
        long max = buffer.getLong();

        // read offsets
        byte size = buffer.get();
        int[] offsets = new int[size];
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = buffer.getInt();
        }

        return new BitSliceIndexBitmap(
                version,
                min,
                max,
                offsets,
                stream,
                offset + headerSerializedSizeInBytes + Byte.BYTES + Integer.BYTES,
                length);
    }
}
