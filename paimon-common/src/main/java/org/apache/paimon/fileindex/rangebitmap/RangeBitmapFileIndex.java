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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fileindex.rangebitmap.factory.StringKeyFactory;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/** implementation of bitmap file index. */
public class RangeBitmapFileIndex implements FileIndexer {

    public static final int VERSION_1 = 1;

    public static final String VERSION = "version";
    public static final String INDEX_BLOCK_SIZE = "index-block-size";
    public static final String READ_ALL = "read-all";

    private final DataType dataType;
    private final Options options;

    public RangeBitmapFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.options = options;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType, options);
    }

    @Override
    public FileIndexReader createReader(
            SeekableInputStream seekableInputStream, int start, int length) {
        try {
            return new Reader(seekableInputStream, start, options);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class Writer extends FileIndexWriter {

        private final StringKeyFactory factory;
        private final RangeEncodeBitSliceBitmap.Appender<String> bitmap;

        public Writer(DataType dataType, Options options) {
            this.factory = new StringKeyFactory();
            this.bitmap = new RangeEncodeBitSliceBitmap.Appender<>(factory, 16 * 1024);
        }

        @Override
        public void write(Object key) {
            if (key instanceof BinaryString) {
                bitmap.append(key.toString());
            } else if (key instanceof String) {
                bitmap.append((String) key);
            }
        }

        @Override
        public byte[] serializedBytes() {
            return bitmap.serialize();
        }
    }

    private static class Reader extends FileIndexReader {

        private RangeEncodeBitSliceBitmap<String> bitmap;

        public Reader(SeekableInputStream seekableInputStream, int start, Options options) {
            try {
                boolean readAll = Boolean.parseBoolean(options.get(READ_ALL));
                this.bitmap = RangeEncodeBitSliceBitmap.map(seekableInputStream, start, new StringKeyFactory(), readAll);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
            return visitIn(fieldRef, Collections.singletonList(literal));
        }

        @Override
        public FileIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
            return visitNotIn(fieldRef, Collections.singletonList(literal));
        }

        @Override
        public FileIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(() -> getInListResultBitmap(literals));
        }

        @Override
        public FileIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () -> {
                        RoaringBitmap32 bitmap = getInListResultBitmap(literals);
                        bitmap.flip(0, bitmap.getCardinality());
                        return bitmap;
                    });
        }

        @Override
        public FileIndexResult visitIsNull(FieldRef fieldRef) {
            return new BitmapIndexResult(() -> new RoaringBitmap32(bitmap.isNull()));
        }

        @Override
        public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
            return new BitmapIndexResult(() -> new RoaringBitmap32(bitmap.isNotNull()));
        }

        private RoaringBitmap32 getInListResultBitmap(List<Object> literals) {
            return RoaringBitmap32.or(
                    literals.stream()
                            .map(
                                    it -> new RoaringBitmap32(bitmap.eq(it.toString())))
                            .iterator());
        }
    }
}
