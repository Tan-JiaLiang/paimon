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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BitSliceIndexRoaringBitmap}. */
public class BitSliceIndexRoaringBitmapTest {

    public static final int NUM_OF_ROWS = 100000;
    public static final int VALUE_BOUND = 1000;

    private List<Pair> pairs;
    private BitSliceIndexRoaringBitmap bsi;

    @BeforeEach
    public void setup() throws IOException {
        Random random = new Random();
        List<Pair> pairs = new ArrayList<>();
        long min = 0;
        long max = 0;
        for (int i = 0; i < NUM_OF_ROWS; i++) {
            if (i % 5 == 0) {
                pairs.add(new Pair(i, null));
                continue;
            }
            long next = random.nextInt(VALUE_BOUND) + 1;
            min = Math.min(min == 0 ? next : min, next);
            max = Math.max(max == 0 ? next : max, next);
            pairs.add(new Pair(i, next));
        }
        BitSliceIndexRoaringBitmap.Appender appender =
                new BitSliceIndexRoaringBitmap.Appender(min, max);
        for (Pair pair : pairs) {
            if (pair.value == null) {
                continue;
            }
            appender.append(pair.index, pair.value);
        }
        this.bsi = appender.build();
        this.pairs = Collections.unmodifiableList(pairs);
    }

    @Test
    public void testSerde() throws IOException {
        BitSliceIndexRoaringBitmap.Appender appender =
                new BitSliceIndexRoaringBitmap.Appender(0, 10);
        appender.append(0, 0);
        appender.append(1, 1);
        appender.append(2, 2);
        appender.append(10, 6);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        appender.serialize(new DataOutputStream(out));

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        assertThat(BitSliceIndexRoaringBitmap.map(new DataInputStream(in)))
                .isEqualTo(appender.build());
    }

    @Test
    public void testEQ() {
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            long predicate = random.nextInt(VALUE_BOUND);
            assertThat(bsi.eq(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> Objects.equals(x.value, predicate))
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap32::new,
                                            RoaringBitmap32::add,
                                            (x, y) -> x.or(y)));
        }
    }

    @Test
    public void testLT() {
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            long predicate = random.nextInt(VALUE_BOUND);
            assertThat(bsi.lt(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value < predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap32::new,
                                            RoaringBitmap32::add,
                                            (x, y) -> x.or(y)));
        }
    }

    @Test
    public void testLTE() {
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            long predicate = random.nextInt(VALUE_BOUND);
            assertThat(bsi.lte(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value <= predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap32::new,
                                            RoaringBitmap32::add,
                                            (x, y) -> x.or(y)));
        }
    }

    @Test
    public void testGT() {
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            long predicate = random.nextInt(VALUE_BOUND);
            assertThat(bsi.gt(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value > predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap32::new,
                                            RoaringBitmap32::add,
                                            (x, y) -> x.or(y)));
        }
    }

    @Test
    public void testGTE() {
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            long predicate = random.nextInt(VALUE_BOUND);
            assertThat(bsi.gte(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value >= predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap32::new,
                                            RoaringBitmap32::add,
                                            (x, y) -> x.or(y)));
        }
    }

    @Test
    public void testIsNotNull() {
        assertThat(bsi.isNotNull())
                .isEqualTo(
                        pairs.stream()
                                .filter(x -> x.value != null)
                                .map(x -> x.index)
                                .collect(
                                        RoaringBitmap32::new,
                                        RoaringBitmap32::add,
                                        (x, y) -> x.or(y)));
    }

    @Test
    public void testMinAndMax() {
        Random random = new Random();

        // test valueAt
        for (int i = 0; i < 100; i++) {
            Pair pair = pairs.get(random.nextInt(NUM_OF_ROWS));
            assertThat(bsi.valueAt(pair.index)).isEqualTo(pair.value);
        }

        // test without found set
        assertThat(bsi.min())
                .isEqualTo(
                        pairs.stream()
                                .map(x -> x.value)
                                .filter(Objects::nonNull)
                                .min(Comparator.comparingLong(x -> x))
                                .orElse(null));
        assertThat(bsi.max())
                .isEqualTo(
                        pairs.stream()
                                .map(x -> x.value)
                                .filter(Objects::nonNull)
                                .max(Comparator.comparingLong(x -> x))
                                .orElse(null));

        // test with found set
        for (int i = 0; i < 10; i++) {
            RoaringBitmap32 foundSet = new RoaringBitmap32();
            for (int j = 0; j < 1000; j++) {
                foundSet.add(random.nextInt(10000));
            }
            assertThat(bsi.min(foundSet))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> foundSet.contains(x.index))
                                    .map(x -> x.value)
                                    .filter(Objects::nonNull)
                                    .min(Comparator.comparingLong(x -> x))
                                    .orElse(null));
            assertThat(bsi.max(foundSet))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> foundSet.contains(x.index))
                                    .map(x -> x.value)
                                    .filter(Objects::nonNull)
                                    .max(Comparator.comparingLong(x -> x))
                                    .orElse(null));
        }
    }

    @Test
    public void testSum() {
        Random random = new Random();

        // test without found set
        assertThat(bsi.sum())
                .isEqualTo(
                        pairs.stream()
                                .map(x -> x.value)
                                .filter(Objects::nonNull)
                                .reduce(0L, Long::sum));

        // test with found set
        for (int i = 0; i < 10; i++) {
            RoaringBitmap32 foundSet = new RoaringBitmap32();
            for (int j = 0; j < 1000; j++) {
                foundSet.add(random.nextInt(NUM_OF_ROWS));
            }
            assertThat(bsi.sum(foundSet))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> foundSet.contains(x.index))
                                    .map(x -> x.value)
                                    .filter(Objects::nonNull)
                                    .reduce(0L, Long::sum));
        }
    }

    @Test
    public void testTopK() {
        Random random = new Random();

        // test without found set
        List<Integer> withoutFoundSetExpected =
                pairs.stream()
                        .filter(x -> x.value != null)
                        .sorted(
                                Comparator.comparing((Pair x) -> x.value)
                                        .thenComparingInt(x -> x.index))
                        .map(x -> x.index)
                        .collect(Collectors.toList());
        Collections.reverse(withoutFoundSetExpected);
        for (int i = 0; i < 100; i++) {
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            withoutFoundSetExpected.subList(0, i).forEach(bitmap::add);
            assertThat(bsi.topK(i)).isEqualTo(bitmap);
        }

        // test with found set
        RoaringBitmap32 foundSet = new RoaringBitmap32();
        for (int i = 0; i < 1000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        List<Integer> witFoundSetExpected =
                pairs.stream()
                        .filter(x -> x.value != null)
                        .filter(x -> foundSet.contains(x.index))
                        .sorted(
                                Comparator.comparing((Pair x) -> x.value)
                                        .thenComparingInt(x -> x.index))
                        .map(x -> x.index)
                        .collect(Collectors.toList());
        Collections.reverse(witFoundSetExpected);
        for (int i = 0; i < 100; i++) {
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            witFoundSetExpected.subList(0, i).forEach(bitmap::add);
            assertThat(bsi.topK(foundSet, i)).isEqualTo(bitmap);
        }
    }

    @Test
    public void testBottomK() {
        Random random = new Random();

        // test without found set
        List<Integer> withoutFoundSetExpected =
                pairs.stream()
                        .filter(x -> x.value != null)
                        .sorted(
                                Comparator.comparing((Pair x) -> x.value)
                                        .thenComparingInt(x -> -x.index))
                        .map(x -> x.index)
                        .collect(Collectors.toList());
        for (int i = 0; i < 100; i++) {
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            withoutFoundSetExpected.subList(0, i).forEach(bitmap::add);
            assertThat(bsi.bottomK(i)).isEqualTo(bitmap);
        }

        // test with found set
        RoaringBitmap32 foundSet = new RoaringBitmap32();
        for (int i = 0; i < 1000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        List<Integer> witFoundSetExpected =
                pairs.stream()
                        .filter(x -> x.value != null)
                        .filter(x -> foundSet.contains(x.index))
                        .sorted(
                                Comparator.comparing((Pair x) -> x.value)
                                        .thenComparingInt(x -> -x.index))
                        .map(x -> x.index)
                        .collect(Collectors.toList());
        for (int i = 0; i < 100; i++) {
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            witFoundSetExpected.subList(0, i).forEach(bitmap::add);
            assertThat(bsi.bottomK(foundSet, i)).isEqualTo(bitmap);
        }
    }

    private static class Pair {
        int index;
        Long value;

        public Pair(int index, Long value) {
            this.index = index;
            this.value = value;
        }
    }
}
