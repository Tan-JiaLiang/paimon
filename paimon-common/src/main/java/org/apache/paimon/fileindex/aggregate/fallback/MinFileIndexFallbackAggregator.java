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

package org.apache.paimon.fileindex.aggregate.fallback;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.fileindex.FileIndexFallbackAggregator;

public class MinFileIndexFallbackAggregator implements FileIndexFallbackAggregator {

    private final Serializer<Object> fieldSerializer;

    private Object minValue;

    public MinFileIndexFallbackAggregator(Serializer<Object> fieldSerializer) {
        this.fieldSerializer = fieldSerializer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void call(Object input) {
        if (!(input instanceof Comparable)) {
            return;
        }
        Comparable<Object> c = (Comparable<Object>) input;
        if (minValue == null || c.compareTo(minValue) < 0) {
            minValue = fieldSerializer.copy(c);
        }
    }

    @Override
    public Object get() {
        return minValue;
    }
}
