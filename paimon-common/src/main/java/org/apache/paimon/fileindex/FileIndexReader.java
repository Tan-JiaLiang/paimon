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

package org.apache.paimon.fileindex;

import org.apache.paimon.fileindex.aggregate.AggregateVisitor;
import org.apache.paimon.fileindex.aggregate.Count;
import org.apache.paimon.fileindex.aggregate.CountStar;
import org.apache.paimon.fileindex.aggregate.Max;
import org.apache.paimon.fileindex.aggregate.Min;
import org.apache.paimon.fileindex.aggregate.Sum;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;

/**
 * Read file index from serialized bytes. Return true, means we need to search this file, else means
 * needn't.
 */
public abstract class FileIndexReader
        implements FunctionVisitor<FileIndexResult>, AggregateVisitor<Optional<FileIndexAggregator>> {

    @Override
    public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitIsNull(FieldRef fieldRef) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitContains(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    @Override
    public FileIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        FileIndexResult fileIndexResult = null;
        for (Object key : literals) {
            fileIndexResult =
                    fileIndexResult == null
                            ? visitEqual(fieldRef, key)
                            : fileIndexResult.or(visitEqual(fieldRef, key));
        }
        return fileIndexResult;
    }

    @Override
    public FileIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
        FileIndexResult fileIndexResult = null;
        for (Object key : literals) {
            fileIndexResult =
                    fileIndexResult == null
                            ? visitNotEqual(fieldRef, key)
                            : fileIndexResult.or(visitNotEqual(fieldRef, key));
        }
        return fileIndexResult;
    }

    @Override
    public FileIndexResult visitAnd(List<FileIndexResult> children) {
        throw new UnsupportedOperationException("Should not invoke this");
    }

    @Override
    public FileIndexResult visitOr(List<FileIndexResult> children) {
        throw new UnsupportedOperationException("Should not invoke this");
    }

    @Override
    public Optional<FileIndexAggregator> visit(FieldRef field) {
        return Optional.empty();
    }

    @Override
    public Optional<FileIndexAggregator> visit(CountStar func) {
        return Optional.empty();
    }

    @Override
    public Optional<FileIndexAggregator> visit(Count func) {
        return Optional.empty();
    }

    @Override
    public Optional<FileIndexAggregator> visit(Min func) {
        return Optional.empty();
    }

    @Override
    public Optional<FileIndexAggregator> visit(Max func) {
        return Optional.empty();
    }

    @Override
    public Optional<FileIndexAggregator> visit(Sum func) {
        return Optional.empty();
    }
}
