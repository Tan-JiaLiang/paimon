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

package org.apache.paimon.fileindex.aggregate;

import org.apache.paimon.predicate.FieldRef;

public abstract class FileIndexAggregatePushDownAnalyzer
        implements AggregateVisitor<Boolean> {

    @Override
    public Boolean visit(FieldRef field) {
        return false;
    }

    @Override
    public Boolean visit(CountStar func) {
        return false;
    }

    @Override
    public Boolean visit(Count func) {
        return false;
    }

    @Override
    public Boolean visit(Min func) {
        return false;
    }

    @Override
    public Boolean visit(Max func) {
        return false;
    }

    @Override
    public Boolean visit(Sum func) {
        return false;
    }
}
