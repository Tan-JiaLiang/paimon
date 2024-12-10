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

import org.apache.paimon.types.DataField;

public class Max implements AggregateFunction {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "MAX";

    private final DataField field;

    public Max(DataField field) {
        this.field = field;
    }

    public DataField field() {
        return field;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public <T> T visit(AggregateVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
