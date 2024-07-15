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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Result of compaction. */
public class CompactResult {

    // 合并的文件
    private final List<DataFileMeta> before;
    // 合并后的文件
    private final List<DataFileMeta> after;
    // changelog文件
    private final List<DataFileMeta> changelog;

    public CompactResult() {
        this(Collections.emptyList(), Collections.emptyList());
    }

    public CompactResult(DataFileMeta before, DataFileMeta after) {
        this(Collections.singletonList(before), Collections.singletonList(after));
    }

    public CompactResult(List<DataFileMeta> before, List<DataFileMeta> after) {
        this(before, after, Collections.emptyList());
    }

    public CompactResult(
            List<DataFileMeta> before, List<DataFileMeta> after, List<DataFileMeta> changelog) {
        this.before = new ArrayList<>(before);
        this.after = new ArrayList<>(after);
        this.changelog = new ArrayList<>(changelog);
    }

    public List<DataFileMeta> before() {
        return before;
    }

    public List<DataFileMeta> after() {
        return after;
    }

    public List<DataFileMeta> changelog() {
        return changelog;
    }

    public void merge(CompactResult that) {
        before.addAll(that.before);
        after.addAll(that.after);
        changelog.addAll(that.changelog);
    }
}
