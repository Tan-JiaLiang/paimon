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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Universal Compaction Style is a compaction style, targeting the use cases requiring lower write
 * amplification, trading off read amplification and space amplification.
 *
 * <p>See RocksDb Universal-Compaction:
 * https://github.com/facebook/rocksdb/wiki/Universal-Compaction.
 */
public class UniversalCompaction implements CompactStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(UniversalCompaction.class);

    // 大小放大的定义是，在changelog mode table的合并树中存储一个字节的数据所需的额外存储量（百分比）。
    // 默认200
    private final int maxSizeAmp;
    // 比较changelog mode table的排序运行大小时的百分比灵活性。
    // 如果候选排序运行的大小比下一个排序运行的大小小1%，则将下一个排序运行纳入该候选集。
    // 默认为1
    private final int sizeRatio;
    // 触发压缩的排序运行编号。包括 0 级文件（一个文件一个排序运行）和高级别运行（一个级别一个排序运行）。
    // 默认为5
    private final int numRunCompactionTrigger;

    // 表示多久执行一次优化压缩，该配置用于确保读取优化系统表的查询及时性。
    // 默认为null
    @Nullable private final Long opCompactionInterval;
    // 上一次优化压缩时间
    @Nullable private Long lastOptimizedCompaction;

    public UniversalCompaction(int maxSizeAmp, int sizeRatio, int numRunCompactionTrigger) {
        this(maxSizeAmp, sizeRatio, numRunCompactionTrigger, null);
    }

    public UniversalCompaction(
            int maxSizeAmp,
            int sizeRatio,
            int numRunCompactionTrigger,
            @Nullable Duration opCompactionInterval) {
        this.maxSizeAmp = maxSizeAmp;
        this.sizeRatio = sizeRatio;
        this.numRunCompactionTrigger = numRunCompactionTrigger;
        this.opCompactionInterval =
                opCompactionInterval == null ? null : opCompactionInterval.toMillis();
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;

        if (opCompactionInterval != null) {
            if (lastOptimizedCompaction == null
                    || currentTimeMillis() - lastOptimizedCompaction > opCompactionInterval) {
                LOG.debug("Universal compaction due to optimized compaction interval");
                updateLastOptimizedCompaction();    // 记录本次触发优化压缩的时间
                return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
            }
        }

        // 1 checking for reducing size amplification
        // 由Space Amplification触发的合并
        CompactUnit unit = pickForSizeAmp(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size amplification");
            }
            return Optional.of(unit);
        }

        // 2 checking for size ratio
        // 由Individual Size Ratio触发的合并
        unit = pickForSizeRatio(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size ratio");
            }
            return Optional.of(unit);
        }

        // 3 checking for file num
        // 由 number of sorted runs 触发的合并
        if (runs.size() > numRunCompactionTrigger) {
            // compacting for file num
            int candidateCount = runs.size() - numRunCompactionTrigger + 1;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to file num");
            }
            return Optional.ofNullable(pickForSizeRatio(maxLevel, runs, candidateCount));
        }

        return Optional.empty();
    }

    @VisibleForTesting
    CompactUnit pickForSizeAmp(int maxLevel, List<LevelSortedRun> runs) {
        // 检测是否满足compaction触发条件（默认sorted-run为5才触发compaction）
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        // 0到（N-1）的sorted run大小
        long candidateSize =
                runs.subList(0, runs.size() - 1).stream()
                        .map(LevelSortedRun::run)
                        .mapToLong(SortedRun::totalSize)
                        .sum();

        // 最后一个sorted run大小
        long earliestRunSize = runs.get(runs.size() - 1).run().totalSize();

        // size amplification = percentage of additional size
        if (candidateSize * 100 > maxSizeAmp * earliestRunSize) {
            updateLastOptimizedCompaction();
            return CompactUnit.fromLevelRuns(maxLevel, runs);
        }

        return null;
    }

    @VisibleForTesting
    CompactUnit pickForSizeRatio(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        return pickForSizeRatio(maxLevel, runs, 1);
    }

    private CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount) {
        return pickForSizeRatio(maxLevel, runs, candidateCount, false);
    }

    public CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount, boolean forcePick) {
        // 尽量多选一些sorted run进行合并
        long candidateSize = candidateSize(runs, candidateCount);   // 总大小
        for (int i = candidateCount; i < runs.size(); i++) {
            LevelSortedRun next = runs.get(i);
            // 总大小 * (100 + ratio) / 100 小于 当前sorted run的总大小
            if (candidateSize * (100.0 + sizeRatio) / 100.0 < next.run().totalSize()) {
                break;
            }

            candidateSize += next.run().totalSize();
            candidateCount++;
        }

        if (forcePick || candidateCount > 1) {
            return createUnit(runs, maxLevel, candidateCount);
        }

        return null;
    }

    private long candidateSize(List<LevelSortedRun> runs, int candidateCount) {
        long size = 0;
        for (int i = 0; i < candidateCount; i++) {
            size += runs.get(i).run().totalSize();
        }
        return size;
    }

    @VisibleForTesting
    CompactUnit createUnit(List<LevelSortedRun> runs, int maxLevel, int runCount) {
        int outputLevel;
        if (runCount == runs.size()) {
            outputLevel = maxLevel;
        } else {
            // level of next run - 1
            outputLevel = Math.max(0, runs.get(runCount).level() - 1);
        }

        // outputLevel置为首个不为0的level
        if (outputLevel == 0) {
            // do not output level 0
            for (int i = runCount; i < runs.size(); i++) {
                LevelSortedRun next = runs.get(i);
                runCount++;
                if (next.level() != 0) {
                    outputLevel = next.level();
                    break;
                }
            }
        }

        // 这里就属于优化compact了
        if (runCount == runs.size()) {
            updateLastOptimizedCompaction();
            outputLevel = maxLevel;
        }

        return CompactUnit.fromLevelRuns(outputLevel, runs.subList(0, runCount));
    }

    private void updateLastOptimizedCompaction() {
        lastOptimizedCompaction = currentTimeMillis();
    }

    long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
