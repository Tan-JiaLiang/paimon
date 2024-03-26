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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestEntry.Identifier;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Metadata of a manifest file.
 * manifest文件的元数据，这个存储在manifest list里面
 * */
public class ManifestFileMeta {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileMeta.class);

    // 文件名
    private final String fileName;
    // 文件大小
    private final long fileSize;
    // 新增了多少文件
    private final long numAddedFiles;
    // 删除了多少文件
    private final long numDeletedFiles;
    // 分区的统计信息（最大值/最小值/空值）
    private final BinaryTableStats partitionStats;
    // schema的id
    private final long schemaId;

    public ManifestFileMeta(
            String fileName,
            long fileSize,
            long numAddedFiles,
            long numDeletedFiles,
            BinaryTableStats partitionStats,
            long schemaId) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.numAddedFiles = numAddedFiles;
        this.numDeletedFiles = numDeletedFiles;
        this.partitionStats = partitionStats;
        this.schemaId = schemaId;
    }

    public String fileName() {
        return fileName;
    }

    public long fileSize() {
        return fileSize;
    }

    public long numAddedFiles() {
        return numAddedFiles;
    }

    public long numDeletedFiles() {
        return numDeletedFiles;
    }

    public BinaryTableStats partitionStats() {
        return partitionStats;
    }

    public long schemaId() {
        return schemaId;
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)));
        fields.add(new DataField(1, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(2, "_NUM_ADDED_FILES", new BigIntType(false)));
        fields.add(new DataField(3, "_NUM_DELETED_FILES", new BigIntType(false)));
        fields.add(new DataField(4, "_PARTITION_STATS", FieldStatsArraySerializer.schema()));
        fields.add(new DataField(5, "_SCHEMA_ID", new BigIntType(false)));
        return new RowType(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestFileMeta)) {
            return false;
        }
        ManifestFileMeta that = (ManifestFileMeta) o;
        return Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && numAddedFiles == that.numAddedFiles
                && numDeletedFiles == that.numDeletedFiles
                && Objects.equals(partitionStats, that.partitionStats)
                && schemaId == that.schemaId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName, fileSize, numAddedFiles, numDeletedFiles, partitionStats, schemaId);
    }

    @Override
    public String toString() {
        return String.format(
                "{%s, %d, %d, %d, %s, %d}",
                fileName, fileSize, numAddedFiles, numDeletedFiles, partitionStats, schemaId);
    }

    /**
     * Merge several {@link ManifestFileMeta}s. {@link ManifestEntry}s representing first adding and
     * then deleting the same data file will cancel each other.
     *
     * <p>NOTE: This method is atomic.
     */
    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long manifestFullCompactionSize,
            RowType partitionType) {
        // these are the newly created manifest files, clean them up if exception occurs
        List<ManifestFileMeta> newMetas = new ArrayList<>();

        try {
            // 看看能否full compaction
            Optional<List<ManifestFileMeta>> fullCompacted =
                    tryFullCompaction(
                            input,
                            newMetas,
                            manifestFile,
                            suggestedMetaSize,
                            manifestFullCompactionSize,
                            partitionType);
            // 不行的话看看能不能做minor compaction
            return fullCompacted.orElseGet(
                    () ->
                            tryMinorCompaction(
                                    input,
                                    newMetas,
                                    manifestFile,
                                    suggestedMetaSize,
                                    suggestedMinMetaCount));
        } catch (Throwable e) {
            // exception occurs, clean up and rethrow
            for (ManifestFileMeta manifest : newMetas) {
                manifestFile.delete(manifest.fileName);
            }
            throw e;
        }
    }

    private static List<ManifestFileMeta> tryMinorCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newMetas,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount) {
        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> candidates = new ArrayList<>();
        long totalSize = 0;
        // merge existing small manifest files
        for (ManifestFileMeta manifest : input) {
            totalSize += manifest.fileSize;
            candidates.add(manifest);
            // manifest的total size每超过8MB就触发一次合并
            if (totalSize >= suggestedMetaSize) {
                // reach suggested file size, perform merging and produce new file
                mergeCandidates(candidates, manifestFile, result, newMetas);
                candidates.clear(); // 已经触发一次合并了，清掉避免重复
                totalSize = 0;
            }
        }

        // merge the last bit of manifests if there are too many
        // 每超过30个manifest文件，就合并
        if (candidates.size() >= suggestedMinMetaCount) {
            mergeCandidates(candidates, manifestFile, result, newMetas);
        } else {
            result.addAll(candidates);
        }
        return result;
    }

    private static void mergeCandidates(
            List<ManifestFileMeta> candidates,
            ManifestFile manifestFile,
            List<ManifestFileMeta> result,
            List<ManifestFileMeta> newMetas) {
        // 只有一个，不需要合并，直接返回
        if (candidates.size() == 1) {
            result.add(candidates.get(0));
            return;
        }

        // 有多个，合并后返回map
        // 底层其实就是将相同的Identifier去重
        Map<Identifier, ManifestEntry> map = new LinkedHashMap<>();
        ManifestEntry.mergeEntries(manifestFile, candidates, map);
        if (!map.isEmpty()) {
            List<ManifestFileMeta> merged = manifestFile.write(new ArrayList<>(map.values()));
            result.addAll(merged);
            newMetas.addAll(merged);
        }
    }

    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,
            List<ManifestFileMeta> newMetas,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType) {
        // 1. should trigger full compaction
        // 判断是否需要触发manifest的full compaction

        List<ManifestFileMeta> base = new ArrayList<>();
        int totalManifestSize = 0;
        int i = 0;
        for (; i < inputs.size(); i++) {
            ManifestFileMeta file = inputs.get(i);
            if (file.numDeletedFiles == 0 && file.fileSize >= suggestedMetaSize) {
                base.add(file);
                totalManifestSize += file.fileSize;
            } else {
                break;
            }
        }

        List<ManifestFileMeta> delta = new ArrayList<>();
        long deltaDeleteFileNum = 0;
        long totalDeltaFileSize = 0;
        for (; i < inputs.size(); i++) {
            ManifestFileMeta file = inputs.get(i);
            delta.add(file);
            totalManifestSize += file.fileSize;
            deltaDeleteFileNum += file.numDeletedFiles();
            totalDeltaFileSize += file.fileSize();
        }

        // 无需触发manifest的full compaction，默认size trigger=16MB
        if (totalDeltaFileSize < sizeTrigger) {
            return Optional.empty();
        }

        // 2. do full compaction
        // 触发full compaction

        LOG.info(
                "Start Manifest File Full Compaction, pick the number of delete file: {}, total manifest file size: {}",
                deltaDeleteFileNum,
                totalManifestSize);

        // 2.1. try to skip base files by partition filter
        // 尝试通过partition filter去过滤掉base文件

        // 将delta manifest文件合并
        Map<Identifier, ManifestEntry> deltaMerged = new LinkedHashMap<>();
        ManifestEntry.mergeEntries(manifestFile, delta, deltaMerged);

        List<ManifestFileMeta> result = new ArrayList<>();
        int j = 0;
        if (partitionType.getFieldCount() > 0) {
            Set<BinaryRow> deletePartitions = computeDeletePartitions(deltaMerged);
            Optional<Predicate> predicateOpt =
                    convertPartitionToPredicate(partitionType, deletePartitions);

            if (predicateOpt.isPresent()) {
                Predicate predicate = predicateOpt.get();
                FieldStatsArraySerializer fieldStatsArraySerializer =
                        new FieldStatsArraySerializer(partitionType);
                for (; j < base.size(); j++) {
                    // TODO: optimize this to binary search.
                    ManifestFileMeta file = base.get(j);
                    if (predicate.test(
                            file.numAddedFiles + file.numDeletedFiles,
                            file.partitionStats.fields(fieldStatsArraySerializer))) {
                        break;
                    } else {
                        result.add(file);
                    }
                }
            } else {
                // There is no DELETE Entry in Delta, Base don't need compaction
                j = base.size();
                result.addAll(base);
            }
        }

        // 2.2. try to skip base files by reading entries
        // 尝试读取ManifestEntry过滤掉base文件

        Set<Identifier> deleteEntries = new HashSet<>();
        deltaMerged.forEach(
                (k, v) -> {
                    if (v.kind() == FileKind.DELETE) {
                        deleteEntries.add(k);
                    }
                });

        Map<Identifier, ManifestEntry> fullMerged = new LinkedHashMap<>();
        for (; j < base.size(); j++) {
            ManifestFileMeta file = base.get(j);
            ManifestEntry.mergeEntries(manifestFile.read(file.fileName), fullMerged);
            boolean contains = false;
            for (Identifier identifier : deleteEntries) {
                if (fullMerged.containsKey(identifier)) {
                    contains = true;
                    break;
                }
            }
            if (contains) {
                // already read this file into fullMerged
                j++;
                break;
            } else {
                fullMerged.clear();
                result.add(file);
            }
        }

        // 2.3. merge base files
        // 合并base文件

        // base文件合并
        ManifestEntry.mergeEntries(manifestFile, base.subList(j, base.size()), fullMerged);
        // 最后delta和base合并
        ManifestEntry.mergeEntries(deltaMerged.values(), fullMerged);

        // 2.4. write new manifest files
        // 新建manifest文件

        if (!fullMerged.isEmpty()) {
            List<ManifestFileMeta> merged =
                    manifestFile.write(new ArrayList<>(fullMerged.values()));
            result.addAll(merged);
            newMetas.addAll(merged);
        }

        return Optional.of(result);
    }

    private static Set<BinaryRow> computeDeletePartitions(
            Map<Identifier, ManifestEntry> deltaMerged) {
        Set<BinaryRow> partitions = new HashSet<>();
        for (ManifestEntry manifestEntry : deltaMerged.values()) {
            if (manifestEntry.kind() == FileKind.DELETE) {
                BinaryRow partition = manifestEntry.partition();
                partitions.add(partition);
            }
        }
        return partitions;
    }

    private static Optional<Predicate> convertPartitionToPredicate(
            RowType partitionType, Set<BinaryRow> partitions) {
        Optional<Predicate> predicateOpt;
        if (!partitions.isEmpty()) {
            RowDataToObjectArrayConverter rowArrayConverter =
                    new RowDataToObjectArrayConverter(partitionType);

            List<Predicate> predicateList =
                    partitions.stream()
                            .map(rowArrayConverter::createEqualPredicate)
                            .collect(Collectors.toList());
            predicateOpt = Optional.of(PredicateBuilder.or(predicateList));
        } else {
            predicateOpt = Optional.empty();
        }
        return predicateOpt;
    }
}
