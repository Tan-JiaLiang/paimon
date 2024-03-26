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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.index.GlobalDynamicBucketSink;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Builder for {@link FlinkSink}.
 */
public class FlinkSinkBuilder {

    private final FileStoreTable table;

    private DataStream<RowData> input;
    @Nullable
    private Map<String, String> overwritePartition;
    @Nullable
    private LogSinkFunction logSinkFunction;
    @Nullable
    private Integer parallelism;
    private boolean boundedInput = false;
    private boolean compactSink = false;

    public FlinkSinkBuilder(FileStoreTable table) {
        this.table = table;
    }

    public FlinkSinkBuilder withInput(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    /**
     * Whether we need to overwrite partitions.
     *
     * @param overwritePartition If we pass null, it means not overwrite. If we pass an empty map,
     *                           it means to overwrite every partition it received. If we pass a non-empty map, it means
     *                           we only overwrite the partitions match the map.
     * @return returns this.
     */
    public FlinkSinkBuilder withOverwritePartition(
            @Nullable Map<String, String> overwritePartition) {
        this.overwritePartition = overwritePartition;
        return this;
    }

    public FlinkSinkBuilder withLogSinkFunction(@Nullable LogSinkFunction logSinkFunction) {
        this.logSinkFunction = logSinkFunction;
        return this;
    }

    public FlinkSinkBuilder withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public FlinkSinkBuilder withBoundedInputStream(boolean bounded) {
        this.boundedInput = bounded;
        return this;
    }

    public FlinkSinkBuilder forCompact(boolean compactSink) {
        this.compactSink = compactSink;
        return this;
    }

    public DataStreamSink<?> build() {
        // 将Flink的RowData转成paimon的Internal Row
        DataStream<InternalRow> input = MapToInternalRow.map(this.input, table.rowType());
        // 如果开启了local-merge-buffer-size（默认不开启），那么就会先进行本地合并，这个在单条消息频繁更新特别有效，主要用来解决数据倾斜问题
        // 譬如你local-merge-buffer-size配置了64MB，那么就会在这个64MB中做deduplicate，在写出去的时候只保留最后一条
        if (table.coreOptions().localMergeEnabled() && table.schema().primaryKeys().size() > 0) {
            input =
                    input.forward()
                            .transform(
                                    "local merge",
                                    input.getType(),
                                    new LocalMergeOperator(table.schema()))
                            .setParallelism(input.getParallelism());
        }

        BucketMode bucketMode = table.bucketMode();
        switch (bucketMode) {
            case FIXED:
                // 指定了bucket，且bucket > 0（适用于AppendOnly和Changelog表）
                return buildForFixedBucket(input);
            case DYNAMIC:
                return buildDynamicBucketSink(input, false);
            case GLOBAL_DYNAMIC:
                return buildDynamicBucketSink(input, true);
            case UNAWARE:
                // bucket为-1（这种模式只适用于AppendOnly）
                return buildUnawareBucketSink(input);
            default:
                throw new UnsupportedOperationException("Unsupported bucket mode: " + bucketMode);
        }
    }

    private DataStreamSink<?> buildDynamicBucketSink(
            DataStream<InternalRow> input, boolean globalIndex) {
        checkArgument(logSinkFunction == null, "Dynamic bucket mode can not work with log system.");
        return compactSink && !globalIndex
                // todo support global index sort compact
                ? new DynamicBucketCompactSink(table, overwritePartition).build(input, parallelism)
                : globalIndex
                ? new GlobalDynamicBucketSink(table, overwritePartition)
                .build(input, parallelism)
                : new RowDynamicBucketSink(table, overwritePartition)
                .build(input, parallelism);
    }

    private DataStreamSink<?> buildForFixedBucket(DataStream<InternalRow> input) {
        // 设置分区器，将同一个bucket交给同一个并行度task（保证消费顺序）
        DataStream<InternalRow> partitioned =
                partition(
                        input,
                        new RowDataChannelComputer(table.schema(), logSinkFunction != null),
                        parallelism);
        FixedBucketSink sink = new FixedBucketSink(table, overwritePartition, logSinkFunction);
        return sink.sinkFrom(partitioned);
    }

    private DataStreamSink<?> buildUnawareBucketSink(DataStream<InternalRow> input) {
        // 都写入同一个bucket-0中，无需保证顺序
        checkArgument(
                table.primaryKeys().isEmpty(),
                "Unaware bucket mode only works with append-only table for now.");
        return new RowUnawareBucketSink(
                        table, overwritePartition, logSinkFunction, parallelism, boundedInput)
                .sinkFrom(input);
    }
}
