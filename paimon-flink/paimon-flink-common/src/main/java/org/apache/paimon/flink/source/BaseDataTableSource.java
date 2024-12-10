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

package org.apache.paimon.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.fileindex.aggregate.AggregateFunction;
import org.apache.paimon.fileindex.aggregate.FileIndexAggregatePushDownVisitor;
import org.apache.paimon.flink.FlinkConnectorOptions.WatermarkEmitStrategy;
import org.apache.paimon.flink.PaimonDataStreamScanProvider;
import org.apache.paimon.flink.log.LogSourceProvider;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.flink.lookup.FileStoreLookupFunction;
import org.apache.paimon.flink.lookup.LookupRuntimeProviderFactory;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.Projection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_ASYNC;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_ASYNC_THREAD_NUMBER;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_REMOVE_NORMALIZE;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_GROUP;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_EMIT_STRATEGY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_IDLE_TIMEOUT;

/**
 * Table source to create {@link StaticFileStoreSource} or {@link ContinuousFileStoreSource} under
 * batch mode or streaming mode.
 */
public abstract class BaseDataTableSource extends FlinkTableSource
        implements LookupTableSource, SupportsWatermarkPushDown, SupportsAggregatePushDown {

    private static final List<ConfigOption<?>> TIME_TRAVEL_OPTIONS =
            Arrays.asList(
                    CoreOptions.SCAN_TIMESTAMP,
                    CoreOptions.SCAN_TIMESTAMP_MILLIS,
                    CoreOptions.SCAN_WATERMARK,
                    CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
                    CoreOptions.SCAN_SNAPSHOT_ID,
                    CoreOptions.SCAN_TAG_NAME,
                    CoreOptions.SCAN_VERSION);

    protected final ObjectIdentifier tableIdentifier;
    protected final boolean streaming;
    protected final DynamicTableFactory.Context context;
    @Nullable protected final LogStoreTableFactory logStoreTableFactory;

    @Nullable protected WatermarkStrategy<RowData> watermarkStrategy;
    protected boolean isBatchCountStar;

    public BaseDataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            @Nullable Predicate predicate,
            @Nullable Predicate indexPredicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            boolean isBatchCountStar) {
        super(table, predicate, projectFields, limit);
        this.tableIdentifier = tableIdentifier;
        this.streaming = streaming;
        this.context = context;
        this.logStoreTableFactory = logStoreTableFactory;
        this.predicate = predicate;
        this.indexPredicate = indexPredicate;
        this.projectFields = projectFields;
        this.limit = limit;
        this.watermarkStrategy = watermarkStrategy;
        this.isBatchCountStar = isBatchCountStar;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (!streaming) {
            // batch merge all, return insert only
            return ChangelogMode.insertOnly();
        }

        if (table.primaryKeys().isEmpty()) {
            return ChangelogMode.insertOnly();
        } else {
            Options options = Options.fromMap(table.options());

            if (new CoreOptions(options).mergeEngine() == FIRST_ROW) {
                return ChangelogMode.insertOnly();
            }

            if (options.get(SCAN_REMOVE_NORMALIZE)) {
                return ChangelogMode.all();
            }

            if (logStoreTableFactory == null
                    && options.get(CHANGELOG_PRODUCER) != ChangelogProducer.NONE) {
                return ChangelogMode.all();
            }

            // optimization: transaction consistency and all changelog mode avoid the generation of
            // normalized nodes. See FlinkTableSink.getChangelogMode validation.
            return options.get(LOG_CONSISTENCY) == LogConsistency.TRANSACTIONAL
                            && options.get(LOG_CHANGELOG_MODE) == LogChangelogMode.ALL
                    ? ChangelogMode.all()
                    : ChangelogMode.upsert();
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        if (isBatchCountStar) {
            return createCountStarScan();
        }

        LogSourceProvider logSourceProvider = null;
        if (logStoreTableFactory != null) {
            logSourceProvider =
                    logStoreTableFactory.createSourceProvider(context, scanContext, projectFields);
        }

        WatermarkStrategy<RowData> watermarkStrategy = this.watermarkStrategy;
        Options options = Options.fromMap(table.options());
        if (watermarkStrategy != null) {
            WatermarkEmitStrategy emitStrategy = options.get(SCAN_WATERMARK_EMIT_STRATEGY);
            if (emitStrategy == WatermarkEmitStrategy.ON_EVENT) {
                watermarkStrategy = new OnEventWatermarkStrategy(watermarkStrategy);
            }
            Duration idleTimeout = options.get(SCAN_WATERMARK_IDLE_TIMEOUT);
            if (idleTimeout != null) {
                watermarkStrategy = watermarkStrategy.withIdleness(idleTimeout);
            }
            String watermarkAlignGroup = options.get(SCAN_WATERMARK_ALIGNMENT_GROUP);
            if (watermarkAlignGroup != null) {
                watermarkStrategy =
                        WatermarkAlignUtils.withWatermarkAlignment(
                                watermarkStrategy,
                                watermarkAlignGroup,
                                options.get(SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT),
                                options.get(SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL));
            }
        }

        FlinkSourceBuilder sourceBuilder =
                new FlinkSourceBuilder(table)
                        .sourceName(tableIdentifier.asSummaryString())
                        .sourceBounded(!streaming)
                        .logSourceProvider(logSourceProvider)
                        .projection(projectFields)
                        .predicate(predicate)
                        .indexPredicate(indexPredicate)
                        .limit(limit)
                        .watermarkStrategy(watermarkStrategy)
                        .dynamicPartitionFilteringFields(dynamicPartitionFilteringFields());

        return new PaimonDataStreamScanProvider(
                !streaming,
                env ->
                        sourceBuilder
                                .sourceParallelism(inferSourceParallelism(env))
                                .env(env)
                                .build());
    }

    private ScanRuntimeProvider createCountStarScan() {
        TableScan scan = table.newReadBuilder().withFilter(predicate).newScan();
        List<PartitionEntry> partitionEntries = scan.listPartitionEntries();
        long rowCount = partitionEntries.stream().mapToLong(PartitionEntry::recordCount).sum();
        NumberSequenceRowSource source = new NumberSequenceRowSource(rowCount, rowCount);
        return new SourceProvider() {
            @Override
            public Source<RowData, ?, ?> createSource() {
                return source;
            }

            @Override
            public boolean isBounded() {
                return true;
            }

            @Override
            public Optional<Integer> getParallelism() {
                return Optional.of(1);
            }
        };
    }

    private ScanRuntimeProvider createFileIndexAggregateScan() {
        return new SourceFunctionProvider() {
            @Override
            public SourceFunction<RowData> createSourceFunction() {
                return new LocalAggregateSource(null, null);
            }

            @Override
            public boolean isBounded() {
                return true;
            }
        };

    }

    protected abstract List<String> dynamicPartitionFilteringFields();

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    "Currently, lookup dim table only support FileStoreTable but is "
                            + table.getClass().getName());
        }

        if (limit != null) {
            throw new RuntimeException(
                    "Limit push down should not happen in Lookup source, but it is " + limit);
        }
        int[] projection =
                projectFields == null
                        ? IntStream.range(0, table.rowType().getFieldCount()).toArray()
                        : Projection.of(projectFields).toTopLevelIndexes();
        int[] joinKey = Projection.of(context.getKeys()).toTopLevelIndexes();
        Options options = new Options(table.options());
        boolean enableAsync = options.get(LOOKUP_ASYNC);
        int asyncThreadNumber = options.get(LOOKUP_ASYNC_THREAD_NUMBER);
        return LookupRuntimeProviderFactory.create(
                getFileStoreLookupFunction(
                        context,
                        timeTravelDisabledTable((FileStoreTable) table),
                        projection,
                        joinKey),
                enableAsync,
                asyncThreadNumber);
    }

    protected FileStoreLookupFunction getFileStoreLookupFunction(
            LookupContext context, Table table, int[] projection, int[] joinKey) {
        return new FileStoreLookupFunction(table, projection, joinKey, predicate);
    }

    private FileStoreTable timeTravelDisabledTable(FileStoreTable table) {
        Map<String, String> newOptions = new HashMap<>(table.options());
        TIME_TRAVEL_OPTIONS.stream().map(ConfigOption::key).forEach(newOptions::remove);

        CoreOptions.StartupMode startupMode = CoreOptions.fromMap(newOptions).startupMode();
        if (startupMode != CoreOptions.StartupMode.COMPACTED_FULL) {
            startupMode = CoreOptions.StartupMode.LATEST_FULL;
        }
        newOptions.put(CoreOptions.SCAN_MODE.key(), startupMode.toString());

        TableSchema newSchema = table.schema().copy(newOptions);
        return table.copy(newSchema);
    }

    @Override
    public boolean applyAggregates(
            List<int[]> groupingSets,
            List<AggregateExpression> aggregateExpressions,
            DataType producedDataType) {
        if (isStreaming()) {
            return false;
        }

        if (!(table instanceof DataTable)) {
            return false;
        }

        if (!table.primaryKeys().isEmpty()) {
            return false;
        }

        CoreOptions options = ((DataTable) table).coreOptions();
        if (options.deletionVectorsEnabled()) {
            return false;
        }

        if (groupingSets.size() != 1) {
            return false;
        }

        if (groupingSets.get(0).length != 0) {
            return false;
        }

        if (aggregateExpressions.size() != 1) {
            return false;
        }

        // todo: support apply aggregate by indexes
        if (indexPredicate != null) {
            return false;
        }

        if (aggregateExpressions
                .get(0)
                .getFunctionDefinition()
                .getClass()
                .getName()
                .equals(
                        "org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction")) {
            isBatchCountStar = true;
            return true;
        }

        // todo: 首先，将不同引擎的Aggregate函数，转换成paimon内置的Aggregate（像Predicate一样组装）
        List<AggregateFunction> functions = new ArrayList<>();

        // todo: 其次，通过visitor的方式判断是否所有aggregate函数都支持下推
        FileIndexAggregatePushDownVisitor indexPushDownVisitor =
                table.fileIndexAggregatePushDownVisitor();
        for (AggregateFunction function : functions) {
            if (!function.visit(indexPushDownVisitor)) {
                return false;
            }
        }

        // 支持，则返回对应的计算函数 和 fallback函数
        // 最终，组装成IndexScanner，然后不同引擎的SourceFunction去包装IndexScanner

        // 如何判断aggregate是否可以直接下推？

        // zone map索引可以加速min/max/count
        // bitmap索引可以加速count/count(字段)，where条件可以是bitmap & bsi索引
        // bsi索可以加速min/max/count/count(字段)/sum，where条件可以是bitmap & bsi索引

        // 判断传入：索引，过滤条件，聚合函数
        // 输出：生成一个IndexScanner，这个IndexScanner是paimon内部的，flink & spark & starrocks & trino直接开箱即用
        // 执行时：问索引，这些函数如何计算，把过滤条件传进去（考虑有索引怎么计算，没有索引又怎么算）
        // 首先判断能否直接用zone map索引返回，如果不能，再判断bitmap索引能否返回，如果不能，fallback to codegen

        // aggregate push down：输入索引，过滤条件，聚合函数。输出Optional<IndexScanner>

        return false;
    }

    @Override
    public String asSummaryString() {
        return "Paimon-DataSource";
    }

    @Override
    public boolean isStreaming() {
        return streaming;
    }
}
