package org.apache.paimon.flink.source;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.table.source.ReadBuilder;

import javax.annotation.Nullable;

public class LocalAggregateSource extends RichSourceFunction<RowData> {

    private final ReadBuilder readBuilder;
    private final @Nullable Long limit;

    public LocalAggregateSource(ReadBuilder readBuilder, @Nullable Long limit) {
        this.readBuilder = readBuilder;
        this.limit = limit;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
