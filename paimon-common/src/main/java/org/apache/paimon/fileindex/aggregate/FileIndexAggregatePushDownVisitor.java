package org.apache.paimon.fileindex.aggregate;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FileIndexAggregatePushDownVisitor implements AggregateVisitor<Boolean> {

    private final Map<String, List<FileIndexAggregatePushDownAnalyzer>> analyzers;

    public FileIndexAggregatePushDownVisitor() {
        this(Collections.emptyMap());
    }

    public FileIndexAggregatePushDownVisitor(Map<String, List<FileIndexAggregatePushDownAnalyzer>> analyzers) {
        this.analyzers = analyzers;
    }

    @Override
    public Boolean visit(CountStar func) {
        for (List<FileIndexAggregatePushDownAnalyzer> analyzers : this.analyzers.values()) {
            for (FileIndexAggregatePushDownAnalyzer analyzer : analyzers) {
                if (analyzer.visit(func)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Boolean visit(Count func) {
        List<FileIndexAggregatePushDownAnalyzer> analyzers =
                this.analyzers.getOrDefault(func.field().name(), Collections.emptyList());
        for (FileIndexAggregatePushDownAnalyzer analyzer : analyzers) {
            if (analyzer.visit(func)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(Min func) {
        List<FileIndexAggregatePushDownAnalyzer> analyzers =
                this.analyzers.getOrDefault(func.field().name(), Collections.emptyList());
        for (FileIndexAggregatePushDownAnalyzer analyzer : analyzers) {
            if (analyzer.visit(func)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(Max func) {
        List<FileIndexAggregatePushDownAnalyzer> analyzers =
                this.analyzers.getOrDefault(func.field().name(), Collections.emptyList());
        for (FileIndexAggregatePushDownAnalyzer analyzer : analyzers) {
            if (analyzer.visit(func)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(Sum func) {
        List<FileIndexAggregatePushDownAnalyzer> analyzers =
                this.analyzers.getOrDefault(func.field().name(), Collections.emptyList());
        for (FileIndexAggregatePushDownAnalyzer analyzer : analyzers) {
            if (analyzer.visit(func)) {
                return true;
            }
        }
        return false;
    }
}
