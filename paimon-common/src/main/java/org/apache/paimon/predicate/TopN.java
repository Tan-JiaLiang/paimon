package org.apache.paimon.predicate;

import java.io.Serializable;

/**
 * Represents the TopN, only a single field is supported currently.
 */
public class TopN implements Serializable {

    private static final long serialVersionUID = 1L;

    private final SortValue sort;
    private final int limit;

    public TopN(SortValue sort, int limit) {
        this.sort = sort;
        this.limit = limit;
    }

    public SortValue sort() {
        return sort;
    }

    public int limit() {
        return limit;
    }

    public <T> T visit(TopNVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
