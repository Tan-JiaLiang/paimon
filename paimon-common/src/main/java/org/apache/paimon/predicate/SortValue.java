package org.apache.paimon.predicate;

import java.io.Serializable;

/** Represents a sort order. */
public class SortValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FieldRef field;
    private final SortDirection direction;
    private final NullOrdering nullOrdering;

    public SortValue(FieldRef field, SortDirection direction, NullOrdering nullOrdering) {
        this.field = field;
        this.direction = direction;
        this.nullOrdering = nullOrdering;
    }

    public FieldRef field() {
        return field;
    }

    public SortDirection direction() {
        return direction;
    }

    public NullOrdering nullOrdering() {
        return nullOrdering;
    }

    /** A null order used in sorting expressions. */
    public enum NullOrdering {
        NULLS_FIRST, NULLS_LAST
    }

    /** A sort direction used in sorting expressions. */
    public enum SortDirection {
        ASCENDING, DESCENDING
    }
}
