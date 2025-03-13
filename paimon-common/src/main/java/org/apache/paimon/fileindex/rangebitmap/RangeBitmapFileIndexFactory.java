package org.apache.paimon.fileindex.rangebitmap;

import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.FileIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

/** Factory to create {@link RangeBitmapFileIndex}. */
public class RangeBitmapFileIndexFactory implements FileIndexerFactory {

    public static final String RANGE_BITMAP_INDEX = "range-bitmap";

    @Override
    public String identifier() {
        return RANGE_BITMAP_INDEX;
    }

    @Override
    public FileIndexer create(DataType dataType, Options options) {
        return new RangeBitmapFileIndex(dataType, options);
    }
}
