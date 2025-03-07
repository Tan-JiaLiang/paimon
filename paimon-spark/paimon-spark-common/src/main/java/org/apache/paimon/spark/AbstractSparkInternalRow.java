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

package org.apache.paimon.spark;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.spark.data.SparkInternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Objects;

import static org.apache.paimon.spark.DataConverter.fromPaimon;
import static org.apache.paimon.utils.InternalRowUtils.copyInternalRow;

/**
 * An abstract {@link SparkInternalRow} that overwrite all the common methods in spark3 and spark4.
 */
public abstract class AbstractSparkInternalRow extends SparkInternalRow {

    protected RowType rowType;

    protected InternalRow row;

    public AbstractSparkInternalRow(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public SparkInternalRow replace(InternalRow row) {
        this.row = row;
        return this;
    }

    @Override
    public int numFields() {
        return row.getFieldCount();
    }

    @Override
    public void setNullAt(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(int i, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.spark.sql.catalyst.InternalRow copy() {
        return SparkInternalRow.create(rowType).replace(copyInternalRow(row, rowType));
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return row.isNullAt(ordinal);
    }

    @Override
    public boolean getBoolean(int ordinal) {
        return row.getBoolean(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
        return row.getByte(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
        return row.getShort(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
        return row.getInt(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
        if (rowType.getTypeAt(ordinal) instanceof BigIntType) {
            return row.getLong(ordinal);
        }

        return getTimestampMicros(ordinal);
    }

    private long getTimestampMicros(int ordinal) {
        DataType type = rowType.getTypeAt(ordinal);
        return fromPaimon(row.getTimestamp(ordinal, DataTypeChecks.getPrecision(type)));
    }

    @Override
    public float getFloat(int ordinal) {
        return row.getFloat(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
        return row.getDouble(ordinal);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
        org.apache.paimon.data.Decimal decimal = row.getDecimal(ordinal, precision, scale);
        return fromPaimon(decimal);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
        return fromPaimon(row.getString(ordinal));
    }

    @Override
    public byte[] getBinary(int ordinal) {
        return row.getBinary(ordinal);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.spark.sql.catalyst.InternalRow getStruct(int ordinal, int numFields) {
        return fromPaimon(row.getRow(ordinal, numFields), (RowType) rowType.getTypeAt(ordinal));
    }

    @Override
    public ArrayData getArray(int ordinal) {
        return fromPaimon(row.getArray(ordinal), (ArrayType) rowType.getTypeAt(ordinal));
    }

    @Override
    public MapData getMap(int ordinal) {
        return fromPaimon(row.getMap(ordinal), rowType.getTypeAt(ordinal));
    }

    @Override
    public Object get(int ordinal, org.apache.spark.sql.types.DataType dataType) {
        if (isNullAt(ordinal) || dataType instanceof NullType) {
            return null;
        }
        if (dataType instanceof BooleanType) {
            return getBoolean(ordinal);
        }
        if (dataType instanceof ByteType) {
            return getByte(ordinal);
        }
        if (dataType instanceof ShortType) {
            return getShort(ordinal);
        }
        if (dataType instanceof IntegerType) {
            return getInt(ordinal);
        }
        if (dataType instanceof LongType) {
            return getLong(ordinal);
        }
        if (dataType instanceof FloatType) {
            return getFloat(ordinal);
        }
        if (dataType instanceof DoubleType) {
            return getDouble(ordinal);
        }
        if (dataType instanceof StringType
                || dataType instanceof CharType
                || dataType instanceof VarcharType) {
            return getUTF8String(ordinal);
        }
        if (dataType instanceof DecimalType) {
            DecimalType dt = (DecimalType) dataType;
            return getDecimal(ordinal, dt.precision(), dt.scale());
        }
        if (dataType instanceof DateType) {
            return getInt(ordinal);
        }
        if (dataType instanceof TimestampType) {
            return getLong(ordinal);
        }
        if (dataType instanceof CalendarIntervalType) {
            return getInterval(ordinal);
        }
        if (dataType instanceof BinaryType) {
            return getBinary(ordinal);
        }
        if (dataType instanceof StructType) {
            return getStruct(ordinal, ((StructType) dataType).size());
        }
        if (dataType instanceof org.apache.spark.sql.types.ArrayType) {
            return getArray(ordinal);
        }
        if (dataType instanceof org.apache.spark.sql.types.MapType) {
            return getMap(ordinal);
        }
        if (dataType instanceof UserDefinedType) {
            return get(ordinal, ((UserDefinedType<?>) dataType).sqlType());
        }

        throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractSparkInternalRow that = (AbstractSparkInternalRow) o;
        if (Objects.equals(rowType, that.rowType)) {
            try {
                return Objects.equals(row, that.row);
            } catch (Exception e) {
                // The underlying row may not support equals or hashcode, e.g., `ProjectedRow`,
                // to be safe, we fallback to a slow performance version.
                return InternalRowUtils.equals(row, that.row, rowType);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        try {
            return Objects.hash(rowType, row);
        } catch (Exception e) {
            return InternalRowUtils.hash(row, rowType);
        }
    }
}
