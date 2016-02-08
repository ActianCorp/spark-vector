package com.actian.spark_vectorh.buffer;

import java.util.ArrayList;
import java.util.List;

import com.actian.spark_vectorh.buffer.decimal.DecimalByteColumnBufferFactory;
import com.actian.spark_vectorh.buffer.decimal.DecimalDoubleLongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.decimal.DecimalIntColumnBufferFactory;
import com.actian.spark_vectorh.buffer.decimal.DecimalLongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.decimal.DecimalShortColumnBufferFactory;
import com.actian.spark_vectorh.buffer.integer.ByteColumnBufferFactory;
import com.actian.spark_vectorh.buffer.integer.IntColumnBufferFactory;
import com.actian.spark_vectorh.buffer.integer.LongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.integer.ShortColumnBufferFactory;
import com.actian.spark_vectorh.buffer.real.DoubleColumnBufferFactory;
import com.actian.spark_vectorh.buffer.real.FloatColumnBufferFactory;
import com.actian.spark_vectorh.buffer.singles.BooleanColumnBufferFactory;
import com.actian.spark_vectorh.buffer.singles.DateColumnBufferFactory;
import com.actian.spark_vectorh.buffer.string.ConstantLengthMultiByteStringColumnBufferFactory;
import com.actian.spark_vectorh.buffer.string.ConstantLengthMultiCharStringColumnBufferFactory;
import com.actian.spark_vectorh.buffer.string.ConstantLengthSingleByteStringColumnBufferFactory;
import com.actian.spark_vectorh.buffer.string.ConstantLengthSingleCharStringColumnBufferFactory;
import com.actian.spark_vectorh.buffer.string.VariableLengthByteStringColumnBufferFactory;
import com.actian.spark_vectorh.buffer.string.VariableLengthCharStringColumnBufferFactory;
import com.actian.spark_vectorh.buffer.time.TimeLZIntColumnBufferFactory;
import com.actian.spark_vectorh.buffer.time.TimeLZLongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.time.TimeNZIntColumnBufferFactory;
import com.actian.spark_vectorh.buffer.time.TimeNZLongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.time.TimeTZIntColumnBufferFactory;
import com.actian.spark_vectorh.buffer.time.TimeTZLongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.timestamp.TimestampLZDoubleLongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.timestamp.TimestampLZLongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.timestamp.TimestampNZDoubleLongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.timestamp.TimestampNZLongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.timestamp.TimestampTZDoubleLongColumnBufferFactory;
import com.actian.spark_vectorh.buffer.timestamp.TimestampTZLongColumnBufferFactory;

public final class ColumnBufferFactoriesRegistry {

    private static final List<ColumnBufferFactory> factories = new ArrayList<ColumnBufferFactory>();
    static {
        factories.add(new ByteColumnBufferFactory());
        factories.add(new ShortColumnBufferFactory());
        factories.add(new IntColumnBufferFactory());
        factories.add(new LongColumnBufferFactory());
        factories.add(new VariableLengthCharStringColumnBufferFactory());
        factories.add(new ConstantLengthMultiCharStringColumnBufferFactory());
        factories.add(new ConstantLengthSingleCharStringColumnBufferFactory());
        factories.add(new VariableLengthByteStringColumnBufferFactory());
        factories.add(new ConstantLengthMultiByteStringColumnBufferFactory());
        factories.add(new ConstantLengthSingleByteStringColumnBufferFactory());
        factories.add(new DecimalByteColumnBufferFactory());
        factories.add(new DecimalDoubleLongColumnBufferFactory());
        factories.add(new DecimalIntColumnBufferFactory());
        factories.add(new DecimalLongColumnBufferFactory());
        factories.add(new DecimalShortColumnBufferFactory());
        factories.add(new DateColumnBufferFactory());
        factories.add(new BooleanColumnBufferFactory());
        factories.add(new DoubleColumnBufferFactory());
        factories.add(new FloatColumnBufferFactory());
        factories.add(new TimestampLZDoubleLongColumnBufferFactory());
        factories.add(new TimestampLZLongColumnBufferFactory());
        factories.add(new TimestampNZDoubleLongColumnBufferFactory());
        factories.add(new TimestampNZLongColumnBufferFactory());
        factories.add(new TimestampTZDoubleLongColumnBufferFactory());
        factories.add(new TimestampTZLongColumnBufferFactory());
        factories.add(new TimeLZIntColumnBufferFactory());
        factories.add(new TimeLZLongColumnBufferFactory());
        factories.add(new TimeNZIntColumnBufferFactory());
        factories.add(new TimeNZLongColumnBufferFactory());
        factories.add(new TimeTZIntColumnBufferFactory());
        factories.add(new TimeTZLongColumnBufferFactory());
    }

    public ColumnBufferFactory findFactoryForColumn(String type, int precision, int scale, boolean nullable) {
        for (ColumnBufferFactory factory : factories) {
            if (factory.supportsColumnType(type, precision, scale, nullable)) {
                return factory;
            }
        }
        return null;
    }
}
