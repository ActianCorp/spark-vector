/*
 * Copyright 2016 Actian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.actian.spark_vectorh.buffer.timestamp;

import static com.actian.spark_vectorh.buffer.timestamp.TimestampLZColumnFactoryCommons.isSupportedColumnType;

import com.actian.spark_vectorh.buffer.timestamp.TimestampConversion.TimestampConverter;
import com.actian.spark_vectorh.buffer.timestamp.TimestampLZColumnFactoryCommons.TimestampLZConverter;

public final class TimestampLZLongColumnBufferFactory extends TimestampLongColumnBufferFactory {
    private static final int MIN_TIMESTAMP_LZ_LONG_SCALE = 0;
    private static final int MAX_TIMESTAMP_LZ_LONG_SCALE = 7;

    @Override
    public boolean adjustToUTC() {
        return false;
    }

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return isSupportedColumnType(type, scale, MIN_TIMESTAMP_LZ_LONG_SCALE, MAX_TIMESTAMP_LZ_LONG_SCALE);
    }

    @Override
    protected TimestampConverter createConverter() {
        return new TimestampLZConverter();
    }
}
