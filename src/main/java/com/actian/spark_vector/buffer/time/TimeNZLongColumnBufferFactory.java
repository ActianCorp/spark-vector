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
package com.actian.spark_vector.buffer.time;

import static com.actian.spark_vector.buffer.time.TimeNZColumnFactoryCommons.isSupportedColumnType;

import com.actian.spark_vector.buffer.time.TimeConversion.TimeConverter;
import com.actian.spark_vector.buffer.time.TimeNZColumnFactoryCommons.TimeNZConverter;

public final class TimeNZLongColumnBufferFactory extends TimeLongColumnBufferFactory {
    private static final int MIN_TIME_LONG_NZ_SCALE = 5;
    private static final int MAX_TIME_LONG_NZ_SCALE = 9;

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return isSupportedColumnType(type, scale, MIN_TIME_LONG_NZ_SCALE, MAX_TIME_LONG_NZ_SCALE);
    }

    @Override
    protected TimeConverter createConverter() {
        return new TimeNZConverter();
    }
}
