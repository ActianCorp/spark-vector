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

import static com.actian.spark_vector.buffer.time.TimeConversion.scaledTime;

import com.actian.spark_vector.buffer.time.TimeConversion.TimeConverter;

final class TimeNZColumnFactoryCommons {
    private static final String TIME_NZ_TYPE_ID_1 = "time";
    private static final String TIME_NZ_TYPE_ID_2 = "time without time zone";

    private TimeNZColumnFactoryCommons() {
        throw new IllegalStateException();
    }

    public static boolean isSupportedColumnType(String columnTypeName, int columnScale, int minScale, int maxScale) {
        return (columnTypeName.equalsIgnoreCase(TIME_NZ_TYPE_ID_1) || columnTypeName.equalsIgnoreCase(TIME_NZ_TYPE_ID_2)) && minScale <= columnScale
                && columnScale <= maxScale;
    }

    public static final class TimeNZConverter extends TimeConverter {
        @Override
        public long convert(long nanos, int scale) {
            return scaledTime(nanos, scale);
        }
    }
}
