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
package com.actian.spark_vectorh.buffer.time;

import static com.actian.spark_vectorh.buffer.time.TimeConversion.MILLISECONDS_IN_MINUTE;
import static com.actian.spark_vectorh.buffer.time.TimeConversion.scaledTime;
import static com.actian.spark_vectorh.buffer.time.TimeConversion.normalizedTime;

import java.util.Calendar;
import java.util.Date;

import com.actian.spark_vectorh.buffer.time.TimeConversion.TimeConverter;

final class TimeTZColumnFactoryCommons {
    private static final String TIME_TZ_TYPE_ID = "time with time zone";

    private TimeTZColumnFactoryCommons() {
        throw new IllegalStateException();
    }

    public static boolean isSupportedColumnType(String columnTypeName, int columnScale, int minScale, int maxScale) {
        return columnTypeName.equalsIgnoreCase(TIME_TZ_TYPE_ID) && minScale <= columnScale && columnScale <= maxScale;
    }

    public static final class TimeTZConverter extends TimeConverter {

        private final static long TIME_MASK = 0xFFFFFFFFFFFFF800L;

        @Override
        public long convert(long nanos, int scale) {
            return ((scaledTime(nanos, scale) << 11) & (TIME_MASK));
        }
    }
}
