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
package com.actian.spark_vector.buffer.commons;

import java.math.BigInteger;

public final class BigIntegerConversion {

    private BigIntegerConversion() {
        throw new IllegalStateException();
    }

    public static byte[] convertToDoubleLongByteArray(BigInteger value) {
        byte[] source = value.toByteArray();
        byte[] target = new byte[16];
        int remaining = target.length - source.length;
        int resultIndex = 0;
        for (int sourceIndex = source.length - 1; sourceIndex >= 0 && resultIndex < target.length; sourceIndex--) {
            target[resultIndex++] = source[sourceIndex];
        }

        if (remaining > 0) {
            for (int index = 0; index < remaining; index++) {
                target[source.length + index] = (byte) (value.signum() >= 0 ? 0 : 0xFF);
            }
        }
        return target;
    }
}
