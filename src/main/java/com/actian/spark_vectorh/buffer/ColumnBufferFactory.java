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
package com.actian.spark_vectorh.buffer;

public abstract class ColumnBufferFactory {

    public final ColumnBuffer<?> createColumnBuffer(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        if (!supportsColumnType(type, precision, scale, nullable)) {
            throw new IllegalArgumentException("Target column type [name=" + type + ", precision=" + precision + ", scale=" + scale + ", nullable= " + nullable
                    + "] is not supported!");
        }

        return createColumnBufferInternal(name, index, type, precision, scale, nullable, maxRowCount);
    }

    public abstract boolean supportsColumnType(String type, int precision, int scale, boolean nullable);

    protected abstract ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable,
            int maxRowCount);
}
