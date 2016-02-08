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
package com.actian.spark_vectorh.buffer.string;

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * String handling utility functions.
 */
final class StringConversion {

    private static final Charset UTF8CHARSET = Charset.forName("UTF-8");
    private static final byte HIGH_BIT_MASK = (byte) 0x80;
    private static final byte MULTI_BYTE_START_MASK = (byte) 0xC0;
    private static final byte[] EMPTY_STRING = {};

    private StringConversion() {
        throw new IllegalStateException();
    }

    /**
     * Convert the given string into UTF-8 bytes truncating if necessary. If the
     * string value encoded into UTF-8 bytes is too long to fit into the given
     * target size, the bytes are truncated. However, UTF-8 supports multi-byte
     * character encoding. Truncate the bytes to prevent splitting a multi-byte
     * character.
     * <p>
     * The returned array contains the exact number of bytes that represent the
     * given string in UTF-8 truncated to size.
     * <p>
     * An empty string (zero-length byte array) may be returned if not
     * characters of the source string will fit in a byte array of the target
     * size.
     * 
     * @param value
     *            string value to convert and truncate
     * @param targetSize
     *            the target size in bytes
     * @return an array of UTF-8 encoded bytes
     */
    public static byte[] truncateToUTF8Bytes(String value, int targetSize) {
        byte[] bytes = value.getBytes(UTF8CHARSET);
        if (bytes.length > targetSize) {

            // Find from the end of the array the first byte which is a single
            // byte character
            // or the start of a multi-byte character
            for (int i = targetSize; i >= 0; i--) {
                // Single byte
                if ((bytes[i] & HIGH_BIT_MASK) != HIGH_BIT_MASK) {
                    // Include the single byte
                    return Arrays.copyOf(bytes, Math.min(targetSize, i + 1));
                }
                // Start of multi-byte
                else if ((bytes[i] & MULTI_BYTE_START_MASK) == MULTI_BYTE_START_MASK) {
                    // Exclude the start of the multi-byte character
                    return Arrays.copyOf(bytes, i);
                }
            }

            // No characters from the source fit in the target size buffer
            return EMPTY_STRING;
        }

        // Encoded bytes fit within wanted size
        return bytes;
    }

    /**
     * Truncates the given string to the desired number of UTF-16 code units, if
     * necessary, and returns the UTF-8 encoded result. If the truncated UTF-16
     * string ends with a high surrogate, it will be removed before encoding in
     * UTF-8.
     * 
     * @param value
     *            string value to truncate and convert
     * @param targetSize
     *            the target size in UTF-16 code units
     * @return an array of UTF-8 encoded bytes
     */
    public static byte[] truncateToUTF16CodeUnits(String value, int targetSize) {
        if (value.length() > targetSize) {
            if (Character.isHighSurrogate(value.charAt(targetSize - 1))) {
                return value.substring(0, targetSize - 1).getBytes(UTF8CHARSET);
            } else {
                return value.substring(0, targetSize).getBytes(UTF8CHARSET);
            }
        } else {
            return value.getBytes(UTF8CHARSET);
        }
    }
}
