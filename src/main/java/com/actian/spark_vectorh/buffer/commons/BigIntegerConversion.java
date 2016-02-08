package com.actian.spark_vectorh.buffer.commons;

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
