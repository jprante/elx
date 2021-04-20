package org.xbib.elx.common;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

public class LongRingBuffer {

    private final Long[] values1, values2;

    private final int limit;

    private final AtomicInteger index;

    public LongRingBuffer(int limit) {
        this.values1 = new Long[limit];
        this.values2 = new Long[limit];
        Arrays.fill(values1, -1L);
        Arrays.fill(values2, -1L);
        this.limit = limit;
        this.index = new AtomicInteger();
    }

    public int add(Long v1, Long v2) {
        int i = index.incrementAndGet() % limit;
        values1[i] = v1;
        values2[i] = v2;
        return i;
    }

    public LongStream longStreamValues1() {
        return Arrays.stream(values1).filter(v -> v != -1L).mapToLong(Long::longValue);
    }

    public LongStream longStreamValues2() {
        return Arrays.stream(values2).filter(v -> v != -1L).mapToLong(Long::longValue);
    }
}
