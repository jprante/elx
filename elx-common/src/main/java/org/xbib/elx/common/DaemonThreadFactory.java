package org.xbib.elx.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DaemonThreadFactory implements ThreadFactory {

    private final ThreadGroup group;

    private final AtomicInteger threadNumber = new AtomicInteger(1);

    private final String namePrefix;

    public DaemonThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix;
        this.group = Thread.currentThread().getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + "[T#" + threadNumber.getAndIncrement() + "]", 0);
        t.setDaemon(true);
        return t;
    }
}
