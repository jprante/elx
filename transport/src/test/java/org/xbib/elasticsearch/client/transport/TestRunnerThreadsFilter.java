package org.xbib.elasticsearch.client.transport;

import com.carrotsearch.randomizedtesting.ThreadFilter;

public class TestRunnerThreadsFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread thread) {
        return thread.getName().startsWith("ObjectCleanerThread");
    }
}
