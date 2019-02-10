package org.xbib.elasticsearch.client.node;

import com.carrotsearch.randomizedtesting.ThreadFilter;

public class TestRunnerThreadsFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread thread) {
        return thread.getName().startsWith("ObjectCleanerThread");
    }
}
