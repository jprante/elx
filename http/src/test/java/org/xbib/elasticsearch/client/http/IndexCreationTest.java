package org.xbib.elasticsearch.client.http;

import org.junit.Test;
import org.xbib.elasticsearch.client.ClientBuilder;

import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class IndexCreationTest {

    private static final Logger logger = Logger.getLogger(IndexCreationTest.class.getName());
    static {
        //System.setProperty("io.netty.leakDetection.level", "paranoid");
        System.setProperty("io.netty.noKeySetOptimization", Boolean.toString(true));
        System.setProperty("log4j2.disable.jmx", Boolean.toString(true));

        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] %5$s %6$s%n");
        LogManager.getLogManager().reset();
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        Handler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        rootLogger.addHandler(handler);
        rootLogger.setLevel(Level.ALL);
        for (Handler h : rootLogger.getHandlers()) {
            handler.setFormatter(new SimpleFormatter());
            h.setLevel(Level.ALL);
        }
    }

    @Test
    public void testNewIndex() throws Exception {
        HttpClient client = ClientBuilder.builder()
                //.put(ClientBuilder.FLUSH_INTERVAL, TimeValue.timeValueSeconds(5))
                .put("urls", "http://localhost:9200")
                //.setMetric(new SimpleBulkMetric())
                //.setControl(new SimpleBulkControl())
                .getClient(HttpClient.class);
        try {
            client.newIndex("demo");
            Thread.sleep(3000L);
        } finally {
            client.shutdown();
        }
    }
}
