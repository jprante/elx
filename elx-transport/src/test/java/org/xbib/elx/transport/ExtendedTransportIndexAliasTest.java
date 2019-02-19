package org.xbib.elx.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExtendedTransportIndexAliasTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(ExtendedTransportIndexAliasTest.class.getSimpleName());

    @Test
    public void testIndexAlias() throws Exception {
        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getSettings()).build();
        try {
            client.newIndex("test1234");
            for (int i = 0; i < 1; i++) {
                client.index("test1234", randomString(1), false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.refreshIndex("test1234");

            List<String> simpleAliases = Arrays.asList("a", "b", "c");
            client.switchIndex("test", "test1234", simpleAliases);

            client.newIndex("test5678");
            for (int i = 0; i < 1; i++) {
                client.index("test5678", randomString(1), false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.refreshIndex("test5678");

            simpleAliases = Arrays.asList("d", "e", "f");
            client.switchIndex("test", "test5678", simpleAliases, (builder, index, alias) ->
                    builder.addAlias(index, alias, QueryBuilders.termQuery("my_key", alias)));
            Map<String, String> indexFilters = client.getIndexFilters("test5678");
            logger.info("index filters of index test5678 = {}", indexFilters);
            assertTrue(indexFilters.containsKey("a"));
            assertTrue(indexFilters.containsKey("b"));
            assertTrue(indexFilters.containsKey("c"));
            assertTrue(indexFilters.containsKey("d"));
            assertTrue(indexFilters.containsKey("e"));

            Map<String, String> aliases = client.getAliasFilters("test");
            logger.info("aliases of alias test = {}", aliases);
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("d"));
            assertTrue(aliases.containsKey("e"));

            client.waitForResponses("30s");
            assertFalse(client.hasThrowable());
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            client.shutdown();
        }
    }
}
