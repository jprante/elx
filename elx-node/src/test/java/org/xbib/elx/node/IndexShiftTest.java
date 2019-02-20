package org.xbib.elx.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Ignore;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Ignore
public class IndexShiftTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(IndexShiftTest.class.getSimpleName());

    @Test
    public void testIndexShift() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .build();
        try {
            client.newIndex("test1234");
            for (int i = 0; i < 1; i++) {
                client.index("test1234", randomString(1), false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flush();
            client.refreshIndex("test1234");

            List<String> simpleAliases = Arrays.asList("a", "b", "c");
            client.shiftIndex("test", "test1234", simpleAliases);

            client.newIndex("test5678");
            for (int i = 0; i < 1; i++) {
                client.index("test5678", randomString(1), false, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flush();
            client.refreshIndex("test5678");

            simpleAliases = Arrays.asList("d", "e", "f");
            client.shiftIndex("test", "test5678", simpleAliases, (builder, index, alias) ->
                    builder.addAlias(index, alias, QueryBuilders.termQuery("my_key", alias)));
            Map<String, String> indexFilters = client.getIndexFilters("test5678");
            logger.info("aliases of index test5678 = {}", indexFilters);
            assertTrue(indexFilters.containsKey("a"));
            assertTrue(indexFilters.containsKey("b"));
            assertTrue(indexFilters.containsKey("c"));
            assertTrue(indexFilters.containsKey("d"));
            assertTrue(indexFilters.containsKey("e"));

            Map<String, String> aliases = client.getIndexFilters(client.resolveAlias("test"));
            logger.info("aliases of alias test = {}", aliases);
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("d"));
            assertTrue(aliases.containsKey("e"));

        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.waitForResponses(30L, TimeUnit.SECONDS);
            client.close();
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
        }
    }
}
