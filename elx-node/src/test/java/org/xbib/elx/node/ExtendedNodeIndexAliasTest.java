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

import static org.junit.Assert.assertFalse;

@Ignore
public class ExtendedNodeIndexAliasTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(ExtendedNodeIndexAliasTest.class.getSimpleName());

    @Test
    public void testIndexAlias() throws Exception {
        final ExtendedNodeClient client = ClientBuilder.builder(client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .build();
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
            Map<String, String> aliases = client.getIndexFilters("test5678");
            logger.info("aliases of index test5678 = {}", aliases);

            aliases = client.getAliasFilters("test");
            logger.info("aliases of alias test = {}", aliases);

        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.waitForResponses("30s");
            client.shutdown();
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
        }
    }
}
