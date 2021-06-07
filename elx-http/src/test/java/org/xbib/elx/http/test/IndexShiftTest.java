package org.xbib.elx.http.test;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.api.IndexShiftResult;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.http.HttpAdminClient;
import org.xbib.elx.http.HttpAdminClientProvider;
import org.xbib.elx.http.HttpBulkClient;
import org.xbib.elx.http.HttpBulkClientProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class IndexShiftTest {

    private static final Logger logger = LogManager.getLogger(IndexShiftTest.class.getSimpleName());

    private final TestExtension.Helper helper;

    IndexShiftTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testIndexShift() throws Exception {
        try (HttpAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(HttpAdminClientProvider.class)
                .put(helper.getClientSettings())
                .build();
             HttpBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(HttpBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "doc");
            indexDefinition.setFullIndexName("test_shift");
            bulkClient.newIndex(indexDefinition);
            for (int i = 0; i < 1; i++) {
                bulkClient.index(indexDefinition, helper.randomString(1), false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            indexDefinition.setShift(true);
            IndexShiftResult indexShiftResult =
                    adminClient.shiftIndex(indexDefinition, Arrays.asList("a", "b", "c"), null);
            assertTrue(indexShiftResult.getNewAliases().contains("a"));
            assertTrue(indexShiftResult.getNewAliases().contains("b"));
            assertTrue(indexShiftResult.getNewAliases().contains("c"));
            assertTrue(indexShiftResult.getMovedAliases().isEmpty());
            Map<String, String> aliases = adminClient.getAliases(indexDefinition.getFullIndexName());
            logger.log(Level.DEBUG, "aliases = " + aliases);
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey(indexDefinition.getIndex()));
            Optional<String> resolved = adminClient.resolveAlias(indexDefinition.getIndex()).stream().findFirst();
            aliases = resolved.isPresent() ?
                    adminClient.getAliases(resolved.get()) : Collections.emptyMap();
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("test"));
            indexDefinition.setFullIndexName("test_shift2");
            bulkClient.newIndex(indexDefinition);
            for (int i = 0; i < 1; i++) {
                bulkClient.index(indexDefinition, helper.randomString(1), false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            indexDefinition.setShift(true);
            indexShiftResult = adminClient.shiftIndex(indexDefinition, Arrays.asList("d", "e", "f"),
                    (index, alias) -> QueryBuilders.termQuery("my_key", alias));
            assertTrue(indexShiftResult.getNewAliases().contains("d"));
            assertTrue(indexShiftResult.getNewAliases().contains("e"));
            assertTrue(indexShiftResult.getNewAliases().contains("f"));
            assertTrue(indexShiftResult.getMovedAliases().contains("a"));
            assertTrue(indexShiftResult.getMovedAliases().contains("b"));
            assertTrue(indexShiftResult.getMovedAliases().contains("c"));
            aliases = adminClient.getAliases("test_shift2");
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("d"));
            assertTrue(aliases.containsKey("e"));
            assertTrue(aliases.containsKey("f"));
            resolved = adminClient.resolveAlias("test").stream().findFirst();
            aliases = resolved.isPresent() ? adminClient.getAliases(resolved.get()) : Collections.emptyMap();
            assertTrue(aliases.containsKey("a"));
            assertTrue(aliases.containsKey("b"));
            assertTrue(aliases.containsKey("c"));
            assertTrue(aliases.containsKey("d"));
            assertTrue(aliases.containsKey("e"));
            assertTrue(aliases.containsKey("f"));
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
        }
    }
}
