package org.xbib.elasticsearch.client.common;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.testframework.ESSingleNodeTestCase;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AliasTests extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(AliasTests.class.getName());

    public void testAlias() {
        CreateIndexRequest indexRequest = new CreateIndexRequest("test");
        client().admin().indices().create(indexRequest).actionGet();
        // put alias
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                .index("test").alias("test_alias")
        );
        client().admin().indices().aliases(indicesAliasesRequest).actionGet();
        // get alias
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest(Strings.EMPTY_ARRAY);
        long t0 = System.nanoTime();
        GetAliasesResponse getAliasesResponse = client().admin().indices().getAliases(getAliasesRequest).actionGet();
        long t1 = (System.nanoTime() - t0) / 1000000;
        logger.info("{} time(ms) = {}", getAliasesResponse.getAliases(), t1);
        assertTrue(t1 >= 0);
    }

    public void testMostRecentIndex() {
        String alias = "test";
        CreateIndexRequest indexRequest = new CreateIndexRequest("test20160101");
        client().admin().indices().create(indexRequest).actionGet();
        indexRequest = new CreateIndexRequest("test20160102");
        client().admin().indices().create(indexRequest).actionGet();
        indexRequest = new CreateIndexRequest("test20160103");
        client().admin().indices().create(indexRequest).actionGet();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add()
                .indices("test20160101", "test20160102", "test20160103")
                .alias(alias)
        );
        client().admin().indices().aliases(indicesAliasesRequest).actionGet();

        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client(),
                GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(alias).execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        Set<String> result = new TreeSet<>(Collections.reverseOrder());
        for (ObjectCursor<String> indexName : getAliasesResponse.getAliases().keys()) {
            Matcher m = pattern.matcher(indexName.value);
            if (m.matches() && alias.equals(m.group(1))) {
                result.add(indexName.value);
            }
        }
        Iterator<String> it = result.iterator();
        assertEquals("test20160103", it.next());
        assertEquals("test20160102", it.next());
        assertEquals("test20160101", it.next());
        logger.info("result={}", result);
    }
}
