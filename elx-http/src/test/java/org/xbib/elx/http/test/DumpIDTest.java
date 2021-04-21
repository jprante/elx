package org.xbib.elx.http.test;

import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.http.HttpSearchClient;
import org.xbib.elx.http.HttpSearchClientProvider;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

class DumpIDTest {

    @Test
    void testDump() throws Exception {
        IndexDefinition indexDefinition = new DefaultIndexDefinition("zdb", "zdb");
        try (HttpSearchClient searchClient = ClientBuilder.builder()
                .setSearchClientProvider(HttpSearchClientProvider.class)
                .put("cluster.name", "es2")
                .put("host", "atlas:9202")
                .put("pool.enabled", false)
                .build();
             BufferedWriter writer = Files.newBufferedWriter(Paths.get("zdb.txt"))) {
            Stream<String> stream = searchClient.getIds(qb -> qb
                            .setIndices(indexDefinition.getIndex())
                            .setQuery(QueryBuilders.matchAllQuery()));
            stream.forEach(id -> {
                try {
                    writer.write(id);
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
