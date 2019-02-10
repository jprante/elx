package org.xbib.elasticsearch.client;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;

/**
 *
 */
@FunctionalInterface
public interface IndexAliasAdder {

    void addIndexAlias(IndicesAliasesRequestBuilder builder, String index, String alias);
}
