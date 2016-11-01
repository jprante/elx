package org.xbib.elasticsearch.extras.client;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;

/**
 *
 */
public interface IndexAliasAdder {

    void addIndexAlias(IndicesAliasesRequestBuilder builder, String index, String alias);
}
