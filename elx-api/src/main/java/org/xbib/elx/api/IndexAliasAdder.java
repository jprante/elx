package org.xbib.elx.api;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;

@FunctionalInterface
public interface IndexAliasAdder {

    void addIndexAlias(IndicesAliasesRequestBuilder builder, String index, String alias);
}
