package org.xbib.elx.api;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;

@FunctionalInterface
public interface IndexAliasAdder {

    void addIndexAlias(IndicesAliasesRequest request, String index, String alias);
}
