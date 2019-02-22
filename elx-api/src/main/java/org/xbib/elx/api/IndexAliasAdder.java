package org.xbib.elx.api;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;

@FunctionalInterface
public interface IndexAliasAdder {

    void addIndexAlias(IndicesAliasesRequest requwst, String index, String alias);
}
