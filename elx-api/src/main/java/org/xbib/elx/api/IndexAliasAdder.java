package org.xbib.elx.api;

import org.elasticsearch.index.query.QueryBuilder;

@FunctionalInterface
public interface IndexAliasAdder {

    QueryBuilder addAliasOnField(String index, String alias);
}
