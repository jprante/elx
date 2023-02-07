package org.xbib.elx.api;

import java.util.Collection;
import java.util.Map;

/**
 * Interface for extended managing and indexing methods of an Elasticsearch client.
 */
public interface AdminClient extends BasicClient {

    Map<String, Object> getMapping(IndexDefinition indexDefinition);

    void checkMapping(IndexDefinition indexDefinition);

    /**
     * Delete an index.
     * @param indexDefinition the index definition
     * @return this
     */
    AdminClient deleteIndex(IndexDefinition indexDefinition);

    AdminClient deleteIndex(String indexName);

    /**
     * Close an index.
     * @param indexDefinition the index definition
     * @return this
     */
    AdminClient closeIndex(IndexDefinition indexDefinition);

    AdminClient closeIndex(String indexName);

    /**
     * Open an index.
     * @param indexDefinition the index definition
     * @return this
     */
    AdminClient openIndex(IndexDefinition indexDefinition);

    AdminClient openIndex(String indexName);

    /**
     * Update replica level to the one in the index definition.
     * @param indexDefinition the index definition
     * @return this
     */
    AdminClient updateReplicaLevel(IndexDefinition indexDefinition);

    /**
     * Get replica level.
     * @param indexDefinition the index name
     * @return the replica level of the index
     */
    int getReplicaLevel(IndexDefinition indexDefinition);

    /**
     * Force segment merge of an index.
     * @param indexDefinition the index definition
     * @return this
     */
    boolean forceMerge(IndexDefinition indexDefinition);

    Collection<String> resolveIndex(String index);

    /**
     * Resolve alias.
     *
     * @param alias the alias
     * @return the index names in ordered sequence behind the alias or an empty list if there is no such alias
     */
    Collection<String> resolveAlias(String alias);

    /**
     * Resolve alias to all connected indices, sort index names with most recent timestamp on top, return this index
     * name.
     *
     * @param alias the alias
     * @return the most recent index name pointing to the alias
     */
    String resolveMostRecentIndex(String alias);

    /**
     * Get all index aliases.
     * @param index the index
     * @return map of index aliases
     */
    Map<String, String> getAliases(String index);

    /**
     * Shift from one index to another.
     * @param indexDefinition the index definition
     * @param additionalAliases new aliases
     * @param indexAliasAdder method to add aliases
     * @return this
     */
    IndexShiftResult shiftIndex(IndexDefinition indexDefinition,
                                Collection<String> additionalAliases,
                                IndexAliasAdder indexAliasAdder);

    /**
     * Prune index.
     *
     * @param indexDefinition the index definition
     * @return the index prune result
     */
    IndexPruneResult pruneIndex(IndexDefinition indexDefinition);

    /**
     * Find the timestamp of the most recently indexed document in the index.
     *
     * @param indexDefinition the index definition
     * @param timestampfieldname the timestamp field name
     * @return millis UTC millis of the most recent document
     */
    Long mostRecentDocument(IndexDefinition indexDefinition, String timestampfieldname);
}
