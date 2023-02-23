package org.xbib.elx.api;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;

/**
 * Interface for extended managing and indexing methods of an Elasticsearch client.
 */
public interface AdminClient extends BasicClient {

    /**
     * List all indices.
     * @return the names of the indices.
     */
    Collection<String> allIndices();

    /**
     * List all closed indices.
     * @return the names of the closed indices.
     */
    Collection<String> allClosedIndices();

    /**
     * List all closed indices which were created before a given instant.
     * @param instant the instant
     * @return the names of the closed indices
     */
    Collection<String> allClosedIndicesOlderThan(Instant instant);

    /**
     * Delete all closed indices which were created before a given instant.
     * @param instant the instant
     */
    void purgeAllClosedIndicesOlderThan(Instant instant);

    /**
     * Get the mapping of an index.
     *
     * @param indexDefinition the index definition
     * @return the mapping
     */
    Map<String, Object> getMapping(IndexDefinition indexDefinition);

    /**
     * Check the mapping.
     * @param indexDefinition the index definition
     */
    void checkMapping(IndexDefinition indexDefinition);

    /**
     * Delete an index.
     * @param indexDefinition the index definition
     */
    void deleteIndex(IndexDefinition indexDefinition);

    /**
     * Delete an index.
     * @param indexName the index name
     */
    void deleteIndex(String indexName);

    /**
     * Close an index.
     * @param indexDefinition the index definition
     */
    void closeIndex(IndexDefinition indexDefinition);

    /**
     * Close an index.
     * @param indexName the index name
     */
    void closeIndex(String indexName);

    /**
     * Open an index.
     * @param indexDefinition the index definition
     */
    void openIndex(IndexDefinition indexDefinition);

    /**
     * Open an index.
     * @param indexName the index name
     */
    void openIndex(String indexName);


    /**
     * Update replica level to the one in the index definition.
     * @param indexDefinition the index definition
     */
    void updateReplicaLevel(IndexDefinition indexDefinition);

    /**
     * Get replica level.
     * @param indexDefinition the index name
     * @return the replica level of the index
     */
    int getReplicaLevel(IndexDefinition indexDefinition);

    /**
     * Force segment merge of an index.
     * @param indexDefinition the index definition
     * @param maxNumSegments maximum number of segments
     */
    void forceMerge(IndexDefinition indexDefinition, int maxNumSegments);

    /**
     * Force segment merge of an index.
     * @param indexName the index name
     * @param maxNumSegments maximum number of segments
     */
    void forceMerge(String indexName, int maxNumSegments);

    /**
     * Resolve an alias.
     *
     * @param alias the alias name.
     * @return the index names in ordered sequence or an empty list if there is no such index
     */
    Collection<String> getAlias(String alias);

    /**
     * Resolve alias to a list of indices from cluster state.
     *
     * @param alias the alias
     * @return the index names in ordered sequence behind the alias or an empty list if there is no such alias
     */
    Collection<String> resolveAliasFromClusterState(String alias);

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
