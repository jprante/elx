package org.xbib.elx.api;

import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Interface for extended managing and indexing methods of an Elasticsearch client.
 */
public interface AdminClient extends BasicClient {

    /**
     * Build index definition from settings.
     *
     * @param index the index name
     * @param settings the settings for the index
     * @return index definition
     * @throws IOException if settings/mapping URL is invalid/malformed
     */
    IndexDefinition buildIndexDefinitionFromSettings(String index, Settings settings) throws IOException;

    Map<String, ?> getMapping(String index) throws IOException;

    void checkMapping(String index);

    /**
     * Delete an index.
     * @param indexDefinition the index definition
     * @return this
     */
    AdminClient deleteIndex(IndexDefinition indexDefinition);

    /**
     * Delete an index.
     *
     * @param index index
     * @return this
     */
    AdminClient deleteIndex(String index);


    /**
     * Update replica level.
     * @param indexDefinition the index definition
     * @param level the replica level
     * @return this
     * @throws IOException if replica setting could not be updated
     */
    AdminClient updateReplicaLevel(IndexDefinition indexDefinition, int level) throws IOException;

    /**
     * Update replica level.
     *
     * @param index index
     * @param level the replica level
     * @param maxWaitTime maximum wait time
     * @param timeUnit time unit
     * @return this
     * @throws IOException if replica setting could not be updated
     */
    AdminClient updateReplicaLevel(String index, int level, long maxWaitTime, TimeUnit timeUnit) throws IOException;

    /**
     * Get replica level.
     * @param indexDefinition the index name
     * @return the replica level of the index
     */
    int getReplicaLevel(IndexDefinition indexDefinition);

    /**
     * Get replica level.
     * @param index the index name
     * @return the replica level of the index
     */
    int getReplicaLevel(String index);

    /**
     * Force segment merge of an index.
     * @param indexDefinition the index definition
     * @return this
     */
    boolean forceMerge(IndexDefinition indexDefinition);

    /**
     * Force segment merge of an index.
     * @param index the index
     * @param maxWaitTime maximum wait time
     * @param timeUnit time unit
     * @return this
     */
    boolean forceMerge(String index, long maxWaitTime, TimeUnit timeUnit);

    /**
     * Resolve alias.
     *
     * @param alias the alias
     * @return this index name behind the alias or the alias if there is no index
     */
    List<String> resolveAlias(String alias);

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
     *  Shift from one index to another.
     * @param indexDefinition the index definition
     * @param additionalAliases new aliases
     * @return this
     */
    IndexShiftResult shiftIndex(IndexDefinition indexDefinition, List<String> additionalAliases);

    /**
     * Shift from one index to another.
     * @param indexDefinition the index definition
     * @param additionalAliases new aliases
     * @param indexAliasAdder method to add aliases
     * @return this
     */
    IndexShiftResult shiftIndex(IndexDefinition indexDefinition,
                                List<String> additionalAliases,
                                IndexAliasAdder indexAliasAdder);

    /**
     * Shift from one index to another.
     * @param index         the index name
     * @param fullIndexName the index name with timestamp
     * @param additionalAliases  a list of names that should be set as index aliases
     * @return this
     */
    IndexShiftResult shiftIndex(String index,
                                String fullIndexName,
                                List<String> additionalAliases);

    /**
     * Shift from one index to another.
     * @param index         the index name
     * @param fullIndexName the index name with timestamp
     * @param additionalAliases  a list of names that should be set as index aliases
     * @param adder         an adder method to create alias term queries
     * @return this
     */
    IndexShiftResult shiftIndex(String index,
                                String fullIndexName, List<String> additionalAliases,
                                IndexAliasAdder adder);


    /**
     * Apply retention policy to prune indices. All indices before delta should be deleted,
     * but the number of mintokeep indices must be kept.
     *
     * @param indexDefinition         index definition
     * @return the index prune result
     */
    IndexPruneResult pruneIndex(IndexDefinition indexDefinition);

    /**
     * Find the timestamp of the most recently indexed document in the index.
     *
     * @param index the index name
     * @param timestampfieldname the timestamp field name
     * @return millis UTC millis of the most recent document
     * @throws IOException if most rcent document can not be found
     */
    Long mostRecentDocument(String index, String timestampfieldname) throws IOException;
}
