package org.xbib.elx.api;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Interface for extended managing and indexing methods of an Elasticsearch client.
 */
public interface ExtendedClient extends Flushable, Closeable {

    /**
     * Set an Elasticsearch client to extend from it. May be null for TransportClient.
     * @param client client
     * @return this client
     */
    ExtendedClient setClient(ElasticsearchClient client);

    /**
     * Return Elasticsearch client.
     *
     * @return Elasticsearch client
     */
    ElasticsearchClient getClient();

    /**
     * Get buulk control.
     * @return the bulk control
     */
    BulkController getBulkController();

    /**
     * Initiative the extended client, the bulk metric and bulk controller,
     * creates instances and connect to cluster, if required.
     *
     * @param settings settings
     * @return this client
     * @throws IOException if init fails
     */
    ExtendedClient init(Settings settings) throws IOException;

    /**
     * Build index definition from settings.
     *
     * @param index the index name
     * @param settings the settings for the index
     * @return index definition
     * @throws IOException if settings/mapping URL is invalid/malformed
     */
    IndexDefinition buildIndexDefinitionFromSettings(String index, Settings settings) throws IOException;

    /**
     * Add index request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when limits are exceeded.
     *
     * @param index  the index
     * @param id     the id
     * @param create true if document must be created
     * @param source the source
     * @return this
     */
    ExtendedClient index(String index, String id, boolean create, BytesReference source);

    /**
     * Index request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when limits are exceeded.
     *
     * @param index  the index
     * @param id     the id
     * @param create true if document is to be created, false otherwise
     * @param source the source
     * @return this client methods
     */
    ExtendedClient index(String index, String id, boolean create, String source);

    /**
     * Index request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     *
     * @param indexRequest the index request to add
     * @return this
     */
    ExtendedClient index(IndexRequest indexRequest);

    /**
     * Delete request.
     *
     * @param index the index
     * @param id    the id
     * @return this
     */
    ExtendedClient delete(String index, String id);

    /**
     * Delete request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     *
     * @param deleteRequest the delete request to add
     * @return this
     */
    ExtendedClient delete(DeleteRequest deleteRequest);

    /**
     * Bulked update request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     * Note that updates only work correctly when all operations between nodes are synchronized.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return this
     * @throws IOException if update fails
     */
    ExtendedClient update(String index, String id, BytesReference source) throws IOException;

    /**
     * Update document. Use with precaution! Does not work in all cases.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return this
     */
    ExtendedClient update(String index, String id, String source);

    /**
     * Bulked update request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     * Note that updates only work correctly when all operations between nodes are synchronized.
     *
     * @param updateRequest the update request to add
     * @return this
     */
    ExtendedClient update(UpdateRequest updateRequest);

    /**
     * Create a new index.
     *
     * @param index index
     * @return this
     * @throws IOException if new index creation fails
     */
    ExtendedClient newIndex(String index) throws IOException;

    /**
     * Create a new index.
     *
     * @param index index
     * @param settings settings
     * @param mapping mapping
     * @return this
     * @throws IOException if settings/mapping is invalid or index creation fails
     */
    ExtendedClient newIndex(String index, InputStream settings, InputStream mapping) throws IOException;

    /**
     * Create a new index.
     *
     * @param index index
     * @param settings settings
     * @return this
     * @throws IOException if settings is invalid or index creation fails
     */
    ExtendedClient newIndex(String index, Settings settings) throws IOException;

    /**
     * Create a new index.
     *
     * @param index index
     * @param settings settings
     * @param mapping mapping
     * @return this
     * @throws IOException if settings/mapping is invalid or index creation fails
     */
    ExtendedClient newIndex(String index, Settings settings, String mapping) throws IOException;

    /**
     * Create a new index.
     *
     * @param index index
     * @param settings settings
     * @param mapping mapping
     * @return this
     * @throws IOException if settings/mapping is invalid or index creation fails
     */
    ExtendedClient newIndex(String index, Settings settings, XContentBuilder mapping) throws IOException;

    /**
     * Create a new index.
     *
     * @param index index
     * @param settings settings
     * @param mapping mapping
     * @return this
     * @throws IOException if settings/mapping is invalid or index creation fails
     */
    ExtendedClient newIndex(String index, Settings settings, Map<String, ?> mapping) throws IOException;

    /**
     * Create a new index.
     * @param indexDefinition the index definition
     * @return this
     * @throws IOException if settings/mapping is invalid or index creation fails
     */
    ExtendedClient newIndex(IndexDefinition indexDefinition) throws IOException;

    /**
     * Delete an index.
     * @param indexDefinition the index definition
     * @return this
     */
    ExtendedClient deleteIndex(IndexDefinition indexDefinition);

    /**
     * Delete an index.
     *
     * @param index index
     * @return this
     */
    ExtendedClient deleteIndex(String index);

    /**
     * Start bulk mode for indexes.
     * @param indexDefinition index definition
     * @return this
     * @throws IOException if bulk could not be started
     */
    ExtendedClient startBulk(IndexDefinition indexDefinition) throws IOException;

    /**
     * Start bulk mode.
     *
     * @param index index
     * @param startRefreshIntervalSeconds refresh interval before bulk
     * @param stopRefreshIntervalSeconds  refresh interval after bulk
     * @return this
     * @throws IOException if bulk could not be started
     */
    ExtendedClient startBulk(String index, long startRefreshIntervalSeconds,
                             long stopRefreshIntervalSeconds) throws IOException;

    /**
     * Stop bulk mode.
     *
     * @param indexDefinition index definition
     * @return this
     * @throws IOException if bulk could not be startet
     */
    ExtendedClient stopBulk(IndexDefinition indexDefinition) throws IOException;

    /**
     * Stops bulk mode.
     *
     * @param index index
     * @param timeout maximum wait time
     * @param timeUnit time unit for timeout
     * @return this
     * @throws IOException if bulk could not be stopped
     */
    ExtendedClient stopBulk(String index, long timeout, TimeUnit timeUnit) throws IOException;

    /**
     * Update replica level.
     * @param indexDefinition the index definition
     * @param level the replica level
     * @return this
     * @throws IOException if replica setting could not be updated
     */
    ExtendedClient updateReplicaLevel(IndexDefinition indexDefinition, int level) throws IOException;

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
    ExtendedClient updateReplicaLevel(String index, int level, long maxWaitTime, TimeUnit timeUnit) throws IOException;

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
     * Refresh the index.
     *
     * @param index index
     * @return this
     */
    ExtendedClient refreshIndex(String index);

    /**
     * Flush the index. The cluster clears cache and completes indexing.
     *
     * @param index index
     * @return this
     */
    ExtendedClient flushIndex(String index);

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
     * Wait for all outstanding bulk responses.
     *
     * @param timeout maximum wait time
     * @param timeUnit unit of timeout value
     * @return true if wait succeeded, false if wait timed out
     */
    boolean waitForResponses(long timeout, TimeUnit timeUnit);

    /**
     * Wait for cluster being healthy.
     *
     * @param healthColor cluster health color to wait for
     * @param maxWaitTime   time value
     * @param timeUnit time unit
     * @return true if wait succeeded, false if wait timed out
     */
    boolean waitForCluster(String healthColor, long maxWaitTime, TimeUnit timeUnit);

    /**
     * Get current health color.
     *
     * @param maxWaitTime maximum wait time
     * @param timeUnit time unit
     * @return the cluster health color
     */
    String getHealthColor(long maxWaitTime, TimeUnit timeUnit);

    /**
     * Wait for index recovery (after replica change).
     *
     * @param index index
     * @param maxWaitTime maximum wait time
     * @param timeUnit time unit
     * @return true if wait succeeded, false if wait timed out
     */
    boolean waitForRecovery(String index, long maxWaitTime, TimeUnit timeUnit);

    /**
     * Update index setting.
     * @param index the index
     * @param key the key of the value to be updated
     * @param value the new value
     * @param timeout timeout
     * @param timeUnit time unit
     * @throws IOException if update index setting failed
     */
    void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit) throws IOException;

    /**
     * Resolve alias.
     *
     * @param alias the alias
     * @return this index name behind the alias or the alias if there is no index
     */
    String resolveAlias(String alias);

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
    IndexShiftResult shiftIndex(IndexDefinition indexDefinition, List<String> additionalAliases,
                                IndexAliasAdder indexAliasAdder);

    /**
     * Shift from one index to another.
     * @param index         the index name
     * @param fullIndexName the index name with timestamp
     * @param additionalAliases  a list of names that should be set as index aliases
     * @return this
     */
    IndexShiftResult shiftIndex(String index, String fullIndexName, List<String> additionalAliases);

    /**
     * Shift from one index to another.
     * @param index         the index name
     * @param fullIndexName the index name with timestamp
     * @param additionalAliases  a list of names that should be set as index aliases
     * @param adder         an adder method to create alias term queries
     * @return this
     */
    IndexShiftResult shiftIndex(String index, String fullIndexName, List<String> additionalAliases,
                                IndexAliasAdder adder);

    /**
     * Prune index.
     * @param indexDefinition the index definition
     * @return the index prune result
     */
    IndexPruneResult pruneIndex(IndexDefinition indexDefinition);

    /**
     * Apply retention policy to prune indices. All indices before delta should be deleted,
     * but the number of mintokeep indices must be kept.
     *
     * @param index         index name
     * @param fullIndexName index name with timestamp
     * @param delta timestamp delta (for index timestamps)
     * @param mintokeep     minimum number of indices to keep
     * @param perform true if pruning should be executed, false if not
     * @return the index prune result
     */
    IndexPruneResult pruneIndex(String index, String fullIndexName, int delta, int mintokeep, boolean perform);

    /**
     * Find the timestamp of the most recently indexed document in the index.
     *
     * @param index the index name
     * @param timestampfieldname the timestamp field name
     * @return millis UTC millis of the most recent document
     * @throws IOException if most rcent document can not be found
     */
    Long mostRecentDocument(String index, String timestampfieldname) throws IOException;

    /**
     * Get cluster name.
     * @return the cluster name
     */
    String getClusterName();
}
