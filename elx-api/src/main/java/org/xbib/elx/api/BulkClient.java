package org.xbib.elx.api;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.io.Flushable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface BulkClient extends BasicClient, Flushable {

    /**
     * Get bulk control.
     * @return the bulk control
     */
    BulkController getBulkController();


    /**
     * Create a new index.
     *
     * @param index index
     * @throws IOException if new index creation fails
     */
    void newIndex(String index) throws IOException;

    /**
     * Create a new index.
     * @param indexDefinition the index definition
     * @throws IOException if settings/mapping is invalid or index creation fails
     */
    void newIndex(IndexDefinition indexDefinition) throws IOException;

    /**
     * Create a new index.
     *
     * @param index index
     * @param settings settings
     * @throws IOException if settings is invalid or index creation fails
     */
    void newIndex(String index, Settings settings) throws IOException;

    /**
     * Create a new index.
     *
     * @param index index
     * @param settings settings
     * @param mapping mapping
     * @throws IOException if settings/mapping is invalid or index creation fails
     */
    void newIndex(String index, Settings settings, XContentBuilder mapping) throws IOException;

    /**
     * Create a new index.
     *
     * @param index index
     * @param settings settings
     * @param mapping mapping
     * @throws IOException if settings/mapping is invalid or index creation fails
     */
    void newIndex(String index, Settings settings, Map<String, ?> mapping) throws IOException;

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
    BulkClient index(String index, String id, boolean create, BytesReference source);

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
    BulkClient index(String index, String id, boolean create, String source);

    /**
     * Index request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     *
     * @param indexRequest the index request to add
     * @return this
     */
    BulkClient index(IndexRequest indexRequest);

    /**
     * Delete request.
     *
     * @param index the index
     * @param id    the id
     * @return this
     */
    BulkClient delete(String index, String id);

    /**
     * Delete request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     *
     * @param deleteRequest the delete request to add
     * @return this
     */
    BulkClient delete(DeleteRequest deleteRequest);

    /**
     * Bulked update request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     * Note that updates only work correctly when all operations between nodes are synchronized.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return this
     */
    BulkClient update(String index, String id, BytesReference source);

    /**
     * Update document. Use with precaution! Does not work in all cases.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return this
     */
    BulkClient update(String index, String id, String source);

    /**
     * Bulked update request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     * Note that updates only work correctly when all operations between nodes are synchronized.
     *
     * @param updateRequest the update request to add
     * @return this
     */
    BulkClient update(UpdateRequest updateRequest);

    /**
     * Start bulk mode for indexes.
     * @param indexDefinition index definition
     * @throws IOException if bulk could not be started
     */
    void startBulk(IndexDefinition indexDefinition) throws IOException;

    /**
     * Start bulk mode.
     *
     * @param index index
     * @param startRefreshIntervalSeconds refresh interval before bulk
     * @param stopRefreshIntervalSeconds  refresh interval after bulk
     * @throws IOException if bulk could not be started
     */
    void startBulk(String index, long startRefreshIntervalSeconds,
                          long stopRefreshIntervalSeconds) throws IOException;

    /**
     * Stop bulk mode.
     *
     * @param indexDefinition index definition
     * @throws IOException if bulk could not be startet
     */
    void stopBulk(IndexDefinition indexDefinition) throws IOException;

    /**
     * Stops bulk mode.
     *
     * @param index index
     * @param timeout maximum wait time
     * @param timeUnit time unit for timeout
     * @throws IOException if bulk could not be stopped
     */
    void stopBulk(String index, long timeout, TimeUnit timeUnit) throws IOException;

    /**
     * Wait for all outstanding bulk responses.
     *
     * @param timeout maximum wait time
     * @param timeUnit unit of timeout value
     * @return true if wait succeeded, false if wait timed out
     */
    boolean waitForResponses(long timeout, TimeUnit timeUnit);

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
     * Refresh the index.
     *
     * @param index index
     */
    void refreshIndex(String index);

    /**
     * Flush the index. The cluster clears cache and completes indexing.
     *
     * @param index index
     */
    void flushIndex(String index);

}
