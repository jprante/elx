package org.xbib.elx.api;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;
import java.io.Flushable;
import java.util.concurrent.TimeUnit;

public interface BulkClient extends BasicClient, Flushable {

    /**
     * Create a new index.
     * @param indexDefinition the index definition
     */
    void newIndex(IndexDefinition indexDefinition);

    /**
     * Start bulk mode for indexes.
     * @param indexDefinition index definition
     */
    void startBulk(IndexDefinition indexDefinition);

    /**
     * Stop bulk mode.
     *
     * @param indexDefinition index definition
     */
    void stopBulk(IndexDefinition indexDefinition);

    /**
     * Add index request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when limits are exceeded.
     *
     * @param indexDefinition the index definition
     * @param id the id
     * @param create true if document must be created
     * @param source the source
     * @return this
     */
    BulkClient index(IndexDefinition indexDefinition, String id, boolean create, BytesReference source);

    /**
     * Index request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when limits are exceeded.
     *
     * @param indexDefinition the index definition
     * @param id the id
     * @param create true if document is to be created, false otherwise
     * @param source the source
     * @return this client methods
     */
    BulkClient index(IndexDefinition indexDefinition, String id, boolean create, String source);

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
     * @param indexDefinition the index definition
     * @param id the id
     * @return this
     */
    BulkClient delete(IndexDefinition indexDefinition, String id);

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
     * @param indexDefinition the index definition
     * @param id the id
     * @param source the source
     * @return this
     */
    BulkClient update(IndexDefinition indexDefinition, String id, BytesReference source);

    /**
     * Update document. Use with precaution! Does not work in all cases.
     *
     * @param indexDefinition the index definition
     * @param id the id
     * @param source the source
     * @return this
     */
    BulkClient update(IndexDefinition indexDefinition, String id, String source);

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
     */
    void updateIndexSetting(String index, String key, Object value, long timeout, TimeUnit timeUnit);

    /**
     * Refresh the index.
     *
     * @param indexDefinition index definition
     */
    void refreshIndex(IndexDefinition indexDefinition);

    /**
     * Flush the index. The cluster clears cache and completes indexing.
     *
     * @param indexDefinition index definition
     */
    void flushIndex(IndexDefinition indexDefinition);

    BulkProcessor getBulkProcessor();
}
