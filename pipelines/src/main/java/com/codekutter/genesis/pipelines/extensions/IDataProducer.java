package com.codekutter.genesis.pipelines.extensions;

import com.codekutter.genesis.pipelines.Context;

import java.util.List;

/**
 * Interface for implementing data producers.
 *
 * @param <T> - Entity Type.
 */
public interface IDataProducer<T> {
    /**
     * Fetch a data set based on the passed query.
     *
     * @param query   - Query Condition.
     * @param context - Context Handle.
     * @return - Fetched entities.
     * @throws DataServiceException
     */
    List<T> fetch(String query, Context context) throws DataServiceException;
}
