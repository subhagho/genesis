package com.codekutter.genesis.pipelines.extensions;


import com.codekutter.genesis.pipelines.Context;

import java.util.List;

/**
 * Interface for implementing Data Consumers.
 *
 * @param <T> - Entity Type
 * @param <O> - Operation Type
 */
public interface IDataConsumer<T, O> {
    /**
     * Process an entity instance.
     *
     * @param data - Entity instance
     * @param operation - Operation to be performed.
     * @param context - Context Handle.
     *
     * @return - Modified Entity.
     * @throws DataServiceException
     */
    T process(T data, O operation, Context context) throws DataServiceException;

    /**
     * Process the list of entities passed.
     *
     * @param dataSet - List of Entities
     * @param operation - Operation to be performed.
     * @param context - Context Handle.
     * @return - Updated List of Entities.
     * @throws DataServiceException
     */
    List<T> process(List<T> dataSet, O operation, Context context) throws DataServiceException;
}
