package com.codekutter.genesis.pipelines.types;

import com.codekutter.genesis.pipelines.*;
import com.codekutter.genesis.pipelines.extensions.IDataProducer;
import com.codekutter.zconfig.common.LogUtils;

import java.util.List;

/**
 * Abstract base class for defining data producer pipelines.
 *
 * @param <T> - Entity Type
 */
public abstract class CollectionDataProducer<T> extends CollectionPipeline<T> {
    /**
     * Data Producer instance.
     */
    protected IDataProducer<T> producer;

    /**
     * Read a collection of entities.
     * <p>
     * If query is passed, the query will be used to filter the results.
     *
     * @param query   - Query condition to filter results.
     * @param context - Context Handle.
     * @return - List of fetched entities.
     * @throws ProcessorException
     */
    public List<T> read(String query, Context context) throws
                                                       ProcessorException {
        try {
            List<T> data = producer.fetch(query, context);
            if (data != null && !data.isEmpty()) {
                ProcessorResponse<List<T>> response =
                        execute(data, null, context);
                if (response == null) {
                    throw new ProcessorException("Execute returned NULL response.");
                }
                if (response.hasError()) {
                    throw new ProcessorException(response.getError());
                }
                if (response.getState() == EProcessorResponse.OK) {
                    return response.getData();
                }
            }
        } catch (Exception ex) {
            LogUtils.debug(getClass(), ex);
            throw new ProcessorException(ex);
        }
        return null;
    }
}
