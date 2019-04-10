package com.codekutter.genesis.pipelines.types;

import com.codekutter.genesis.pipelines.*;
import com.codekutter.genesis.pipelines.extensions.IEntityDataProducer;
import com.codekutter.zconfig.common.LogUtils;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Data producer base class for keyed entities.
 *
 * @param <T> - Entity Type.
 * @param <K> - Entity Key Type.
 */
public class DataProducer<T extends Entity<K>, K> extends BasicPipeline<T> {
    protected IEntityDataProducer<T, K> producer;

    /**
     * Read and process an entity instance fetched by the specified key.
     *
     * @param key     - Entity Key.
     * @param context - Context Handle.
     * @return - Fetched entity.
     * @throws ProcessorException
     */
    public T read(@Nonnull K key, Context context) throws ProcessorException {
        Preconditions.checkArgument(key != null);
        try {
            T data = read(key, context);
            if (data != null) {
                ProcessorResponse<T> response =
                        execute(data, null, context);
                if (response == null) {
                    throw new ProcessorException(
                            "Execute returned NULL response.");
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
                List<T> results = new ArrayList<>();
                for (T d : data) {
                    ProcessorResponse<T> response =
                            execute(d, null, context);
                    if (response == null) {
                        throw new ProcessorException(
                                "Execute returned NULL response.");
                    }
                    if (response.hasError()) {
                        throw new ProcessorException(response.getError());
                    }
                    if (response.getState() == EProcessorResponse.OK) {
                        results.add(response.getData());
                    }
                }
                return results;
            }
        } catch (Exception ex) {
            LogUtils.debug(getClass(), ex);
            throw new ProcessorException(ex);
        }
        return null;
    }
}
