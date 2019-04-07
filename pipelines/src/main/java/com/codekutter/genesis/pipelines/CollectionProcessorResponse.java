package com.codekutter.genesis.pipelines;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response handle type for CollectionProcessors/CollectionPipelines.
 *
 * @param <T> - Entity Type.
 */
public class CollectionProcessorResponse<T> extends ProcessorResponse<List<T>> {
    private Map<T, Exception> exceptions = null;

    /**
     * Add an error associated with the specified entity.
     *
     * @param data - Entity instance.
     * @param ex   - Error handle.
     * @return - Self
     */
    public CollectionProcessorResponse<T> addException(@Nonnull T data,
                                                       @Nonnull Exception ex) {
        Preconditions.checkArgument(data != null);
        Preconditions.checkArgument(ex != null);

        if (exceptions == null) {
            exceptions = new HashMap<>();
        }
        exceptions.put(data, ex);

        return this;
    }

    /**
     * Add an error associated with the specified entity.
     *
     * @param data    - Entity instance.
     * @param message - Error message.
     * @return - Self
     */
    public CollectionProcessorResponse<T> addException(@Nonnull T data,
                                                       @Nonnull String message) {
        Preconditions.checkArgument(data != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(message));

        if (exceptions == null) {
            exceptions = new HashMap<>();
        }
        exceptions.put(data, new ProcessorException(message));

        return this;
    }

    /**
     * Get the entity exceptions registered.
     *
     * @return - Entity Exceptions.
     */
    public Map<T, Exception> getExceptions() {
        return exceptions;
    }

    /**
     * Check if this response instance has entity exceptions.
     *
     * @return - Has Errors?
     */
    public boolean hasErrors() {
        return (exceptions != null && !exceptions.isEmpty());
    }
}
