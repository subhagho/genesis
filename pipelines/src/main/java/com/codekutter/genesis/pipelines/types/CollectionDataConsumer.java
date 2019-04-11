package com.codekutter.genesis.pipelines.types;

import com.codekutter.genesis.pipelines.*;
import com.codekutter.genesis.pipelines.extensions.IDataConsumer;
import com.codekutter.zconfig.common.LogUtils;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Abstract base class to implement data consumers/sinks for a list of data elements.
 *
 * @param <T> - Entity Type.
 * @param <O> - Operation Type.
 */
public abstract class CollectionDataConsumer<T, O> extends CollectionProcessor<T>
        implements IOperationParser<O> {
    protected IDataConsumer<T, O> consumer;

    /**
     * Execute method to be implemented for processing the data passed.
     *
     * @param data     - List of Entity Object.
     * @param context  - Context Handle
     * @param response - Processor Response.
     * @return - Processor Response.
     */
    @Override
    protected CollectionProcessorResponse<T> execute(@Nonnull List<T> data,
                                                     Context context, @Nonnull
                                                             CollectionProcessorResponse<T> response) {
        Preconditions.checkArgument(data != null);
        Preconditions.checkArgument(context != null);
        Preconditions.checkArgument(response != null);

        try {
            Object os = context.getParameter(CONTEXT_KEY_OPERATION);
            if (os == null) {
                response.setError(EProcessorResponse.FatalError,
                                  new ProcessorException(
                                          String.format(
                                                  "Operation not found in Context. [key=%s]",
                                                  CONTEXT_KEY_OPERATION)));
            }
            O operation = parseOperation(os);
            data = consumer.process(data, operation, context);
            response.setState(EProcessorResponse.OK);
            response.setData(data);

        } catch (Throwable ex) {
            LogUtils.debug(getClass(), ex);
            response.setError(EProcessorResponse.UnhandledError, ex);
        }
        return response;
    }
}
