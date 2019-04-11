package com.codekutter.genesis.pipelines.types;

import com.codekutter.genesis.pipelines.*;
import com.codekutter.genesis.pipelines.extensions.IDataConsumer;
import com.codekutter.zconfig.common.LogUtils;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;

/**
 * Abstract base class to implement data consumers/sinks.
 *
 * @param <T> - Entity Type.
 * @param <O> - Operation Type.
 */
public abstract class DataConsumer<T, O> extends BasicProcessor<T>
        implements IOperationParser<O> {

    protected IDataConsumer<T, O> consumer;

    /**
     * Execute method to be implemented for processing the data passed.
     * <p>
     * context is required in this case:
     * Context Key:
     * email.operation = EEmailOperation.
     *
     * @param data     - Entity Object.
     * @param context  - Context Handle
     * @param response - Processor Response.
     * @return - Processor Response.
     */
    @Override
    protected ProcessorResponse<T> execute(@Nonnull T data,
                                           Context context, @Nonnull
                                                   ProcessorResponse<T> response) {
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
