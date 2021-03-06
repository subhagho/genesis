package com.codekutter.genesis.pipelines.types;

import com.codekutter.genesis.pipelines.*;
import com.codekutter.zconfig.common.LogUtils;
import com.google.common.base.Preconditions;
import lombok.Data;

import javax.annotation.Nonnull;

/**
 * Abstract base class for implemented entity transformation processor.
 *
 * @param <S> - Source Entity Type.
 * @param <T> - Target Entity Type.
 */
@Data
public abstract class Transformer<S, T> extends BasicProcessor<S> {
    protected BasicProcessor<T> processor;
    protected IEntityTransformer<S, T> transformer;

    /**
     * Execute method to be implemented for processing the data passed.
     *
     * @param data     - Entity Object.
     * @param context  - Context Handle
     * @param response - Processor Response.
     * @return - Processor Response.
     */
    @Override
    protected ProcessorResponse<S> execute(@Nonnull S data,
                                           Context context,
                                           @Nonnull ProcessorResponse<S> response) {
        Preconditions.checkArgument(data != null);
        Preconditions.checkArgument(response != null);

        try {
            T output = transformer.transform(data);
            if (output == null) {
                response.setData(null);
                response.setState(EProcessorResponse.NullData);
            } else {
                ProcessorResponse<T> resp =
                        processor.execute(output, null, context);
                if (resp.hasError()) {
                    if (resp.getState() == EProcessorResponse.FatalError) {
                        throw new ProcessorException(resp.getError());
                    }
                } else {
                    response.setState(resp.getState());
                    if (resp.hasError()) {
                        response.setError(resp.getState(), resp.getError());
                    }
                }
            }
        } catch (TransformtationException | ProcessorException ex) {
            LogUtils.debug(getClass(), ex);
            response.setError(EProcessorResponse.FatalError, ex);
        } catch (Exception ex) {
            LogUtils.debug(getClass(), ex);
            response.setError(EProcessorResponse.UnhandledError, ex);
        }
        return response;
    }
}
