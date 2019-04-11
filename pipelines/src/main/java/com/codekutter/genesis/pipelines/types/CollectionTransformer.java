package com.codekutter.genesis.pipelines.types;

import com.codekutter.genesis.pipelines.*;
import com.codekutter.zconfig.common.LogUtils;
import com.google.common.base.Preconditions;
import lombok.Data;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for implemented entity collection transformation processor.
 *
 * @param <S> - Source Entity Type.
 * @param <T> - Target Entity Type.
 */
@Data
public abstract class CollectionTransformer<S, T> extends CollectionProcessor<S> {

    protected CollectionProcessor<T> processor;
    protected IEntityTransformer<S, T> transformer;

    /**
     * Execute method to be implemented for processing the data passed.
     *
     * @param data     - List of Entity Object.
     * @param context  - Context Handle
     * @param response - Processor Response.
     * @return - Processor Response.
     */
    @Override
    protected CollectionProcessorResponse<S> execute(@Nonnull List<S> data,
                                                     Context context, @Nonnull
                                                             CollectionProcessorResponse<S> response) {
        Preconditions.checkArgument(data != null && !data.isEmpty());
        Preconditions.checkArgument(response != null);

        try {
            List<T> targetData = new ArrayList<>();
            for (S d : data) {
                T t = transformer.transform(d);
                if (t != null) {
                    targetData.add(t);
                }
            }
            if (!targetData.isEmpty()) {
                ProcessorResponse<List<T>> resp =
                        processor.execute(targetData, null, context);
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
            } else {
                response.setData(null);
                response.setState(EProcessorResponse.NullData);
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
