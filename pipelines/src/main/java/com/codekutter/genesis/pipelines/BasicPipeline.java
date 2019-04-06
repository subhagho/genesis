package com.codekutter.genesis.pipelines;

import com.codekutter.zconfig.common.LogUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * Basic Pipeline type - Pipeline executed processors on an entity instance.
 *
 * @param <T> - Entity Type.
 */
public class BasicPipeline<T> extends BasicProcessor<T> implements Pipeline<T> {
    private Map<String, BasicProcessor<T>> processors = new HashMap<>();
    private Map<String, String> conditions = new HashMap<>();

    /**
     * Add a processor to this pipeline.
     *
     * If the condition string is non-null, it will be used to decide if this
     * processor should execute on the passed entity or skipped.
     *
     * @param processor - Processor instance.
     * @param condition - Condition string.
     * @return - Self.
     */
    public BasicPipeline<T> addProcessor(@Nonnull BasicProcessor<T> processor,
                                         String condition) {
        Preconditions.checkArgument(processor != null);
        processors.put(processor.name, processor);
        if (!Strings.isNullOrEmpty(condition)) {
            conditions.put(processor.name, condition);
        }
        return this;
    }

    /**
     * Dispose this process instance.
     */
    @Override
    public void dispose() {
        super.dispose();
        if (!processors.isEmpty()) {
            for (String name : processors.keySet()) {
                processors.get(name).dispose();
            }
        }
    }

    /**
     * Execute method to be implemented for processing the data passed.
     *
     * @param data     - Entity Object.
     * @param context  - Context Handle
     * @param response - Processor Response.
     * @return - Processor Response.
     */
    @Override
    protected ProcessorResponse<T> execute(@Nonnull T data, Context context,
                                           @Nonnull ProcessorResponse<T> response) {
        Preconditions.checkArgument(data != null);
        Preconditions.checkArgument(response != null);
        if (!processors.isEmpty()) {
            response.setData(data);
            for (String name : processors.keySet()) {
                BasicProcessor<T> processor = processors.get(name);
                try {
                    String condition = conditions.get(name);
                    response = processor.execute(response.data, condition, context);
                    if (response.getState() == EProcessorResponse.FatalError ||
                            response.getState() ==
                                    EProcessorResponse.UnhandledError) {
                        throw new ProcessorException(response.getError());
                    } else if (response.getState() ==
                            EProcessorResponse.StopWithError) {
                        LogUtils.error(getClass(), response.getError());
                        break;
                    } else if (response.getState() ==
                            EProcessorResponse.ContinueWithError) {
                        LogUtils.warn(getClass(), response.getError());
                    } else if (response.getState() ==
                            EProcessorResponse.StopWithOk) {
                        break;
                    }
                    if (response.data == null) {
                        LogUtils.debug(getClass(), String.format(
                                "Response returned NULL data. [processor=%s]",
                                processor.name));
                        break;
                    }
                } catch (ProcessorException e) {
                    LogUtils.error(getClass(), e);
                    response.setError(e);
                }
            }
        } else {
            response.setState(EProcessorResponse.Skipped);
        }
        return response;
    }
}
