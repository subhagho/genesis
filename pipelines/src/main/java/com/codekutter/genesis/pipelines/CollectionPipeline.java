package com.codekutter.genesis.pipelines;

import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.LogUtils;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectionPipeline<T> extends CollectionProcessor<T>
        implements Pipeline<List<T>> {
    private Map<String, CollectionProcessor<T>> processors = new HashMap<>();
    private Map<String, String> conditions = new HashMap<>();
    private List<ExceptionProcessor<List<T>>> exceptionProcessors;

    /**
     * Add a processor to this pipeline.
     * <p>
     * If the condition string is non-null, it will be used to decide if this
     * processor should execute on the passed entity or skipped.
     *
     * @param processor - Processor instance.
     * @param condition - Condition string.
     * @return - Self.
     */
    @SuppressWarnings("unchecked")
    public CollectionPipeline<T> addProcessor(
            @Nonnull CollectionProcessor<?> processor,
            String condition) {
        Preconditions.checkArgument(processor != null);
        processors.put(processor.name, (CollectionProcessor<T>) processor);
        if (!Strings.isNullOrEmpty(condition)) {
            conditions.put(processor.name, condition);
        }
        return this;
    }

    /**
     * Add an exception processor for this pipeline.
     *
     * @param handler - Exception Handler.
     * @return - Self
     */
    public CollectionPipeline<T> addErrorHandler(
            @Nonnull ExceptionProcessor<List<T>> handler) {
        Preconditions.checkArgument(handler != null);

        if (exceptionProcessors == null) {
            exceptionProcessors = new ArrayList<>();
        }
        exceptionProcessors.add(handler);
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
     * Method to initialize the processor from the configuration.
     * <p>
     * Note: Use the MethodInvoke annotation with the required path
     * to auto-wire the initialisation.
     *
     * @param node - Configuration Node.
     * @throws ConfigurationException
     */
    @Override
    public void init(AbstractConfigNode node) throws ConfigurationException {
        // Nothing Additional to be done.
    }

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
                                                     Context context,
                                                     @Nonnull
                                                             CollectionProcessorResponse<T> response) {
        Preconditions.checkArgument(data != null);
        Preconditions.checkArgument(response != null);
        if (!processors.isEmpty()) {
            response.setData(data);
            for (String name : processors.keySet()) {
                CollectionProcessor<T> processor = processors.get(name);
                try {
                    String condition = conditions.get(name);
                    response = (CollectionProcessorResponse<T>) processor
                            .execute(response.data, condition, context);
                    if (response.hasError()) {
                        response = handleException(response);
                    }
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

    /**
     * Check and invoke the exception handlers.
     *
     * @param response - Exception Response.
     * @return - Processed Response.
     */
    private CollectionProcessorResponse<T> handleException(
            CollectionProcessorResponse<T> response) {
        if (exceptionProcessors != null && !exceptionProcessors.isEmpty()) {
            for (ExceptionProcessor<List<T>> ep : exceptionProcessors) {
                response =
                        (CollectionProcessorResponse<T>) ep.handleError(response);
            }
        }
        return response;
    }
}
