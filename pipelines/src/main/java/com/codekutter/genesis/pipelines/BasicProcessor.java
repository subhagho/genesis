package com.codekutter.genesis.pipelines;

import com.codekutter.genesis.pipelines.utils.ConditionProcessor;
import com.codekutter.genesis.pipelines.utils.ConditionProcessorFactory;
import com.codekutter.zconfig.common.LogUtils;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Basic Abstract processor class to operate on entities.
 *
 * @param <T> - Entity type.
 */
public abstract class BasicProcessor<T> extends Processor<T> {
    @ConfigAttribute(name = "type", required = true)
    private Class<T> type;

    /**
     * Get the entity type for this processor.
     *
     * @return - Entity Type.
     */
    public Class<T> getType() {
        return type;
    }

    /**
     * Set the entity type for this processor.
     *
     * @param type - Entity Type.
     */
    public void setType(Class<T> type) {
        this.type = type;
    }

    /**
     * Check if the passed entity matches the specified condition.
     *
     * @param data      - Entity Data
     * @param condition - Condition to match.
     * @return - Matches?
     */
    private boolean matchCondition(T data, String condition) {
        if (Strings.isNullOrEmpty(condition)) {
            return true;
        }
        ConditionProcessor<T> processor =
                ConditionProcessorFactory.getProcessor(type);
        return processor.matches(data, condition);
    }

    /**
     * Processing method to be implemented by sub-classes. Entry method
     * to trigger the processor.
     *
     * @param data      - Data Object
     * @param condition - Query Condition to check if execution is required.
     * @param context   - Context Handle.
     * @return - Processor Response.
     * @throws ProcessorException
     */
    @SuppressWarnings("unchecked")
    @Override
    public ProcessorResponse<T> execute(@Nonnull T data, String condition,
                                        Context context) throws ProcessorException {
        isAvailable();

        ProcessorResponse<T> response = new ProcessorResponse<>();
        response.setState(EProcessorResponse.Unknown);
        response.setData(data);
        try {
            if (!matchCondition(data, condition)) {
                response.setState(EProcessorResponse.Skipped);
            } else {
                ProcessorResponse<T> r = execute(data, context, response);
                if (r == null) {
                    LogUtils.error(getClass(), String.format(
                            "BasicProcessor returned NULL response. [type=%s]",
                            getClass().getCanonicalName()));
                    response.setError(EProcessorResponse.FatalError,
                                      new Exception(String.format(
                                              "BasicProcessor returned NULL response. [type=%s]",
                                              getClass().getCanonicalName())));
                } else {
                    response = r;
                    if (response.getState() == EProcessorResponse.UnhandledError ||
                            response.getState() == EProcessorResponse.FatalError) {
                        LogUtils.error(getClass(), response.getError());
                    }
                }
            }
        } catch (Exception ex) {
            response.setError(EProcessorResponse.UnhandledError, ex);
            LogUtils.error(getClass(), response.getError());
        }
        return response;
    }

    /**
     * Execute method to be implemented for processing the data passed.
     *
     * @param data     - Entity Object.
     * @param context  - Context Handle
     * @param response - Processor Response.
     * @return - Processor Response.
     */
    protected abstract ProcessorResponse<T> execute(@Nonnull T data,
                                                    Context context,
                                                    @Nonnull
                                                            ProcessorResponse<T> response);
}
