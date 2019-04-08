package com.codekutter.genesis.pipelines;

import com.codekutter.genesis.pipelines.utils.ConditionProcessor;
import com.codekutter.genesis.pipelines.utils.ConditionProcessorFactory;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

/**
 * Pipeline Exception Response processor.
 *
 * @param <T> - Entity Type.
 */
public abstract class ExceptionProcessor<T> {
    private Class<T> type;
    @ConfigValue(name = "condition", required = false)
    private String condition;

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
    @SuppressWarnings("unchecked")
    public void setType(@Nonnull Class<?> type) {
        Preconditions.checkArgument(type != null);
        this.type = (Class<T>) type;
    }

    /**
     * Get the filter condition for calling this
     * handler.
     *
     * @return - Filter condition
     */
    public String getCondition() {
        return condition;
    }

    /**
     * Set the filter condition for calling this
     * handler.
     *
     * @param condition - Filter condition
     */
    public void setCondition(String condition) {
        this.condition = condition;
    }

    /**
     * Check if the passed entity matches the specified condition.
     *
     * @param data      - Entity Data
     * @param condition - Condition to match.
     * @return - Matches?
     */
    private boolean matchCondition(T data, String condition) {
        if (data == null || Strings.isNullOrEmpty(condition)) {
            return true;
        }
        ConditionProcessor<T> processor =
                ConditionProcessorFactory.getProcessor(type);
        return processor.matches(data, condition);
    }

    /**
     * Handle Error response.
     * <p>
     * Note: If the severity of the error is reduced (from FatalError/UnhandledError)
     * the pipeline processing will continue.
     *
     * @param errorResponse - Response with Error.
     * @return - Handler Response.
     */
    public ProcessorResponse<T> handleError(
            @Nonnull ProcessorResponse<T> errorResponse) {
        if (matchCondition(errorResponse.data, condition)) {
            errorResponse = handle(errorResponse);
        }
        return errorResponse;
    }

    /**
     * Handler function to be implemented to handle the error condition.
     *
     * @param errorResponse - Response with Error.
     * @return - Handler Response.
     */
    protected abstract ProcessorResponse<T> handle(
            @Nonnull ProcessorResponse<T> errorResponse);
}
