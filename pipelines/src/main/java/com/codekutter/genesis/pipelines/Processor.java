package com.codekutter.genesis.pipelines;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;

/**
 * Abstract base class for defining processors.
 *
 * @param <T> - Entity type this processor handles.
 */
public abstract class Processor<T> {
    /**
     * State of this processor instance.
     */
    protected ProcessState state = new ProcessState();

    /**
     * Name of this processor.
     */
    protected String name;

    /**
     * Get the name of this processor.
     *
     * @return - Processor Name
     */
    public String getName() {
        return name;
    }

    /**
     * Set the name of this processor.
     *
     * @param name - Processor Name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the state of this processor.
     *
     * @return - Processor State.
     */
    public EProcessState getState() {
        return state.getState();
    }

    /**
     * Check if the state of this processor matches the passed state.
     *
     * @param state - Expected State
     * @throws ProcessorException - Will throw exception if state doesn't match.
     */
    protected void checkState(EProcessState state) throws ProcessorException {
        Preconditions.checkArgument(state != null);
        if (this.state.getState() != state) {
            throw new ProcessorException(
                    String.format("Processor State error. [expected=%s][actual=%s]",
                                  state.name(), this.state.getState().name()));
        }
    }

    /**
     * Check if this processor is available.
     *
     * @throws ProcessorException - Will throw exception if processor is not available.
     */
    protected void isAvailable() throws ProcessorException {
        if (!state.isAvailable()) {
            throw new ProcessorException(
                    String.format("Processor is not available. [actual=%s]",
                                  this.state.getState().name()));
        }
    }

    /**
     * Dispose this process instance.
     */
    public void dispose() {
        if (!state.hasError()) {
            state.setState(EProcessState.Disposed);
        }
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
    public abstract ProcessorResponse<T> execute(@Nonnull T data, String condition,
                                                 Context context)
    throws ProcessorException;
}
