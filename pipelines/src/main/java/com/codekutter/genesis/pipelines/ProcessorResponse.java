package com.codekutter.genesis.pipelines;

import com.codekutter.zconfig.common.AbstractState;

/**
 * Response object returned by process executions.
 *
 * @param <T> - Entity Type.
 */
public class ProcessorResponse<T> extends AbstractState<EProcessorResponse> {
    protected T data;

    /**
     * Get the exception associated with this state. Exception handle will be returned
     * only if the current state is error.
     *
     * @return - Exception handle, null if state is not error.
     */
    @Override
    public Throwable getError() {
        if (getState() == EProcessorResponse.ContinueWithError ||
                getState() == EProcessorResponse.FatalError ||
                getState() == EProcessorResponse.StopWithError ||
                getState() == EProcessorResponse.UnhandledError) {
            return error;
        }
        return null;
    }

    /**
     * Set the exception handle for this state instance. Will also set the current state to error state.
     *
     * @param state - Error State to set.
     * @param error - Exception handle.
     */
    public void setError(EProcessorResponse state, Throwable error) {
        if (state == EProcessorResponse.ContinueWithError ||
                state == EProcessorResponse.FatalError ||
                state == EProcessorResponse.StopWithError ||
                state == EProcessorResponse.UnhandledError) {
            setState(state);
            this.error = error;
        }
        super.setError(error);
    }

    /**
     * Check if the state is in Error State.
     *
     * @return - In error state?
     */
    @Override
    public boolean hasError() {
        if (getState() == EProcessorResponse.ContinueWithError ||
                getState() == EProcessorResponse.FatalError ||
                getState() == EProcessorResponse.StopWithError ||
                getState() == EProcessorResponse.UnhandledError) {
            return true;
        }
        return false;
    }

    /**
     * Get the data handle for this response.
     *
     * @return - Data handle.
     */
    public T getData() {
        return data;
    }

    /**
     * Set the data handle for this state.
     *
     * @param data - Data handle.
     */
    public void setData(T data) {
        this.data = data;
    }
}
