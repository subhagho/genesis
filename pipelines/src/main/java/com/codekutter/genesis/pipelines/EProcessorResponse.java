package com.codekutter.genesis.pipelines;

import com.codekutter.zconfig.common.IStateEnum;

/**
 * Enumeration to define processor response states.
 */
public enum EProcessorResponse implements IStateEnum<EProcessorResponse> {
    /**
     * State is Unknown.
     */
    Unknown,
    /**
     * Everything went OK.
     */
    OK,
    /**
     * Fatal Error occurred.
     */
    FatalError,
    /**
     * Error occurred, but OK to continue.
     */
    ContinueWithError,
    /**
     * Error occurred, stop further processing.
     */
    StopWithError,
    /**
     * Everything OK, but stop further processing.
     */
    StopWithOk,
    /**
     * Unhandled Exception occurred.
     */
    UnhandledError,
    /**
     * Response has NULL data.
     */
    NullData,
    /**
     * BasicProcessor step was skipped due to defined condition.
     */
    Skipped;


    /**
     * Get the state that represents an error state.
     *
     * @return - Error state.
     */
    @Override
    public EProcessorResponse getErrorState() {
        return FatalError;
    }
}
