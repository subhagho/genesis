package com.codekutter.genesis.pipelines;

import com.codekutter.zconfig.common.IStateEnum;

/**
 * Enumeration to define the states of a process instance.
 */
public enum EProcessState implements IStateEnum<EProcessState> {
    /**
     * BasicProcessor State is Unknown
     */
    Unknown,
    /**
     * BasicProcessor has been initialized.
     */
    Initialized,
    /**
     * BasicProcessor instance is available
     */
    Available,
    /**
     * BasicProcessor instance is not available due to errors
     */
    Error,
    /**
     * BasicProcessor instance has been disposed.
     */
    Disposed;


    /**
     * Get the state that represents an error state.
     *
     * @return - Error state.
     */
    @Override
    public EProcessState getErrorState() {
        return Error;
    }
}
