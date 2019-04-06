package com.codekutter.genesis.pipelines;

import com.codekutter.zconfig.common.LogUtils;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class to process a set of entity data.
 *
 * @param <T> - Entity Type.
 */
public abstract class CollectionProcessor<T> extends Processor<List<T>> {
    private boolean includedFiltered = true;

    /**
     * Include records that were filtered in the returned result set.
     *
     * @return - Include Filtered?
     */
    public boolean isIncludedFiltered() {
        return includedFiltered;
    }

    /**
     * Include records that were filtered in the returned result set.
     *
     * @param includedFiltered - Include Filtered?
     */
    public void setIncludedFiltered(boolean includedFiltered) {
        this.includedFiltered = includedFiltered;
    }

    /**
     * Filter the input data set based on the passed condition.
     *
     * @param data      - Input Data set.
     * @param condition - Filter condition.
     * @return - Filtered Result Set.
     */
    private List<T> filter(List<T> data, String condition) {

        return data;
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
    @Override
    public ProcessorResponse<List<T>> execute(@Nonnull List<T> data,
                                              String condition, Context context)
    throws ProcessorException {
        isAvailable();

        ProcessorResponse<List<T>> response = new ProcessorResponse<>();
        response.setState(EProcessorResponse.Unknown);
        response.setData(data);
        try {
            List<T> filtered = filter(data, condition);
            if (filtered == null || filtered.isEmpty()) {
                response.setState(EProcessorResponse.Skipped);
                if (includedFiltered) {
                    response.data = data;
                } else {
                    response.data = null;
                    response.setState(EProcessorResponse.NullData);
                }
            } else {
                List<T> removed = new ArrayList<>();
                for (T d : data) {
                    if (!filtered.contains(d)) {
                        removed.add(d);
                    }
                }

                ProcessorResponse<List<T>> r = execute(data, context, response);
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
                    if (includedFiltered) {
                        if (r.data == null) {
                            r.data = removed;
                        } else {
                            r.data.addAll(removed);
                        }
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
     * @param data     - List of Entity Object.
     * @param context  - Context Handle
     * @param response - Processor Response.
     * @return - Processor Response.
     */
    protected abstract ProcessorResponse<List<T>> execute(@Nonnull List<T> data,
                                                          Context context,
                                                          @Nonnull ProcessorResponse<List<T>> response);
}
