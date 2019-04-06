package com.codekutter.genesis.pipelines;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * A Context handle that can be passed around within an execution context.
 *
 * Note: Instance is not thread safe, hence should not be shared.
 */
public class Context {
    private Map<String, Object> parameters = new HashMap<>();

    /**
     * Get a map of all the parameters defined.
     *
     * @return - Map of Parameters.
     */
    public Map<String, Object> getParameters() {
        return parameters;
    }

    /**
     * Get the defined parameter with the specified name.
     *
     * @param name - Parameter name
     * @return - Value, if found.
     */
    public Object getParameter(String name) {
        return parameters.get(name);
    }

    /**
     * Add a parameter to this context instance.
     *
     * @param name - Parameter name.
     * @param value - Parameter Value.
     * @return - Self.
     */
    public Context addParameter(@Nonnull String name, Object value) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        parameters.put(name, value);
        return this;
    }

    /**
     * Check if this instance contains a parameter with the specified name.
     *
     * @param name - Parameter name.
     * @return - Parameter present?
     */
    public boolean hasParameter(String name) {
        return parameters.containsKey(name);
    }

    /**
     * Check if this context is empty.
     *
     * @return - Is Empty?
     */
    public boolean isEmpty() {
        return parameters.isEmpty();
    }
}
