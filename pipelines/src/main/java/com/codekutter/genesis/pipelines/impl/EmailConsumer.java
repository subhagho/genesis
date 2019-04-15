package com.codekutter.genesis.pipelines.impl;

import com.codekutter.genesis.pipelines.*;
import com.codekutter.genesis.pipelines.extensions.EEmailOperations;
import com.codekutter.genesis.pipelines.extensions.email.EmailDataConsumer;
import com.codekutter.genesis.pipelines.types.DataConsumer;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.LogUtils;
import com.codekutter.zconfig.common.model.annotations.MethodInvoke;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.mail.Message;

/**
 * Email consumer processor implementation - Used for sending/updating emails.
 */
public class EmailConsumer extends DataConsumer<Message, EEmailOperations> {

    /**
     * Parse the operation type based on the input context object.
     *
     * @param operation - Operation context object.
     * @return - Operation type.
     * @throws ProcessorException
     */
    @Override
    public EEmailOperations parseOperation(Object operation)
    throws ProcessorException {
        Preconditions
                .checkArgument(operation != null && (operation instanceof Strings));
        return EEmailOperations.valueOf((String) operation);
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
    @MethodInvoke
    public void init(AbstractConfigNode node) throws ConfigurationException {
        Preconditions
                .checkArgument(node != null && (node instanceof ConfigPathNode));
        try {
            consumer = ConfigurationAnnotationProcessor
                    .readConfigAnnotations(EmailDataConsumer.class,
                                           (ConfigPathNode) node);
            state.setState(EProcessState.Available);
        } catch (Exception ex) {
            state.setError(ex);
            LogUtils.debug(getType(), ex);
            throw new ConfigurationException(ex);
        }
    }
}
