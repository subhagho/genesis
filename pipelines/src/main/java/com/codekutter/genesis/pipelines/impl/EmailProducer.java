package com.codekutter.genesis.pipelines.impl;

import com.codekutter.genesis.pipelines.extensions.email.EmailDataProducer;
import com.codekutter.genesis.pipelines.types.CollectionDataProducer;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.LogUtils;
import com.codekutter.zconfig.common.model.annotations.MethodInvoke;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;

import javax.mail.Message;

/**
 * Producer pipeline to read and process emails.
 */
public class EmailProducer extends CollectionDataProducer<Message> {

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
        super.init(node);
        try {
            producer = ConfigurationAnnotationProcessor
                    .readConfigAnnotations(EmailDataProducer.class,
                                           (ConfigPathNode) node);
        } catch (Exception ex) {
            state.setError(ex);
            LogUtils.debug(getClass(), ex);
            throw new ConfigurationException(ex);
        }
    }
}
