package com.codekutter.genesis.pipelines.extensions.email;

import com.codekutter.genesis.pipelines.Context;
import com.codekutter.genesis.pipelines.extensions.DataServiceException;
import com.codekutter.genesis.pipelines.extensions.EEmailOperations;
import com.codekutter.genesis.pipelines.extensions.IDataConsumer;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.GlobalConstants;
import com.codekutter.zconfig.common.LogUtils;
import com.codekutter.zconfig.common.model.annotations.*;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import lombok.Data;

import javax.mail.Message;
import javax.mail.Transport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Data Consumer implementation for handling Email messages.
 */
@ConfigPath(path = "emailDataConsumer")
@Data
public class EmailDataConsumer implements IDataConsumer<Message, EEmailOperations> {
    @ConfigAttribute(name = "server", required = true)
    private String server;
    @ConfigAttribute(name = "port", required = false)
    private int port;
    @ConfigValue(name = "username", required = true)
    private String username;
    @ConfigValue(name = "password", required = true)
    private String password;
    private boolean initialized = false;
    private Properties properties = new Properties();
    private EmailDataProducer producer;

    /**
     * Initialize this Consumer instance.
     *
     * @throws DataServiceException
     */
    private synchronized void init() throws DataServiceException {
        if (initialized)
            return;
        properties.put("mail.smtp.auth", true);
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.smtp.host", server);
        properties.put("mail.smtp.port", port);
        properties.put("mail.smtp.ssl.trust", server);

        initialized = true;
    }

    /**
     * Initialize the producer instance based on the
     * configuration node.
     *
     * @param config - Configuration node.
     * @throws ConfigurationException
     */
    @MethodInvoke()
    public void initProducer(
            @ConfigParam(name = GlobalConstants.DEFAULT_CONFIG_PARAM_NAME)
                    AbstractConfigNode config) throws
                                               ConfigurationException {
        Preconditions.checkArgument(
                config != null && (config instanceof ConfigPathNode));
        LogUtils.debug(getClass(),
                       String.format("Initializing email producer. [config=%s]",
                                     config.getSearchPath()));
        producer = ConfigurationAnnotationProcessor
                .readConfigAnnotations(EmailDataProducer.class,
                                       (ConfigPathNode) config);
    }

    @Override
    public void close() throws IOException {

    }

    /**
     * Process an entity instance.
     *
     * @param data      - Entity instance
     * @param operation - Operation to be performed.
     * @param context   - Context Handle.
     * @return - Modified Entity.
     * @throws DataServiceException
     */
    @Override
    public Message process(Message data, EEmailOperations operation,
                           Context context)
    throws DataServiceException {
        Preconditions.checkArgument(data != null);
        Preconditions.checkArgument(
                operation != null && operation != EEmailOperations.Receive);
        if (!initialized) {
            init();
        }

        switch (operation) {
            case Send:
                return send(data);
            case Delete:
                return delete(data);
            case Answered:
                return answered(data);
            case MarkAsRead:
                return markAsRead(data);
            case MarkAsUnread:
                return markAsUnread(data);
        }
        return null;
    }

    /**
     * Process the list of entities passed.
     *
     * @param dataSet   - List of Entities
     * @param operation - Operation to be performed.
     * @param context   - Context Handle.
     * @return - Updated List of Entities.
     * @throws DataServiceException
     */
    @Override
    public List<Message> process(List<Message> dataSet, EEmailOperations operation,
                                 Context context) throws DataServiceException {
        Preconditions.checkArgument(dataSet != null && !dataSet.isEmpty());
        Preconditions.checkArgument(operation != null);
        List<Message> result = new ArrayList<>();
        for (Message email : dataSet) {
            Message e = process(email, operation, context);
            if (e != null) {
                result.add(e);
            }
        }
        return result;
    }

    /**
     * Send the message.
     *
     * @param email - Message to send.
     * @return - Message handle.
     * @throws DataServiceException
     */
    private Message send(Message email) throws DataServiceException {
        try {
            Transport.send(email);
            return email;
        } catch (Exception ex) {
            LogUtils.debug(getClass(), ex);
            throw new DataServiceException(ex);
        }
    }

    /**
     * Mark the message as read on the server.
     *
     * @param email - Message handle
     * @return - Message handle.
     * @throws DataServiceException
     */
    private Message markAsRead(Message email) throws DataServiceException {
        return producer.markAsRead(email);
    }

    /**
     * Mark the message as unread on the server.
     *
     * @param email - Message handle
     * @return - Message handle.
     * @throws DataServiceException
     */
    private Message markAsUnread(Message email) throws DataServiceException {
        return producer.markAsUnRead(email);
    }

    /**
     * Mark the message as deleted on the server.
     *
     * @param email - Message handle
     * @return - Message handle.
     * @throws DataServiceException
     */
    private Message delete(Message email) throws DataServiceException {
        return producer.delete(email);
    }

    /**
     * Mark the message as answered on the server.
     *
     * @param email - Message handle
     * @return - Message handle.
     * @throws DataServiceException
     */
    private Message answered(Message email) throws DataServiceException {
        return producer.answered(email);
    }
}
