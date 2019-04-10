package com.codekutter.genesis.pipelines.extensions;

import com.codekutter.genesis.pipelines.Context;
import com.codekutter.zconfig.common.LogUtils;
import com.codekutter.zconfig.common.model.annotations.ConfigAttribute;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import javax.mail.*;
import javax.mail.search.FlagTerm;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Data producer implementation for getting Emails
 * from a mail server. (IMAP protocol required).
 */
@Data
@ConfigPath(path = "emailDataProducer")
public class EmailDataProducer implements DataProducer<Message> {
    private static final int DEFAULT_SSL_PORT = 993;
    private static final int DEFAULT_IMAP_PORT = 143;
    private static final String DEFAULT_IMAP_FOLDER = "INBOX";

    @ConfigAttribute(name = "server", required = true)
    private String server;
    @ConfigAttribute(name = "port", required = false)
    private int port = DEFAULT_SSL_PORT;
    @ConfigAttribute(name = "useSSL", required = false)
    private boolean useSSL = true;
    @ConfigValue(name = "username")
    private String username;
    @ConfigValue(name = "password")
    private String password;
    @ConfigAttribute(name = "folder")
    private String folder = DEFAULT_IMAP_FOLDER;

    private boolean initialized = false;
    private Properties properties = new Properties();

    /**
     * Check and initialize this producer instance.
     *
     * @throws DataServiceException
     */
    private synchronized void init() throws DataServiceException {
        if (initialized)
            return;
        if (Strings.isNullOrEmpty(server)) {
            throw new DataServiceException("IMAP Server Host not set.");
        }
        if (!useSSL && port == DEFAULT_SSL_PORT) {
            port = DEFAULT_IMAP_PORT;
        }
        // server setting
        properties.put("mail.imap.host", server);
        properties.put("mail.imap.port", port);

        if (useSSL) {
            // SSL setting
            properties.setProperty("mail.imap.socketFactory.class",
                                   "javax.net.ssl.SSLSocketFactory");
            properties.setProperty("mail.imap.socketFactory.fallback", "false");
            properties.setProperty("mail.imap.socketFactory.port",
                                   String.valueOf(port));
        }
        initialized = true;
    }

    /**
     * Fetch a data set based on the passed query.
     *
     * @param query   - Query Condition.
     * @param context - Context Handle.
     * @return - Fetched entities.
     * @throws DataServiceException
     */
    @Override
    public List<Message> fetch(String query, Context context)
    throws DataServiceException {
        if (!initialized)
            init();
        if (Strings.isNullOrEmpty(query)) {
            return fetchUnread();
        } else {
            return searchEmails(query, context);
        }
    }

    /**
     * Mark the message as deleted on the server.
     *
     * @param message - Message to delete
     * @return - Message handle.
     * @throws DataServiceException
     */
    public Message delete(Message message) throws DataServiceException {
        Preconditions.checkArgument(message != null);
        try {
            Session session = Session.getDefaultInstance(properties);
            Store imapStore = session.getStore("imaps");
            if (imapStore == null) {
                throw new DataServiceException(
                        "Error getting store handle for protocol. [protocol=imaps]");
            }
            try {
                imapStore.connect(username, password);
                Folder imapFolder = imapStore.getFolder(folder);
                if (imapFolder == null) {
                    throw new DataServiceException(
                            String.format("Error getting IMAP folder. [folder=%s]",
                                          folder));
                }
                imapFolder.open(Folder.READ_WRITE);
                try {
                    message.setFlag(Flags.Flag.DELETED, true);
                } finally {
                    imapFolder.close(true);
                }
            } finally {
                imapStore.close();
            }
            return message;
        } catch (Exception e) {
            LogUtils.debug(getClass(), e);
            throw new DataServiceException(e);
        }
    }

    /**
     * Mark the message as read on the server.
     *
     * @param message - Message handle
     * @return - Message handle.
     * @throws DataServiceException
     */
    public Message markAsRead(Message message) throws DataServiceException {
        Preconditions.checkArgument(message != null);
        try {
            Session session = Session.getDefaultInstance(properties);
            Store imapStore = session.getStore("imaps");
            if (imapStore == null) {
                throw new DataServiceException(
                        "Error getting store handle for protocol. [protocol=imaps]");
            }
            try {
                imapStore.connect(username, password);
                Folder imapFolder = imapStore.getFolder(folder);
                if (imapFolder == null) {
                    throw new DataServiceException(
                            String.format("Error getting IMAP folder. [folder=%s]",
                                          folder));
                }
                imapFolder.open(Folder.READ_WRITE);
                try {
                    message.setFlag(Flags.Flag.SEEN, true);
                } finally {
                    imapFolder.close(false);
                }
            } finally {
                imapStore.close();
            }
            return message;
        } catch (Exception e) {
            LogUtils.debug(getClass(), e);
            throw new DataServiceException(e);
        }
    }

    /**
     * Mark the message as unread on the server.
     *
     * @param message - Message handle
     * @return - Message handle.
     * @throws DataServiceException
     */
    public Message markAsUnRead(Message message) throws DataServiceException {
        Preconditions.checkArgument(message != null);
        try {
            Session session = Session.getDefaultInstance(properties);
            Store imapStore = session.getStore("imaps");
            if (imapStore == null) {
                throw new DataServiceException(
                        "Error getting store handle for protocol. [protocol=imaps]");
            }
            try {
                imapStore.connect(username, password);
                Folder imapFolder = imapStore.getFolder(folder);
                if (imapFolder == null) {
                    throw new DataServiceException(
                            String.format("Error getting IMAP folder. [folder=%s]",
                                          folder));
                }
                imapFolder.open(Folder.READ_WRITE);
                try {
                    message.setFlag(Flags.Flag.SEEN, false);
                } finally {
                    imapFolder.close(false);
                }
            } finally {
                imapStore.close();
            }
            return message;
        } catch (Exception e) {
            LogUtils.debug(getClass(), e);
            throw new DataServiceException(e);
        }
    }

    /**
     * Mark the message as answered on the server.
     *
     * @param message - Message handle
     * @return - Message handle.
     * @throws DataServiceException
     */
    public Message answered(Message message) throws DataServiceException {
        Preconditions.checkArgument(message != null);
        try {
            Session session = Session.getDefaultInstance(properties);
            Store imapStore = session.getStore("imaps");
            if (imapStore == null) {
                throw new DataServiceException(
                        "Error getting store handle for protocol. [protocol=imaps]");
            }
            try {
                imapStore.connect(username, password);
                Folder imapFolder = imapStore.getFolder(folder);
                if (imapFolder == null) {
                    throw new DataServiceException(
                            String.format("Error getting IMAP folder. [folder=%s]",
                                          folder));
                }
                imapFolder.open(Folder.READ_WRITE);
                try {
                    message.setFlag(Flags.Flag.ANSWERED, true);
                } finally {
                    imapFolder.close(false);
                }
            } finally {
                imapStore.close();
            }
            return message;
        } catch (Exception e) {
            LogUtils.debug(getClass(), e);
            throw new DataServiceException(e);
        }
    }

    /**
     * Fetch all the unread emails on the server.
     *
     * @return - List of fetched messages.
     * @throws DataServiceException
     */
    private List<Message> fetchUnread() throws DataServiceException {
        try {
            Session session = Session.getDefaultInstance(properties);
            Store imapStore = session.getStore("imaps");
            if (imapStore == null) {
                throw new DataServiceException(
                        "Error getting store handle for protocol. [protocol=imaps]");
            }
            try {
                imapStore.connect(username, password);
                Folder imapFolder = imapStore.getFolder(folder);
                if (imapFolder == null) {
                    throw new DataServiceException(
                            String.format("Error getting IMAP folder. [folder=%s]",
                                          folder));
                }
                imapFolder.open(Folder.READ_ONLY);
                try {
                    // Fetch unseen messages from inbox folder
                    Message[] messages = imapFolder.search(
                            new FlagTerm(new Flags(Flags.Flag.SEEN), false));
                    if (messages != null && messages.length > 0) {
                        // Sort messages from recent to oldest
                        Arrays.sort(messages, (m1, m2) -> {
                            try {
                                return m2.getSentDate().compareTo(m1.getSentDate());
                            } catch (MessagingException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        return Arrays.asList(messages);
                    }
                } finally {
                    imapFolder.close(false);
                }
            } finally {
                imapStore.close();
            }

        } catch (Exception e) {
            LogUtils.debug(getClass(), e);
            throw new DataServiceException(e);
        }
        return null;
    }

    /**
     * Fetch all emails that match the search condition. (currently not supported)
     * <p>
     * TODO: Implement Search.
     *
     * @param query   - Search Condition.
     * @param context - Context handle.
     * @return - List of fetched messages.
     * @throws DataServiceException
     */
    private List<Message> searchEmails(String query, Context context)
    throws DataServiceException {
        throw new DataServiceException("Search not implemented.");
    }

    /**
     * Check if this message has attachments.
     *
     * @param message - Email message.
     * @return - Has Attachments?
     * @throws DataServiceException
     */
    public static boolean hasAttachments(Message message)
    throws DataServiceException {
        if (message != null) {
            try {
                Object content = message.getContent();
                if (content instanceof Multipart) {
                    return true;
                }
            } catch (Exception ex) {
                LogUtils.debug(EmailDataProducer.class, ex);
                throw new DataServiceException(ex);
            }
        }
        return false;
    }

    /**
     * Extract the email attachments for the passed message.
     *
     * @param message - Email message.
     * @return - List of Input Streams for the attachments.
     * @throws Exception
     */
    public static List<InputStream> extractAttachments(Message message)
    throws DataServiceException {
        Preconditions.checkArgument(message != null);
        try {
            Object content = message.getContent();
            if (content instanceof String)
                return null;

            if (content instanceof Multipart) {
                Multipart multipart = (Multipart) content;
                List<InputStream> result = new ArrayList<>();

                for (int i = 0; i < multipart.getCount(); i++) {
                    result.addAll(getAttachments(multipart.getBodyPart(i)));
                }
                return result;

            }
            return null;
        } catch (Exception ex) {
            LogUtils.debug(EmailDataProducer.class, ex);
            throw new DataServiceException(ex);
        }
    }

    private static List<InputStream> getAttachments(BodyPart part)
    throws Exception {
        List<InputStream> result = new ArrayList<InputStream>();
        Object content = part.getContent();
        if (content instanceof InputStream || content instanceof String) {
            if (Part.ATTACHMENT.equalsIgnoreCase(part.getDisposition()) ||
                    StringUtils
                            .isNotBlank(part.getFileName())) {
                result.add(part.getInputStream());
                return result;
            } else {
                return new ArrayList<InputStream>();
            }
        }

        if (content instanceof Multipart) {
            Multipart multipart = (Multipart) content;
            for (int i = 0; i < multipart.getCount(); i++) {
                BodyPart bodyPart = multipart.getBodyPart(i);
                result.addAll(getAttachments(bodyPart));
            }
        }
        return result;
    }
}
