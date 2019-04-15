package com.codekutter.genesis.pipelines.extensions.email;

import com.codekutter.genesis.pipelines.Context;
import com.codekutter.genesis.pipelines.extensions.DataServiceException;
import com.codekutter.genesis.pipelines.extensions.IDataProducer;
import com.codekutter.zconfig.common.ConfigurationAnnotationProcessor;
import com.codekutter.zconfig.common.LogUtils;
import com.codekutter.zconfig.common.model.annotations.*;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.OpAnd;
import org.springframework.expression.spel.ast.OpOr;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import javax.mail.*;
import javax.mail.search.AndTerm;
import javax.mail.search.FlagTerm;
import javax.mail.search.OrTerm;
import javax.mail.search.SearchTerm;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Data producer implementation for getting Emails
 * from a mail server. (IMAP protocol required).
 */
@ConfigPath(path = "emailDataProducer")
public class EmailDataProducer implements IDataProducer<Message> {
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
    private EmailQueryParser queryParser = new EmailQueryParser();

    private boolean initialized = false;
    private Properties properties = new Properties();
    private Session session;
    private Store messageStore;
    private Folder currentFolder;

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Check and initialize this producer instance.
     *
     * @throws DataServiceException
     */
    @MethodInvoke
    private synchronized void init(
            @ConfigParam(name = "config") AbstractConfigNode config)
    throws DataServiceException {
        if (initialized)
            return;
        try {
            if (Strings.isNullOrEmpty(server)) {
                throw new DataServiceException("IMAP Server Host not set.");
            }
            if (!useSSL && port == DEFAULT_SSL_PORT) {
                port = DEFAULT_IMAP_PORT;
            }
            // server setting
            // properties.put("mail.debug", "true");
            properties.put("mail.store.protocol", "imaps");
            properties.put("mail.imaps.host", server);
            properties.put("mail.imaps.port", String.valueOf(port));
            properties.put("mail.imaps.timeout", "10000");


            if (useSSL) {
                // SSL setting
                properties.setProperty("mail.imap.socketFactory.class",
                                       "javax.net.ssl.SSLSocketFactory");
                properties.setProperty("mail.imap.socketFactory.fallback", "false");
                properties.setProperty("mail.imap.socketFactory.port",
                                       String.valueOf(port));
            }

            session = Session.getInstance(properties);
            messageStore = session.getStore("imaps");
            if (messageStore == null) {
                throw new DataServiceException(
                        "Error getting store handle for protocol. [protocol=imaps]");
            }
            messageStore.connect(username, password);
            currentFolder = messageStore.getFolder(folder);
            if (currentFolder == null) {
                throw new DataServiceException(
                        String.format("Error getting IMAP folder. [folder=%s]",
                                      folder));
            }
            currentFolder.open(Folder.READ_WRITE);
            if (config != null) {
                queryParser = ConfigurationAnnotationProcessor
                        .readConfigAnnotations(queryParser.getClass(),
                                               (ConfigPathNode) config,
                                               queryParser);
            }

            initialized = true;
        } catch (Exception ex) {
            throw new DataServiceException(ex);
        }
    }

    private void init() throws DataServiceException {
        init(null);
    }

    @Override
    public void close() throws IOException {
        try {
            if (messageStore != null && messageStore.isConnected()) {
                messageStore.close();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
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
        if (!initialized || !messageStore.isConnected()) {
            init();
        }
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
            if (!initialized || !messageStore.isConnected()) {
                init();
            }
            message.setFlag(Flags.Flag.DELETED, true);
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
            if (!initialized || !messageStore.isConnected()) {
                init();
            }
            message.setFlag(Flags.Flag.SEEN, true);
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
            if (!initialized || !messageStore.isConnected()) {
                init();
            }
            message.setFlag(Flags.Flag.SEEN, false);
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
            if (!initialized || !messageStore.isConnected()) {
                init();
            }
            message.setFlag(Flags.Flag.ANSWERED, true);
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
            if (!initialized || !messageStore.isConnected()) {
                init();
            }
            // Fetch unseen messages from inbox folder
            Message[] messages = currentFolder.search(
                    new FlagTerm(new Flags(Flags.Flag.SEEN), false));

            /* Use a suitable FetchProfile    */
            FetchProfile fp = new FetchProfile();
            fp.add(FetchProfile.Item.ENVELOPE);
            fp.add(FetchProfile.Item.CONTENT_INFO);
            currentFolder.fetch(messages, fp);

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
        try {
            SearchTerm searchTerm = queryParser.parse(query);
            // Fetch unseen messages from inbox folder
            Message[] messages = currentFolder.search(searchTerm);

            /* Use a suitable FetchProfile    */
            FetchProfile fp = new FetchProfile();
            fp.add(FetchProfile.Item.ENVELOPE);
            fp.add(FetchProfile.Item.CONTENT_INFO);
            currentFolder.fetch(messages, fp);

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
        } catch (Exception ex) {
            throw new DataServiceException(ex);
        }
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
                if (!result.isEmpty())
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
        if (!result.isEmpty()) {
            return result;
        }
        return null;
    }

    private static final Pattern HTML_CONTENT_MATCH = Pattern
            .compile(Pattern.quote("text/html"),
                     Pattern.CASE_INSENSITIVE);

    public static String getHtmlContent(Message message) throws Exception {
        Object content = message.getContent();
        if (content instanceof Multipart) {
            Multipart mp = (Multipart) content;
            for (int i = 0; i < mp.getCount(); i++) {
                BodyPart bp = mp.getBodyPart(i);
                if (HTML_CONTENT_MATCH.matcher(bp.getContentType()).find()) {
                    // found html part
                    return (String) bp.getContent();
                } else {
                    // some other bodypart...
                }
            }
        }
        return null;
    }
}
