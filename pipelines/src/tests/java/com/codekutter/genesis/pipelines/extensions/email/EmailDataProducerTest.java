package com.codekutter.genesis.pipelines.extensions.email;
import com.codekutter.zconfig.common.LogUtils;
import com.google.common.base.Strings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.mail.Message;
import javax.mail.Multipart;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EmailDataProducerTest {
    private static final String EMAIL_SERVER = "imap.gmail.com";
    private static final int EMAIL_SERVER_PORT = 993;
    private static final String USERNAME = "demo.codekutter@gmail.com";
    private static final String PASSWORD = "wh0c@res";

    private static EmailDataProducer dataProducer = new EmailDataProducer();

    @BeforeAll
    public static void init() throws Exception {
        dataProducer.setServer(EMAIL_SERVER);
        dataProducer.setPort(EMAIL_SERVER_PORT);
        dataProducer.setUseSSL(true);
        dataProducer.setUsername(USERNAME);
        dataProducer.setPassword(PASSWORD);
    }

    @Test
    void fetch() {
        try {
            String format = dataProducer.getDateFormat();
            DateFormat fmt = new SimpleDateFormat(format);

            String query =
                    String.format(
                            "receivedDate between {\"%s\", \"%s\"}",
                            fmt.format(new Date(0)),
                            fmt.format(new Date()));

            List<Message> messages = dataProducer.fetch(query, null);
            assertNotNull(messages);
            for (Message message : messages) {
                String mid = EmailDataProducer.getMessageId(message);
                assertFalse(Strings.isNullOrEmpty(mid));
                LogUtils.debug(getClass(), String.format("[type=%s][ID=%s]",
                                                         message.getClass()
                                                                .getCanonicalName(),
                                                         mid));
                Object content = message.getContent();
                if (content != null) {
                    if (content instanceof String) {
                        LogUtils.debug(getClass(), String.format("Message: {%s}",
                                                                 (String) content));
                    } else if (content instanceof Multipart) {
                        List<InputStream> streams =
                                EmailDataProducer.extractAttachments(message);
                        if (streams != null)
                            assertFalse(streams.isEmpty());
                        else {
                            String html = EmailDataProducer.getHtmlContent(message);
                            assertFalse(Strings.isNullOrEmpty(html));
                            LogUtils.debug(getClass(),
                                           String.format("Message HTML: {%s}",
                                                         html));
                        }
                    }
                }
            }
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex.getLocalizedMessage());
        }
    }

    @Test
    void delete() {
        try {

        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex.getLocalizedMessage());
        }
    }

    @Test
    void markAsRead() {
        try {

        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex.getLocalizedMessage());
        }
    }

    @Test
    void markAsUnRead() {
        try {

        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex.getLocalizedMessage());
        }
    }

    @Test
    void answered() {
        try {

        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex.getLocalizedMessage());
        }
    }
}