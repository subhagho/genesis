package com.codekutter.genesis.pipelines.extensions.email;

import com.codekutter.zconfig.common.LogUtils;
import org.junit.jupiter.api.Test;

import javax.mail.search.SearchTerm;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class EmailQueryParserTest {

    @Test
    void parseBetweenQuery() {
        try {
            EmailQueryParser parser = new EmailQueryParser();
            DateFormat fmt = new SimpleDateFormat(parser.getDateFormat());

            String query =
                    String.format(
                            "receivedDate between {\"%s\", \"%s\"} and to == \"abc@def.com\"",
                            fmt.format(new Date(0)),
                            fmt.format(new Date()));
            SearchTerm term = parser.parse(query);
            assertNotNull(term);
            LogUtils.debug(getClass(), term);
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex.getLocalizedMessage());
        }
    }
}