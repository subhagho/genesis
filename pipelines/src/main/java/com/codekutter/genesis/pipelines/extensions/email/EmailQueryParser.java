package com.codekutter.genesis.pipelines.extensions.email;

import com.codekutter.genesis.pipelines.IQueryParser;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Data;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.*;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import javax.annotation.Nonnull;
import javax.mail.Message;
import javax.mail.search.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Utility class to parse an input query as a email (JavaMail) Search Term.
 */
@Data
@ConfigPath(path = "emailQueryParser")
public class EmailQueryParser implements IQueryParser<SearchTerm> {
    public static final String DEFAULT_DATE_FORMAT = "MM-dd-yyyy HH:mm:ss z";

    /**
     * Date Format to use for parsing date strings.
     */
    @ConfigValue(name = "dateFormat")
    private String dateFormat;

    /**
     * Default Empty constructor.
     */
    public EmailQueryParser() {
        dateFormat = DEFAULT_DATE_FORMAT;
    }

    /**
     * Parse the passed query and return the JavaMail Search Term.
     *
     * @param query - Input Query String.
     * @return - Mail Search Term
     * @throws QueryParseException
     */
    @Override
    public SearchTerm parse(@Nonnull String query) throws QueryParseException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(query));

        try {
            ExpressionParser parser = new SpelExpressionParser();
            SpelExpression expression =
                    (SpelExpression) parser.parseExpression(query);
            if (expression != null) {
                SpelNode node = expression.getAST();
                return processSearchTerm(node);
            } else {
                throw new QueryParseException(
                        String.format("Error parsing query. [query=%s]", query));
            }
        } catch (Exception ex) {
            throw new QueryParseException(ex);
        }
    }

    /**
     * Process the parsed AST node.
     *
     * @param node - Parsed AST node.
     * @return - Processed Search Term.
     * @throws QueryParseException
     */
    private SearchTerm processSearchTerm(SpelNode node) throws QueryParseException {
        if (node instanceof OpAnd) {
            OpAnd operator = (OpAnd) node;
            SpelNode ln = operator.getChild(0);
            Preconditions.checkNotNull(ln);
            SearchTerm tl = processSearchTerm(ln);
            SpelNode lr = operator.getChild(1);
            Preconditions.checkNotNull(lr);
            SearchTerm tr = processSearchTerm(lr);
            return new AndTerm(tl, tr);
        } else if (node instanceof OpOr) {
            OpOr operator = (OpOr) node;
            SpelNode ln = operator.getChild(0);
            Preconditions.checkNotNull(ln);
            SearchTerm tl = processSearchTerm(ln);
            SpelNode lr = operator.getChild(1);
            Preconditions.checkNotNull(lr);
            SearchTerm tr = processSearchTerm(lr);
            return new OrTerm(tl, tr);
        } else if (node instanceof OperatorBetween) {
            return processBetweenOper((OperatorBetween) node);
        } else if (node instanceof OpEQ) {
            OpEQ operator = (OpEQ) node;
            SpelNode ln = operator.getChild(0);
            Preconditions.checkNotNull(ln);
            SpelNode lr = operator.getChild(1);
            Preconditions.checkNotNull(lr);
            return processCompare(ComparisonTerm.EQ, ln, lr);
        }
        throw new QueryParseException(
                String.format("Unsupported search construct: [type=%s]",
                              node.getClass().getCanonicalName()));
    }

    /**
     * Process the between condition.
     *
     * @param operator - Operator AST node.
     * @return - Processed Search Term
     * @throws QueryParseException
     */
    private SearchTerm processBetweenOper(OperatorBetween operator)
    throws QueryParseException {
        SpelNode ln = operator.getChild(0);
        Preconditions.checkNotNull(ln);
        SpelNode vn = operator.getChild(1);
        Preconditions.checkNotNull(vn);
        SpelNode vl = vn.getChild(0);
        Preconditions.checkNotNull(vl);
        SpelNode vu = vn.getChild(1);
        Preconditions.checkNotNull(vu);

        SearchTerm s1 = processCompare(ComparisonTerm.GE, ln, vl);
        Preconditions.checkNotNull(s1);
        SearchTerm s2 = processCompare(ComparisonTerm.LE, ln, vu);
        Preconditions.checkNotNull(s2);

        return new AndTerm(s1, s2);
    }

    /**
     * Process into a comparison term.
     *
     * @param comparison - Comparison Type.
     * @param node - Variable AST node.
     * @param value - Value AST node.
     * @return - Processed Search Term
     * @throws QueryParseException
     */
    private SearchTerm processCompare(int comparison, SpelNode node,
                                      SpelNode value)
    throws QueryParseException {
        if (node instanceof CompoundExpression) {
            PropertyOrFieldReference prefix =
                    (PropertyOrFieldReference) node.getChild(0);
            PropertyOrFieldReference field =
                    (PropertyOrFieldReference) node.getChild(1);
            return processCompare(comparison, prefix.getName(), field, value);
        } else if (node instanceof PropertyOrFieldReference) {
            PropertyOrFieldReference field =
                    (PropertyOrFieldReference) node;
            return processCompare(comparison, null, field, value);
        }
        throw new QueryParseException(
                String.format("Invalid SPeL node type. [type=%s]",
                              node.getClass().getCanonicalName()));
    }

    /**
     * Header Search Prefix - Used to prefix header search items
     */
    public static final String PREFIX_HEADER = "header";
    /**
     * Sent TO Search
     */
    public static final String SEARCH_TERM_TO = "to";
    /**
     * Sent to CC Search
     */
    public static final String SEARCH_TERM_CC = "cc";
    /**
     * Sent to BCC Search
     */
    public static final String SEARCH_TERM_BCC = "bcc";
    /**
     * Received From Search
     */
    public static final String SEARCH_TERM_FROM = "receivedFrom";
    /**
     * Subject Search
     */
    public static final String SEARCH_TERM_SUBJECT = "subject";
    /**
     * Email Body Search.
     */
    public static final String SEARCH_TERM_BODY = "body";
    /**
     * Search By Message ID.
     */
    public static final String SEARCH_TERM_ID = "messageId";
    /**
     * Received Date Search.
     */
    public static final String SEARCH_TERM_RECVD_DATE = "receivedDate";

    private SearchTerm processCompare(int comparison, String prefix,
                                      PropertyOrFieldReference field,
                                      SpelNode value)
    throws QueryParseException {
        String name = field.getName();
        Object v = processValue(value);
        try {
            if (!Strings.isNullOrEmpty(prefix)) {
                if (prefix.compareToIgnoreCase(PREFIX_HEADER) == 0) {
                    return new HeaderTerm(name, (String) v);
                }
            } else {
                if (name.compareToIgnoreCase(SEARCH_TERM_BODY) == 0) {
                    return new BodyTerm((String) v);
                } else if (name.compareToIgnoreCase(SEARCH_TERM_FROM) == 0) {
                    return new FromStringTerm((String) v);
                } else if (name.compareToIgnoreCase(SEARCH_TERM_ID) == 0) {
                    return new MessageIDTerm((String) v);
                } else if (name.compareToIgnoreCase(SEARCH_TERM_RECVD_DATE) == 0) {
                    SimpleDateFormat df = new SimpleDateFormat(dateFormat);
                    Date dt = df.parse((String) v);
                    return new ReceivedDateTerm(comparison, dt);
                } else if (name.compareToIgnoreCase(SEARCH_TERM_SUBJECT) == 0) {
                    return new SubjectTerm((String) v);
                } else if (name.compareToIgnoreCase(SEARCH_TERM_TO) == 0) {
                    return new RecipientStringTerm(Message.RecipientType.TO,
                                                   (String) v);
                } else if (name.compareToIgnoreCase(SEARCH_TERM_CC) == 0) {
                    return new RecipientStringTerm(Message.RecipientType.CC,
                                                   (String) v);
                } else if (name.compareToIgnoreCase(SEARCH_TERM_BCC) == 0) {
                    return new RecipientStringTerm(Message.RecipientType.BCC,
                                                   (String) v);
                }
            }
        } catch (Exception ex) {
            throw new QueryParseException(ex);
        }
        throw new QueryParseException(
                String.format("Invalid field spec. [prefix=%s][name=%s]", prefix,
                              name));
    }

    private Object processValue(SpelNode value) throws QueryParseException {
        if (value instanceof StringLiteral) {
            return ((StringLiteral) value).getLiteralValue().getValue();
        } else if (value instanceof RealLiteral) {
            return ((RealLiteral) value).getLiteralValue().getValue();
        } else if (value instanceof IntLiteral) {
            return ((IntLiteral) value).getLiteralValue().getValue();
        } else if (value instanceof LongLiteral) {
            return ((LongLiteral) value).getLiteralValue().getValue();
        } else if (value instanceof BooleanLiteral) {
            return ((BooleanLiteral) value).getLiteralValue().getValue();
        }
        throw new QueryParseException(
                String.format("Invalid value type. [type=%s]",
                              value.getClass().getCanonicalName()));
    }
}
