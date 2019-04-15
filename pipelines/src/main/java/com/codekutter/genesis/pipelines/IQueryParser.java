package com.codekutter.genesis.pipelines;

public interface IQueryParser<T> {
    /**
     * Exception instance used to raise error in query parsing.
     */
    public class QueryParseException extends Exception {
        private static final String PREFIX = "Data Service Error : %s";

        /**
         * Exception constructor with error message string.
         *
         * @param s - Error message string.
         */
        public QueryParseException(String s) {
            super(String.format(PREFIX, s));
        }

        /**
         * Exception constructor with error message string and inner cause.
         *
         * @param s         - Error message string.
         * @param throwable - Inner cause.
         */
        public QueryParseException(String s, Throwable throwable) {
            super(String.format(PREFIX, s), throwable);
        }

        /**
         * Exception constructor inner cause.
         *
         * @param throwable - Inner cause.
         */
        public QueryParseException(Throwable throwable) {
            super(String.format(PREFIX, throwable.getLocalizedMessage()),
                  throwable);
        }
    }

    T parse(String query) throws QueryParseException;
}
