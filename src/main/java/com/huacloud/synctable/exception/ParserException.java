package com.huacloud.synctable.exception;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/25/2019 5:54 PM
 */
public class ParserException extends RuntimeException {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -724913199583039157L;
    private final String      sql;
    private int               position;
    private int               line;
    private int               column;

    public ParserException(String sql) {
        this(sql, null);
    }

    public ParserException(String sql, String message) {
        this(sql, message, null);
    }

    public ParserException(String sql, String message, Throwable cause) {
        super(message, cause);
        this.sql = sql;
    }

    /**
     * The zero-based position within the SQL string at which an exception was thrown, if applicable.
     */
    public final int position() {
        return position;
    }

    /**
     * Set the {@link #position()}.
     */
    public final ParserException position(int p) {
        this.position = p;
        return this;
    }

    /**
     * The one-based line number within the SQL string at which an exception was thrown, if applicable.
     */
    public final int line() {
        return line;
    }

    /**
     * Set the {@link #line()}.
     */
    public final ParserException line(int l) {
        this.line = l;
        return this;
    }

    /**
     * The one-based column number within the SQL string at which an exception was thrown, if applicable.
     */
    public final int column() {
        return column;
    }

    /**
     * Set the {@link #column()}.
     */
    public final ParserException column(int c) {
        this.column = c;
        return this;
    }

    /**
     * The SQL string that caused the exception.
     */
    public final String sql() {
        return sql;
    }

}
