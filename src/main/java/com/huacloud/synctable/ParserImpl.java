package com.huacloud.synctable;

import com.huacloud.synctable.dialect.Dialect;
import com.huacloud.synctable.exception.ParserException;
import com.huacloud.synctable.mapping.*;
import com.huacloud.synctable.mapping.datatype.DataType;
import com.huacloud.synctable.mapping.datatype.SQLDataType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * SQL解析
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 4/25/2019 5:50 PM
 */
public class ParserImpl {

    private static Logger logger = LoggerFactory.getLogger(ParserImpl.class);

    public Table parseTable(ParserContext ctx) {
        parseKeyword(ctx, "CREATE");

        if (parseKeywordIf(ctx, "TABLE")) {
            return parseCreateTable(ctx);
        }
        return null;
    }

    public Table parseCreateTable(ParserContext ctx) {
        Table table = new Table();
        String tableName = parseName(ctx);
        if (StringUtils.contains(tableName, ".")) {
            String[] split = StringUtils.split(tableName, ".");
            table.setSchema(split[0]);
            table.setName(split[1]);

        } else {
            table.setName(tableName);
        }
        logger.info("tableName: {}", tableName);

        int position = ctx.position();
        List<Column> fields = new ArrayList<>();
        List<UniqueKey> constraints = new ArrayList<>();
        PrimaryKey primaryKey = null;
        boolean primary = false;

        parse(ctx, '(');

        do {
            UniqueKey constraint = null;
            if (parseKeywordIf(ctx, "CONSTRAINT")) {
                String identifier = parseIdentifier(ctx);
                constraint = new UniqueKey();
                constraint.setName(identifier);
                System.out.println("constraint name:" + identifier);
            }
            if (parseKeywordIf(ctx, "PRIMARY KEY")) {
                if (primary) {
                    throw ctx.exception("Duplicate primary key specification");
                }
                List<String> keyColumnNames = parseConstraintColumnNames(ctx);
                primary = true;

                primaryKey = new PrimaryKey();
                primaryKey.addAllColumnNames(keyColumnNames);

                continue;
            }
            else if (parseKeywordIf(ctx, "UNIQUE")) {
                if (!parseKeywordIf(ctx, "KEY")) {
                    parseKeywordIf(ctx, "INDEX");
                }
                List<String> uniqueKeyColumnNames = parseConstraintColumnNames(ctx);
                constraint.addAllColumnNames(uniqueKeyColumnNames);
                constraints.add(constraint);
                continue;
            }
            else if (parseKeywordIf(ctx, "FOREIGN KEY")) {
                //暂时不考虑外键
                logger.warn("暂不考虑外键信息");
                continue;
            }
            else if (parseKeywordIf(ctx, "CHECK")) {
                constraints.add(null);
                continue;
            }
            else if (constraints.size() == 0 &&
                    (parseKeywordIf(ctx, "KEY") ||
                            parseKeywordIf(ctx, "INDEX"))) {
                int p2 = ctx.position();

                // [#7348] [#7651] Look ahead if the next tokens indicate a MySQL index definition
                if (parseIf(ctx, '(') || (parseIdentifierIf(ctx) != null && parseIf(ctx, '('))) {
                    ctx.position(p2);
                    //indexes.add(parseIndexSpecification(ctx, tableName));
                    continue;
                }
                else {
                    ctx.position(position);
                }
            }
            Column column = new Column();
            String fieldName = parseIdentifier(ctx);
            column.setName(fieldName);
            logger.info("column name : {}", fieldName);
            DataType dataType = parseDataType(ctx);
            column.setDataType(dataType);
            String fieldComment = null;

            boolean nullable = false;
            boolean defaultValue = false;
            boolean onUpdate = false;
            boolean unique = false;
            boolean comment = false;

            for (;;) {
                if (!nullable) {
                    if (parseKeywordIf(ctx, "NULL")) {
                        column.setNullable(true);
                        nullable = true;
                        continue;
                    }
                    else if (parseKeywordIf(ctx, "NOT NULL")) {
                        column.setNullable(false);
                        nullable = true;
                        continue;
                    }
                }

                if (!defaultValue) {
                    if (parseKeywordIf(ctx, "IDENTITY")) {
                        if (parseIf(ctx, '(')) {
                            parseSignedInteger(ctx);
                            parse(ctx, ',');
                            parseSignedInteger(ctx);
                            parse(ctx, ')');
                        }

                        defaultValue = true;
                        continue;
                    }
                    else if (parseKeywordIf(ctx, "DEFAULT")) {

                        // TODO: Ignored keyword from Oracle
                        //parseKeywordIf(ctx, "ON NULL");
                        String defaultContent = parseStringLiteral(ctx);
                        column.setDefaultValue(defaultContent);

                        defaultValue = true;
                        continue;
                    }
                    else if (parseKeywordIf(ctx, "GENERATED")) {
                        if (!parseKeywordIf(ctx, "ALWAYS")) {
                            parseKeyword(ctx, "BY DEFAULT");

                            // TODO: Ignored keyword from Oracle
                            parseKeywordIf(ctx, "ON NULL");
                        }

                        parseKeyword(ctx, "AS IDENTITY");

                        // TODO: Ignored identity options from Oracle
                        if (parseIf(ctx, '(')) {
                            boolean identityOption = false;

                            for (;;) {
                                if (identityOption) {
                                    parseIf(ctx, ',');
                                }
                                if (parseKeywordIf(ctx, "START WITH")) {
                                    if (!parseKeywordIf(ctx, "LIMIT VALUE")) {
                                        parseUnsignedInteger(ctx);
                                    }
                                    identityOption = true;
                                    continue;
                                }
                                else if (parseKeywordIf(ctx, "INCREMENT BY")
                                        || parseKeywordIf(ctx, "MAXVALUE")
                                        || parseKeywordIf(ctx, "MINVALUE")
                                        || parseKeywordIf(ctx, "CACHE")) {
                                    parseUnsignedInteger(ctx);
                                    identityOption = true;
                                    continue;
                                }
                                else if (parseKeywordIf(ctx, "NOMAXVALUE")
                                        || parseKeywordIf(ctx, "NOMINVALUE")
                                        || parseKeywordIf(ctx, "CYCLE")
                                        || parseKeywordIf(ctx, "NOCYCLE")
                                        || parseKeywordIf(ctx, "NOCACHE")
                                        || parseKeywordIf(ctx, "ORDER")
                                        || parseKeywordIf(ctx, "NOORDER")) {
                                    identityOption = true;
                                    continue;
                                }

                                if (identityOption) {
                                    break;
                                }
                                else {
                                    throw ctx.unsupportedClause();
                                }
                            }

                            parse(ctx, ')');
                        }

                        defaultValue = true;
                        continue;
                    }
                }

                if (!onUpdate) {
                    if (parseKeywordIf(ctx, "ON UPDATE")) {
                        parseKeyword(ctx, "ON UPDATE");
                        continue;
                    }
                }

                if (!unique) {
                    if (parseKeywordIf(ctx, "PRIMARY KEY")) {
                        primaryKey = new PrimaryKey();
                        primaryKey.addColumn(column);
                        primary = true;
                        unique = true;
                        continue;
                    }
                    else if (parseKeywordIf(ctx, "UNIQUE")) {
                        if (!parseKeywordIf(ctx, "KEY")) {
                            parseKeywordIf(ctx, "INDEX");
                        }
                        UniqueKey uniqueKey = new UniqueKey();
                        uniqueKey.addColumn(column);
                        constraints.add(uniqueKey);
                        unique = true;
                        continue;
                    }
                }

                if (parseKeywordIf(ctx, "AUTO_INCREMENT") ||
                        parseKeywordIf(ctx, "AUTOINCREMENT")) {
                    continue;
                }

                if (!comment) {
                    if (parseKeywordIf(ctx, "COMMENT")) {
                        fieldComment = parseComment(ctx);
                        continue;
                    }
                }

                break;
            }

            column.setName(fieldName);
            column.setComment(fieldComment);
            fields.add(column);
        }
        while (parseIf(ctx, ','));

        if (fields.size() == 0) {
            throw ctx.expected("At least one column");
        }
        parse(ctx, ')');

        String tableComment;
        if (parseKeywordIf(ctx, "COMMENT")) {
            tableComment = parseComment(ctx);
            table.setComment(tableComment);
        }

        for (Column column : fields) {
            table.addColumn(column);
        }

        if (primaryKey != null) {
            primaryKey.setTable(table);
            List<String> columnNames = primaryKey.getColumnNames();
            for (String columnName : columnNames) {
                if (table.containColumn(columnName)) {

                    primaryKey.addColumn(table.getColumn(columnName));

                } else {
                    throw ctx.exception("列 <" + columnName + "> 不存在！");
                }
            }
        }
        table.setPrimaryKey(primaryKey);

        if (CollectionUtils.isNotEmpty(constraints)) {

            for (UniqueKey constraint : constraints) {

                List<String> columnNames = constraint.getColumnNames();
                for (String columnName : columnNames) {
                    if (table.containColumn(columnName)) {
                        constraint.addColumn(table.getColumn(columnName));
                    } else {
                        throw ctx.exception("列 <" + columnName + "> 不存在！");
                    }
                }
                constraint.setTable(table);
                table.addUniqueKey(constraint);
            }
        }
        return table;
    }

    public List<String> parseConstraintColumnNames(ParserContext ctx) {
        parse(ctx, '(');
        List<String> fieldNames = parseFieldNames(ctx);
        parse(ctx, ')');


        parseConstraintStateIf(ctx);
        return fieldNames;
    }

    public boolean parseConstraintStateIf(ParserContext ctx) {
        parseKeywordIf(ctx, "ENABLE");
        return true;
    }

    public List<String> parseFieldNames(ParserContext ctx) {
        List<String> result = new ArrayList<>();

        do {
            result.add(parseFieldName(ctx));
        }
        while (parseIf(ctx, ','));

        return result;
    }

    public String parseFieldName(ParserContext ctx) {
        return parseName(ctx);
    }

    public String parseComment(ParserContext ctx) {
        return parseStringLiteral(ctx);
    }

    public String parseStringLiteral(ParserContext ctx) {
        String result = parseStringLiteralIf(ctx);

        if (result == null) {
            throw ctx.expected("String literal");
        }

        return result;
    }

    public String parseStringLiteralIf(ParserContext ctx) {
        if (parseIf(ctx, 'q', '\'', false) ||
                parseIf(ctx, 'Q', '\'', false)) {
            return parseOracleQuotedStringLiteral(ctx);
        }
        else if (parseIf(ctx, 'e', '\'', false) ||
                parseIf(ctx, 'E', '\'', false)) {
            return parseUnquotedStringLiteral(ctx, true);
        }
        else if (peek(ctx, '\'')) {
            return parseUnquotedStringLiteral(ctx, false);
        }
        else {
            return null;
        }
    }

    public String parseUnquotedStringLiteral(ParserContext ctx, boolean postgresEscaping) {
        parse(ctx, '\'', false);

        StringBuilder sb = new StringBuilder();

        characterLoop:
        for (int i = ctx.position(); i < ctx.sql.length; i++) {
            char c1 = ctx.character(i);

            // TODO MySQL string escaping...
            switch (c1) {
                case '\\': {
                    if (!postgresEscaping)
                        break;

                    i++;
                    char c2 = ctx.character(i);
                    switch (c2) {

                        // Escaped whitespace characters
                        case 'b':
                            c1 = '\b';
                            break;
                        case 'n':
                            c1 = '\n';
                            break;
                        case 't':
                            c1 = '\t';
                            break;
                        case 'r':
                            c1 = '\r';
                            break;
                        case 'f':
                            c1 = '\f';
                            break;

                        // Hexadecimal byte value
                        case 'x': {
                            char c3 = ctx.character(i + 1);
                            char c4 = ctx.character(i + 2);

                            int d3;
                            if ((d3 = Character.digit(c3, 16)) != -1) {
                                i++;

                                int d4;
                                if ((d4 = Character.digit(c4, 16)) != -1) {
                                    c1 = (char) (0x10 * d3 + d4);
                                    i++;
                                }
                                else
                                    c1 = (char) d3;
                            }
                            else
                                throw ctx.exception("Illegal hexadecimal byte value");

                            break;
                        }

                        // Unicode character value UTF-16
                        case 'u':
                            c1 = (char) Integer.parseInt(new String(ctx.sql, i + 1, 4), 16);
                            i += 4;
                            break;

                        // Unicode character value UTF-32
                        case 'U':
                            sb.appendCodePoint(Integer.parseInt(new String(ctx.sql, i + 1, 8), 16));
                            i += 8;
                            continue characterLoop;

                        default:

                            // Octal byte value
                            if (Character.digit(c2, 8) != -1) {
                                char c3 = ctx.character(i + 1);

                                if (Character.digit(c3, 8) != -1) {
                                    i++;
                                    char c4 = ctx.character(i + 1);

                                    if (Character.digit(c4, 8) != -1) {
                                        i++;
                                        c1 = (char) Integer.parseInt("" + c2 + c3 + c4, 8);
                                    }
                                    else {
                                        c1 = (char) Integer.parseInt("" + c2 + c3, 8);
                                    }
                                }
                                else {
                                    c1 = (char) Integer.parseInt("" + c2, 8);
                                }
                            }

                            // All other characters
                            else {
                                c1 = c2;
                            }

                            break;
                    }

                    break;
                }
                case '\'': {
                    if (ctx.character(i + 1) != '\'') {
                        ctx.position(i + 1);
                        parseWhitespaceIf(ctx);
                        return sb.toString();
                    }

                    i++;
                    break;
                }
            }

            sb.append(c1);
        }

        throw ctx.exception("String literal not terminated");
    }

    public String parseOracleQuotedStringLiteral(ParserContext ctx) {
        parse(ctx, '\'', false);

        char start = ctx.character();
        char end;

        switch (start) {
            case '[' : end = ']'; ctx.positionInc(); break;
            case '{' : end = '}'; ctx.positionInc(); break;
            case '(' : end = ')'; ctx.positionInc(); break;
            case '<' : end = '>'; ctx.positionInc(); break;
            case ' ' :
            case '\t':
            case '\r':
            case '\n': throw ctx.exception("Illegal quote string character");
            default  : end = start; ctx.positionInc(); break;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = ctx.position(); i < ctx.sql.length; i++) {
            char c = ctx.character(i);

            if (c == end) {
                if (ctx.character(i + 1) == '\'') {
                    ctx.position(i + 2);
                    parseWhitespaceIf(ctx);
                    return sb.toString();
                } else {
                    i++;
                }
            }
            sb.append(c);
        }

        throw ctx.exception("Quoted string literal not terminated");
    }

    public Long parseSignedInteger(ParserContext ctx) {
        Long result = parseSignedIntegerIf(ctx);

        if (result == null) {
            throw ctx.expected("Signed integer");
        }
        return result;
    }

    public Long parseSignedIntegerIf(ParserContext ctx) {
        Sign sign = parseSign(ctx);
        Long unsigned;

        if (sign == Sign.MINUS) {
            unsigned = parseUnsignedInteger(ctx);
        }
        else {
            unsigned = parseUnsignedIntegerIf(ctx);
        }
        return unsigned == null
                ? null
                : sign == Sign.MINUS
                ? -unsigned
                : unsigned;
    }

    public Sign parseSign(ParserContext ctx) {
        Sign sign = Sign.NONE;

        for (;;) {
            if (parseIf(ctx, '+')) {
                sign = sign == Sign.NONE ? Sign.PLUS : sign;
            } else if (parseIf(ctx, '-')) {
                sign = sign == Sign.NONE ? Sign.MINUS : sign.invert();
            } else {
                break;
            }
        }
        return sign;
    }

    private enum Sign {
        NONE,
        PLUS,
        MINUS;

        final Sign invert() {
            if (this == PLUS) {
                return MINUS;
            }
            else if (this == MINUS) {
                return PLUS;
            }
            else {
                return NONE;
            }
        }
    }

    public DataType parseDataType(ParserContext ctx) {
        DataType result = parseDataTypePrefix(ctx);
        return result;
    }

    public DataType parseDataTypePrefix(ParserContext ctx) {
        char character = ctx.character();

        if (character == '[' || character == '"' || character == '`') {
            character = ctx.characterNext();
        }

        switch (character) {
            case 'b':
            case 'B':
                if (parseKeywordOrIdentifierIf(ctx, "BIGINT")) {
                    return parseDataTypeLength(ctx, SQLDataType.BIGINT);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BINARY")) {
                    return parseDataTypeLength(ctx, SQLDataType.BINARY);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BIT")) {
                    return parseDataTypeLength(ctx, SQLDataType.BIT);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BLOB")) {
                    return parseDataTypeLength(ctx, SQLDataType.BLOB);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BOOLEAN") ||
                        parseKeywordOrIdentifierIf(ctx, "BOOL")) {
                    return SQLDataType.BOOLEAN;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BYTEA")) {
                    return SQLDataType.BLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BIGSERIAL")) {
                    return parseDataTypeLength(ctx, SQLDataType.BIGINT);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BINARY_INTEGER")) {
                    return SQLDataType.INTEGER;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BINARY_BIGINT")) {
                    return SQLDataType.BIGINT;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BINARY_DOUBLE")) {
                    return SQLDataType.DOUBLE;
                }
                break;

            case 'c':
            case 'C':
                if (parseKeywordOrIdentifierIf(ctx, "CHARACTER VARYING")) {
                    return parseDataTypeLength(ctx, SQLDataType.VARCHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "CHAR") ||
                        parseKeywordOrIdentifierIf(ctx, "CHARACTER")) {
                    return parseDataTypeLength(ctx, SQLDataType.CHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "CLOB")) {
                    return parseDataTypeLength(ctx, SQLDataType.CLOB);
                }

                break;

            case 'd':
            case 'D':
                if (parseKeywordOrIdentifierIf(ctx, "DATE")) {
                    return SQLDataType.DATE;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DATETIME")) {
                    return parseDataTypePrecision(ctx, SQLDataType.TIMESTAMP);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DECIMAL")) {
                    return parseDataTypePrecisionScale(ctx, SQLDataType.DECIMAL);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DOUBLE PRECISION") ||
                        parseKeywordOrIdentifierIf(ctx, "DOUBLE")) {
                    return parseDataTypePrecisionScale(ctx, SQLDataType.DOUBLE);
                }
                break;

            case 'e':
            case 'E':
                if (parseKeywordOrIdentifierIf(ctx, "ENUM")) {
                    throw ctx.exception("不支持ENUM类型");
                }
                break;

            case 'f':
            case 'F':
                if (parseKeywordOrIdentifierIf(ctx, "FLOAT")) {
                    return parseDataTypePrecisionScale(ctx, SQLDataType.FLOAT);
                }
                break;

            case 'i':
            case 'I':
                if (parseKeywordOrIdentifierIf(ctx, "INTEGER") ||
                        parseKeywordOrIdentifierIf(ctx, "INT") ||
                        parseKeywordOrIdentifierIf(ctx, "INT4")) {
                    return parseUnsigned(ctx, parseAndIgnoreDataTypeLength(ctx, SQLDataType.INTEGER));
                }
                else if (parseKeywordOrIdentifierIf(ctx, "INT2")) {
                    return SQLDataType.SMALLINT;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "INT8")) {
                    return SQLDataType.BIGINT;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "INTERVAL")) {

                    //INTERVAL YEAR[(n)] TO MONTH
                    //INTERVAL DAY［(n1)］ TO SECOND [(n2)]

                    if (parseKeywordOrIdentifierIf(ctx, "YEAR")) {
                        int yearPrecision = parseDataTypePrecision(ctx);
                        if (parseKeywordOrIdentifierIf(ctx, "TO MONTH")) {
                            return SQLDataType.INTERVALYEARTOMONTH(yearPrecision);
                        }

                    } else if (parseKeywordOrIdentifierIf(ctx, "DAY")) {
                        int dayPrecision = parseDataTypePrecision(ctx);
                        if (parseKeywordOrIdentifierIf(ctx, "TO SECOND")) {
                            int fractionalSecondsPrecision = parseDataTypePrecision(ctx);
                            return SQLDataType.INTERVALDAYTOSECOND(dayPrecision, fractionalSecondsPrecision);
                        }
                    }

                }
                else if (parseKeywordOrIdentifierIf(ctx, "IMAGE")) {
                    return SQLDataType.IMAGE;
                }
                break;

            case 'l':
            case 'L':
                if (parseKeywordOrIdentifierIf(ctx, "LONG")) {
                    return parseDataTypeLength(ctx, SQLDataType.LONGVARCHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "LONGBLOB")) {
                    return SQLDataType.BLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "LONGTEXT")) {
                    return SQLDataType.CLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "LONG NVARCHAR")) {
                    return parseDataTypeLength(ctx, SQLDataType.LONGNVARCHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "LONG VARBINARY")) {
                    return parseDataTypeLength(ctx, SQLDataType.LONGVARBINARY);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "LONG VARCHAR")) {
                    return parseDataTypeLength(ctx, SQLDataType.LONGVARCHAR);
                }
                break;

            case 'm':
            case 'M':
                if (parseKeywordOrIdentifierIf(ctx, "MEDIUMBLOB")) {
                    return SQLDataType.BLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "MEDIUMINT")) {
                    return parseUnsigned(ctx, parseAndIgnoreDataTypeLength(ctx, SQLDataType.INTEGER));
                }
                else if (parseKeywordOrIdentifierIf(ctx, "MEDIUMTEXT")) {
                    return SQLDataType.CLOB;
                }
                break;

            case 'n':
            case 'N':
                if (parseKeywordOrIdentifierIf(ctx, "NCHAR")) {
                    return parseDataTypeLength(ctx, SQLDataType.NCHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "NCLOB")) {
                    return SQLDataType.NCLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "NUMBER") ||
                        parseKeywordOrIdentifierIf(ctx, "NUMERIC")) {
                    return parseDataTypePrecisionScale(ctx, SQLDataType.NUMERIC);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "NVARCHAR") ||
                        parseKeywordOrIdentifierIf(ctx, "NVARCHAR2")) {
                    return parseDataTypeLength(ctx, SQLDataType.NVARCHAR);
                }
                break;

            case 'o':
            case 'O':
                if (parseKeywordOrIdentifierIf(ctx, "OTHER")) {
                    return SQLDataType.OTHER;
                }
                break;

            case 'r':
            case 'R':
                if (parseKeywordOrIdentifierIf(ctx, "REAL")) {
                    return parseAndIgnoreDataTypePrecisionScale(ctx, SQLDataType.REAL);
                }
                break;

            case 's':
            case 'S':
                if (parseKeywordOrIdentifierIf(ctx, "SERIAL4") ||
                        parseKeywordOrIdentifierIf(ctx, "SERIAL")) {
                    return SQLDataType.INTEGER.identity(true);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "SERIAL8")) {
                    return SQLDataType.BIGINT.identity(true);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "SET")) {
                    throw ctx.exception("不支持的类型：SET");
                }
                else if (parseKeywordOrIdentifierIf(ctx, "SMALLINT")) {
                    return parseUnsigned(ctx, parseAndIgnoreDataTypeLength(ctx, SQLDataType.SMALLINT));
                }
                else if (parseKeywordOrIdentifierIf(ctx, "SMALLSERIAL") ||
                        parseKeywordOrIdentifierIf(ctx, "SERIAL2")) {
                    return SQLDataType.INTEGER.identity(true);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "STRING")) {
                    return SQLDataType.VARCHAR;
                }

                break;

            case 't':
            case 'T':
                if (parseKeywordOrIdentifierIf(ctx, "TEXT")) {
                    return parseAndIgnoreDataTypeLength(ctx, SQLDataType.CLOB);
                }

                else if (parseKeywordOrIdentifierIf(ctx, "TIMESTAMPTZ")) {
                    return parseDataTypePrecision(ctx, SQLDataType.TIMESTAMPWITHTIMEZONE);
                }

                else if (parseKeywordOrIdentifierIf(ctx, "TIMESTAMP")) {
                    Integer precision = parseDataTypePrecision(ctx);
                    if (parseKeywordOrIdentifierIf(ctx, "WITH TIME ZONE")) {
                        return SQLDataType.TIMESTAMPWITHTIMEZONE(precision);
                    }
                    else if (parseKeywordOrIdentifierIf(ctx, "WITH LOCAL TIME ZONE")) {
                        return SQLDataType.TIMESTAMP(precision);
                    }
                    else if (parseKeywordOrIdentifierIf(ctx, "WITHOUT TIME ZONE") || true) {
                        return SQLDataType.TIMESTAMP(precision);
                    }
                }

                else if (parseKeywordOrIdentifierIf(ctx, "TIMETZ")) {
                    return parseDataTypePrecision(ctx, SQLDataType.TIMEWITHTIMEZONE);
                }

                else if (parseKeywordOrIdentifierIf(ctx, "TIME")) {
                    Integer precision = parseDataTypePrecision(ctx);

                    if (parseKeywordOrIdentifierIf(ctx, "WITH TIME ZONE")) {
                        return SQLDataType.TIMEWITHTIMEZONE(precision);
                    }
                    else if (parseKeywordOrIdentifierIf(ctx, "WITHOUT TIME ZONE") || true) {
                        return SQLDataType.TIME(precision);
                    }
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TINYBLOB")) {
                    return SQLDataType.BLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TINYINT")) {
                    return parseUnsigned(ctx, parseAndIgnoreDataTypeLength(ctx, SQLDataType.TINYINT));
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TINYTEXT")) {
                    return SQLDataType.VARCHAR;
                }

                break;

            case 'u':
            case 'U':
                if (parseKeywordOrIdentifierIf(ctx, "UUID")) {
                    return SQLDataType.VARCHAR;
                }
                break;

            case 'v':
            case 'V':
                if (parseKeywordOrIdentifierIf(ctx, "VARCHAR") ||
                        parseKeywordOrIdentifierIf(ctx, "VARCHAR2")) {
                    return parseDataTypeLength(ctx, SQLDataType.VARCHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "VARBINARY")) {
                    return parseDataTypeLength(ctx, SQLDataType.VARBINARY);
                }

                break;

        }



        throw ctx.exception("Unknown data type");
    }

    public DataType parseUnsigned(ParserContext ctx, DataType result) {
        if (parseKeywordIf(ctx, "UNSIGNED")) {
            if (result == SQLDataType.TINYINT) {
                return SQLDataType.TINYINTUNSIGNED;

            } else if (result == SQLDataType.SMALLINT) {
                return SQLDataType.SMALLINTUNSIGNED;

            } else if (result == SQLDataType.INTEGER) {
                return SQLDataType.INTEGERUNSIGNED;

            } else if (result == SQLDataType.BIGINT) {
                return SQLDataType.BIGINTUNSIGNED;
            }
        }
        return result;
    }

    public DataType parseAndIgnoreDataTypePrecisionScale(ParserContext ctx, DataType result) {
        if (parseIf(ctx, '(')) {
            parseUnsignedInteger(ctx);

            if (parseIf(ctx, ',')) {
                parseUnsignedInteger(ctx);
            }
            parse(ctx, ')');
        }
        return result;
    }

    public DataType parseDataTypePrecisionScale(ParserContext ctx, DataType result) {
        if (parseIf(ctx, '(')) {
            int precision = (int) (long) parseUnsignedInteger(ctx);

            if (parseIf(ctx, ',')) {
                result.precision(precision);
                result.scale((int) (long) parseUnsignedInteger(ctx));
            }
            else {
                result.precision(precision);
                result.scale(0);
            }
            parse(ctx, ')');
        }

        return result;
    }

    public int parseDataTypePrecision(ParserContext ctx) {
        int precision = 0;

        if (parseIf(ctx, '(')) {
            precision = (int) (long) parseUnsignedInteger(ctx);
            parse(ctx, ')');
        }

        return precision;
    }

    public DataType parseDataTypePrecision(ParserContext ctx, DataType column) {
        if (parseIf(ctx, '(')) {
            int precision = (int) (long) parseUnsignedInteger(ctx);
            column.precision(precision);
            parse(ctx, ')');
        }

        return column;
    }

    public DataType parseDataTypeLength(ParserContext ctx, DataType in) {
        DataType result = in;

        if (parseIf(ctx, '(')) {

            if (!parseKeywordIf(ctx, "MAX")) {
                result.length((int) (long) parseUnsignedInteger(ctx));
            }

            /*String sqlType = in.getSqlType();
            if (StringUtils.equalsIgnoreCase(sqlType, "varchar") ||
                    StringUtils.equalsIgnoreCase(sqlType, "char")) {
                if (!parseKeywordIf(ctx, "BYTE")) {
                    parseKeywordIf(ctx, "CHAR");
                }
            }*/
            parse(ctx, ')');
        }

        return result;
    }

    public Long parseUnsignedInteger(ParserContext ctx) {
        Long result = parseUnsignedIntegerIf(ctx);

        if (result == null) {
            throw ctx.expected("Unsigned integer");
        }
        return result;
    }

    public Long parseUnsignedIntegerIf(ParserContext ctx) {
        int position = ctx.position();

        for (;;) {
            char c = ctx.character();

            if (c >= '0' && c <= '9') {
                ctx.positionInc();
            }
            else {
                break;
            }
        }

        if (position == ctx.position()) {
            return null;
        }

        String s = ctx.substring(position, ctx.position());
        parseWhitespaceIf(ctx);
        return Long.valueOf(s);
    }

    public boolean parseKeywordOrIdentifierIf(ParserContext ctx, String keyword) {
        int position = ctx.position();
        char quoteEnd = parseQuote(ctx, false);
        boolean result = parseKeywordIf(ctx, keyword);

        if (!result) {
            ctx.position(position);
        }
        else if (quoteEnd != 0) {
            parse(ctx, quoteEnd);
        }
        return result;
    }

    public DataType parseAndIgnoreDataTypeLength(ParserContext ctx, DataType result) {
        if (parseIf(ctx, '(')) {
            parseUnsignedInteger(ctx);
            parse(ctx, ')');
        }

        return result;
    }

    public Table parseTable(String sql, Dialect dialect) {
        ParserContext ctx = ctx(sql);
        ctx.setDialect(dialect);
        Table result = parseTable(ctx);

        ctx.done("Unexpected content after end of table input");
        return result;
    }

    private ParserContext ctx(String sql, Object... bindings) {
        ParserContext ctx = new ParserContext(sql, bindings);
        parseWhitespaceIf(ctx);
        return ctx;
    }

    public boolean parseWhitespaceIf(ParserContext ctx) {
        int position = ctx.position();
        ctx.position(afterWhitespace(ctx, position));
        return position != ctx.position();
    }

    public int afterWhitespace(ParserContext ctx, int position) {
        loop:
        for (int i = position; i < ctx.sql.length; i++) {
            switch (ctx.sql[i]) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    position = i + 1;
                    continue loop;

                case '/':
                    if (i + 1 < ctx.sql.length && ctx.sql[i + 1] == '*') {
                        i = i + 2;

                        while (i < ctx.sql.length) {
                            switch (ctx.sql[i]) {
                                case '+':
                                    if (!ctx.ignoreHints() && i + 1 < ctx.sql.length &&
                                            ((ctx.sql[i + 1] >= 'A' && ctx.sql[i + 1] <= 'Z') ||
                                                    (ctx.sql[i + 1] >= 'a' && ctx.sql[i + 1] <= 'z')))
                                        break loop;

                                    break;

                                case '*':
                                    if (i + 1 < ctx.sql.length && ctx.sql[i + 1] == '/') {
                                        position = (i = i + 1) + 1;
                                        continue loop;
                                    }

                                    break;
                            }

                            i++;
                        }
                    }

                    break loop;

                case '-':
                    if (i + 1 < ctx.sql.length && ctx.sql[i + 1] == '-') {
                        i = i + 2;

                        while (i < ctx.sql.length) {
                            switch (ctx.sql[i]) {
                                case '\r':
                                case '\n':
                                    position = i;
                                    continue loop;

                                default:
                                    i++;
                            }
                        }

                        position = i;
                    }

                    break loop;

                // TODO MySQL comments require a whitespace after --. Should we deal with this?
                // TODO Some databases also support # as a single line comment character.

                default:
                    position = i;
                    break loop;
            }
        }

        return position;
    }

    public void parseKeyword(ParserContext ctx, String keyword) {
        if (!parseKeywordIf(ctx, keyword)) {
            throw ctx.expected("Keyword '" + keyword + "'");
        }
    }

    public boolean parseKeywordIf(ParserContext ctx, String keyword) {
        return peekKeyword(ctx, keyword, true, false, false);
    }

    public String parseAndGetKeywordIf(ParserContext ctx, String... keywords) {
        for (String keyword : keywords) {
            if (parseKeywordIf(ctx, keyword)) {
                return keyword.toLowerCase();
            }
        }
        return null;
    }

    public String parseAndGetKeywordIf(ParserContext ctx, String keyword) {
        if (parseKeywordIf(ctx, keyword)) {
            return keyword.toLowerCase();
        }
        return null;
    }

    public boolean peekKeyword(
            ParserContext ctx, String keyword, boolean updatePosition,
            boolean peekIntoParens, boolean requireFunction) {
        int length = keyword.length();
        int skip;
        int position = ctx.position();

        if (ctx.sql.length < position + length) {
            return false;
        }

        skipLoop:
        for (skip = 0; position + skip < ctx.sql.length; skip++) {
            char c = ctx.sql[position + skip];

            switch (c) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue skipLoop;

                case '(':
                    if (peekIntoParens) {
                        continue skipLoop;
                    }
                    else {
                        break skipLoop;
                    }

                default:
                    break skipLoop;
            }
        }


        for (int i = 0; i < length; i++) {
            char c = keyword.charAt(i);
            int p = position + i + skip;

            switch (c) {
                case ' ':
                    skip = skip + (afterWhitespace(ctx, p) - p - 1);
                    break;

                default:
                    if (upper(ctx.sql[p]) != c) {
                        return false;
                    }
                    break;
            }
        }

        if (ctx.isIdentifierPart(position + length + skip)) {
            return false;
        }

        if (requireFunction) {
            if (ctx.character(afterWhitespace(ctx, position + length + skip)) != '(') {
                return false;
            }
        }

        if (updatePosition) {
            ctx.positionInc(length + skip);
            parseWhitespaceIf(ctx);
        }

        return true;
    }

    public char upper(char c) {
        return c >= 'a' && c <= 'z' ? (char) (c - ('a' - 'A')) : c;
    }

    public boolean parseIf(ParserContext ctx, String string) {
        return parseIf(ctx, string, true);
    }

    public boolean parseIf(ParserContext ctx, String string, boolean skipAfterWhitespace) {
        boolean result = peek(ctx, string);

        if (result) {
            ctx.positionInc(string.length());

            if (skipAfterWhitespace)
                parseWhitespaceIf(ctx);
        }

        return result;
    }

    public boolean parse(ParserContext ctx, char c) {
        return parse(ctx, c, true);
    }

    public boolean parse(ParserContext ctx, char c, boolean skipAfterWhitespace) {
        if (!parseIf(ctx, c, skipAfterWhitespace)) {
            throw ctx.expected("Token '" + c + "'");
        }
        return true;
    }

    public boolean parseIf(ParserContext ctx, char c) {
        return parseIf(ctx, c, true);
    }

    public boolean parseIf(ParserContext ctx, char c, boolean skipAfterWhitespace) {
        boolean result = peek(ctx, c);

        if (result) {
            ctx.positionInc();

            if (skipAfterWhitespace) {
                parseWhitespaceIf(ctx);
            }
        }

        return result;
    }

    public boolean parseIf(ParserContext ctx, char c, char peek, boolean skipAfterWhitespace) {
        if (ctx.character() != c) {
            return false;
        }
        if (ctx.characterNext() != peek) {
            return false;
        }
        ctx.positionInc();
        if (skipAfterWhitespace) {
            parseWhitespaceIf(ctx);
        }
        return true;
    }

    public boolean peek(ParserContext ctx, char c) {
        if (ctx.character() != c) {
            return false;
        }
        return true;
    }

    public boolean peek(ParserContext ctx, String string) {
        int length = string.length();

        if (ctx.sql.length < ctx.position() + length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (ctx.sql[ctx.position() + i] != string.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    public boolean peekKeyword(ParserContext ctx, String... keywords) {
        for (String keyword : keywords)
            if (peekKeyword(ctx, keyword))
                return true;

        return false;
    }

    public boolean peekKeyword(ParserContext ctx, String keyword) {
        return peekKeyword(ctx, keyword, false, false, false);
    }

    public List<String> parseIdentifiers(ParserContext ctx) {
        LinkedHashSet<String> result = new LinkedHashSet<>();

        do {
            if (!result.add(parseIdentifier(ctx)))
                throw ctx.exception("Duplicate identifier encountered");
        }
        while (parseIf(ctx, ','));
        return new ArrayList<>(result);
    }

    public String parseIdentifier(ParserContext ctx) {
        return parseIdentifier(ctx, false);
    }

    public String parseIdentifier(ParserContext ctx, boolean allowAposQuotes) {
        String result = parseIdentifierIf(ctx, allowAposQuotes);

        if (result == null)
            throw ctx.expected("Identifier");

        return result;
    }

    public String parseIdentifierIf(ParserContext ctx) {
        return parseIdentifierIf(ctx, false);
    }

    public char parseQuote(ParserContext ctx, boolean allowAposQuotes) {
        return parseIf(ctx, '"', false) ? '"'
                : parseIf(ctx, '`', false) ? '`'
                : parseIf(ctx, '[', false) ? ']'
                : allowAposQuotes && parseIf(ctx, '\'', false) ? '\''
                : 0;
    }

    public String parseIdentifierIf(ParserContext ctx, boolean allowAposQuotes) {
        char quoteEnd = parseQuote(ctx, allowAposQuotes);

        int start = ctx.position();
        if (quoteEnd != 0) {
            while (ctx.character() != quoteEnd && ctx.hasMore()) {
                ctx.positionInc();
            }
        }
        else {
            while (ctx.isIdentifierPart() && ctx.hasMore()) {
                ctx.positionInc();
            }
        }
        if (ctx.position() == start) {
            return null;
        }
        String result = ctx.substring(start, ctx.position());

        if (quoteEnd != 0) {
            if (ctx.character() != quoteEnd) {
                throw ctx.exception("Quoted identifier must terminate in " + quoteEnd);
            }
            ctx.positionInc();
            parseWhitespaceIf(ctx);
            return result;
        }
        else {
            parseWhitespaceIf(ctx);
            return result;
        }
    }

    public String parseName(ParserContext ctx) {
        String result = parseNameIf(ctx);

        if (result == null) {
            throw ctx.expected("Identifier");
        }
        return result;
    }

    public String parseNameIf(ParserContext ctx) {
        String identifier = parseIdentifierIf(ctx);

        if (identifier == null) {
            return null;
        }

        if (parseIf(ctx, '.')) {
            List<String> result = new ArrayList<>();
            result.add(identifier);

            do {
                result.add(parseIdentifier(ctx));
            }
            while (parseIf(ctx, '.'));

            return StringUtils.join(result, ".");
        }
        else {
            return identifier;
        }
    }

}

final class ParserContext {

    final String                 sqlString;
    final char[]                 sql;
    private int                  position    = 0;
    private boolean              ignoreHints = true;
    private final Object[]       bindings;
    private int                  bindIndex   = 0;
    private String               delimiter   = ";";

    private Dialect dialect;

    ParserContext(
            String sqlString,
            Object[] bindings
    ) {
        this.sqlString = sqlString;
        this.sql = sqlString.toCharArray();
        this.bindings = bindings;
    }

    String substring(int startPosition, int endPosition) {
        return new String(sql, startPosition, endPosition - startPosition);
    }

    ParserException internalError() {
        return exception("Internal Error");
    }

    ParserException expected(String object) {
        return init(new ParserException(mark(), object + " expected"));
    }

    ParserException expected(String... objects) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < objects.length; i++) {
            if (i == 0) {
                sb.append(objects[i]);
            } else if (i == objects.length - 1) {
                sb.append(", or ").append(objects[i]);
            } else {
                sb.append(", ").append(objects[i]);
            }
        }
        return init(new ParserException(mark(), sb.toString() + " expected"));
    }

    ParserException notImplemented(String feature) {
        return init(new ParserException(mark(), feature + " not yet implemented"));
    }

    ParserException unsupportedClause() {
        return init(new ParserException(mark(), "Unsupported clause"));
    }

    ParserException exception(String message) {
        return init(new ParserException(mark(), message));
    }

    ParserException init(ParserException e) {
        int[] line = line();
        return e.position(position).line(line[0]).column(line[1]);
    }

    Object nextBinding() {
        if (bindIndex < bindings.length)
            return bindings[bindIndex++];
        else if (bindings.length == 0)
            return null;
        else
            throw exception("No binding provided for bind index " + (bindIndex + 1));
    }

    int[] line() {
        int line = 1;
        int column = 1;

        for (int i = 0; i < position; i++) {
            if (sql[i] == '\r') {
                line++;
                column = 1;

                if (i + 1 < sql.length && sql[i + 1] == '\n')
                    i++;
            }
            else if (sql[i] == '\n') {
                line++;
                column = 1;
            }
            else {
                column++;
            }
        }

        return new int[] { line, column };
    }

    char character() {
        return character(position);
    }

    char character(int pos) {
        return pos >= 0 && pos < sql.length ? sql[pos] : ' ';
    }

    char characterNext() {
        return character(position + 1);
    }

    int position() {
        return position;
    }

    void position(int newPosition) {
        position = newPosition;
    }

    void positionInc() {
        positionInc(1);
    }

    void positionInc(int inc) {
        position(position + inc);
    }

    String delimiter() {
        return delimiter;
    }

    void delimiter(String newDelimiter) {
        delimiter = newDelimiter;
    }

    boolean ignoreHints() {
        return ignoreHints;
    }

    void ignoreHints(boolean newIgnoreHints) {
        ignoreHints = newIgnoreHints;
    }

    boolean isWhitespace() {
        return Character.isWhitespace(character());
    }

    boolean isWhitespace(int pos) {
        return Character.isWhitespace(character(pos));
    }

    boolean isIdentifierPart() {
        return isIdentifierPart(character());
    }

    boolean isIdentifierPart(int pos) {
        return isIdentifierPart(character(pos));
    }

    boolean isIdentifierPart(char character) {
        return Character.isJavaIdentifierPart(character)
                || ((character == '@'
                ||   character == '#')
                &&   character != delimiter.charAt(0));
    }

    boolean hasMore() {
        return position < sql.length;
    }

    boolean done() {
        return position >= sql.length && (bindings.length == 0 || bindings.length == bindIndex);
    }

    boolean done(String message) {
        if (done()) {
            return true;
        }
        else {
            throw exception(message);
        }
    }

    String mark() {
        int[] line = line();
        return "[" + line[0] + ":" + line[1] + "] "
                + (position > 50 ? "..." : "")
                + sqlString.substring(Math.max(0, position - 50), position)
                + "[*]"
                + sqlString.substring(position, Math.min(sqlString.length(), position + 80))
                + (sqlString.length() > position + 80 ? "..." : "");
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public String toString() {
        return mark();
    }
}
