package com.huacloud.synctable;

import com.huacloud.synctable.mapping.datatype.DataType;
import com.huacloud.synctable.mapping.datatype.MySqlDataType;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/17/2019 8:01 PM
 */
public class MySQLParserImpl extends ParserImpl {

    @Override
    public DataType parseDataType(ParserContext ctx) {
        char character = ctx.character();

        if (character == '`') {
            character = ctx.characterNext();
        }

        switch (character) {
            case 'b':
            case 'B':
                if (parseKeywordOrIdentifierIf(ctx, "BIGINT")) {
                    return parseDataTypeLength(ctx, MySqlDataType.BIGINT);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BINARY")) {
                    return parseDataTypeLength(ctx, MySqlDataType.BINARY);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BIT")) {
                    return parseDataTypeLength(ctx, MySqlDataType.BIT);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BLOB")) {
                    return parseDataTypeLength(ctx, MySqlDataType.BLOB);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BOOL") ||
                        parseKeywordOrIdentifierIf(ctx, "BOOLEAN")) {
                    return MySqlDataType.BOOLEAN;
                }
                break;

            case 'c':
            case 'C':
                if (parseKeywordOrIdentifierIf(ctx, "CHAR")) {
                    return parseDataTypeLength(ctx, MySqlDataType.CHAR);
                }
                break;

            case 'd':
            case 'D':
                if (parseKeywordOrIdentifierIf(ctx, "DATE")) {
                    return MySqlDataType.DATE;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DATETIME")) {
                    return parseDataTypePrecision(ctx, MySqlDataType.DATETIME);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DECIMAL") ||
                        parseKeywordOrIdentifierIf(ctx, "DEC")) {
                    return parseDataTypePrecisionScale(ctx, MySqlDataType.DECIMAL);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DOUBLE")) {
                    parseKeywordOrIdentifierIf(ctx, "PRECISION");
                    return parseDataTypePrecisionScale(ctx, MySqlDataType.DOUBLE);
                }
                break;

            case 'e':
            case 'E':
                if (parseKeywordOrIdentifierIf(ctx, "ENUM")) {
                    return MySqlDataType.ENUM;
                }
                break;

            case 'f':
            case 'F':
                if (parseKeywordOrIdentifierIf(ctx, "FLOAT")) {
                    return parseDataTypePrecisionScale(ctx, MySqlDataType.FLOAT);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "FIXED")) {
                    return parseDataTypePrecisionScale(ctx, MySqlDataType.DECIMAL);
                }
                break;

            case 'i':
            case 'I':
                if (parseKeywordOrIdentifierIf(ctx, "INTEGER") ||
                        parseKeywordOrIdentifierIf(ctx, "INT")) {
                    return parseUnsigned(ctx, parseAndIgnoreDataTypeLength(ctx, MySqlDataType.INT));
                }
                break;

            case 'j':
            case 'J':
                if (parseKeywordOrIdentifierIf(ctx, "JSON")) {
                    return MySqlDataType.JSON;
                }
                break;

            case 'l':
            case 'L':
                if (parseKeywordOrIdentifierIf(ctx, "LONGBLOB")) {
                    return MySqlDataType.LONGBLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "LONGTEXT")) {
                    return MySqlDataType.LONGTEXT;
                }
                break;

            case 'm':
            case 'M':
                if (parseKeywordOrIdentifierIf(ctx, "MEDIUMBLOB")) {
                    return MySqlDataType.MEDIUMBLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "MEDIUMINT")) {
                    return parseUnsigned(ctx, parseAndIgnoreDataTypeLength(ctx, MySqlDataType.MEDIUMINT));
                }
                else if (parseKeywordOrIdentifierIf(ctx, "MEDIUMTEXT")) {
                    return MySqlDataType.MEDIUMTEXT;
                }
                break;

            case 'n':
            case 'N':
                if (parseKeywordOrIdentifierIf(ctx, "NUMERIC")) {
                    return parseDataTypePrecisionScale(ctx, MySqlDataType.NUMERIC);
                }
                break;

            case 'r':
            case 'R':
                if (parseKeywordOrIdentifierIf(ctx, "REAL")) {
                    return parseDataTypePrecisionScale(ctx, MySqlDataType.REAL);
                }
                break;

            case 's':
            case 'S':
                if (parseKeywordOrIdentifierIf(ctx, "SET")) {
                    return MySqlDataType.SET;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "SMALLINT")) {
                    return parseUnsigned(ctx, parseAndIgnoreDataTypeLength(ctx, MySqlDataType.SMALLINT));
                }
                break;

            case 't':
            case 'T':
                if (parseKeywordOrIdentifierIf(ctx, "TEXT")) {
                    return parseDataTypeLength(ctx, MySqlDataType.TEXT);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TIME")) {
                    return parseDataTypeLength(ctx, MySqlDataType.TIME);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TIMESTAMP")) {
                    return parseDataTypeLength(ctx, MySqlDataType.TIMESTAMP);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TINYBLOB")) {
                    return MySqlDataType.TINYBLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TINYINT")) {
                    return parseUnsigned(ctx, parseAndIgnoreDataTypeLength(ctx, MySqlDataType.TINYINT));
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TINYTEXT")) {
                    return MySqlDataType.TINYTEXT;
                }
                break;

            case 'v':
            case 'V':
                if (parseKeywordOrIdentifierIf(ctx, "VARCHAR")) {
                    return parseDataTypeLength(ctx, MySqlDataType.VARCHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "VARBINARY")) {
                    return parseDataTypeLength(ctx, MySqlDataType.VARBINARY);
                }
                break;

            case 'y':
            case 'Y':
                if (parseKeywordOrIdentifierIf(ctx, "YEAR")) {
                    return parseDataTypeLength(ctx, MySqlDataType.YEAR);
                }
                break;
        }
        throw ctx.exception("Unknown data type");
    }

}
