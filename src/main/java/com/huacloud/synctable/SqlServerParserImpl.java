package com.huacloud.synctable;

import com.huacloud.synctable.mapping.datatype.DataType;
import com.huacloud.synctable.mapping.datatype.SqlServerDataType;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/18/2019 12:27 PM
 */
public class SqlServerParserImpl extends ParserImpl {

    @Override
    public DataType parseDataType(ParserContext ctx) {
        char character = ctx.character();

        if (character == '[') {
            character = ctx.characterNext();
        }

        switch (character) {
            case 'b':
            case 'B':
                if (parseKeywordOrIdentifierIf(ctx, "BIGINT")) {
                    return SqlServerDataType.BIGINT;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BINARY")) {
                    return parseDataTypeLength(ctx, SqlServerDataType.BINARY);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BIT")) {
                    return SqlServerDataType.BIT;
                }
                break;

            case 'c':
            case 'C':
                if (parseKeywordOrIdentifierIf(ctx, "CHAR")) {
                    return parseDataTypeLength(ctx, SqlServerDataType.CHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "CURSOR")) {
                    return SqlServerDataType.CURSOR;
                }

                break;

            case 'd':
            case 'D':
                if (parseKeywordOrIdentifierIf(ctx, "DATE")) {
                    return SqlServerDataType.DATE;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DATETIME")) {
                    return SqlServerDataType.DATETIME;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DATETIME2")) {
                    return parseDataTypePrecision(ctx, SqlServerDataType.DATETIME2);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DATETIMEOFFSET")) {
                    return parseDataTypePrecision(ctx, SqlServerDataType.DATETIMEOFFSET);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DECIMAL")) {
                    return parseDataTypePrecisionScale(ctx, SqlServerDataType.DECIMAL);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DEC")) {
                    return parseDataTypePrecisionScale(ctx, SqlServerDataType.DECIMAL);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "DOUBLE PRECISION")) {
                    return SqlServerDataType.DOUBLE_PRECISION;
                }
                break;

            case 'f':
            case 'F':
                if (parseKeywordOrIdentifierIf(ctx, "FLOAT")) {
                    return parseDataTypePrecision(ctx, SqlServerDataType.FLOAT);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "HIERARCHYID")) {
                    return SqlServerDataType.HIERARCHYID;
                }
                break;

            case 'i':
            case 'I':
                if (parseKeywordOrIdentifierIf(ctx, "INT")) {
                    return SqlServerDataType.INT;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "IMAGE")) {
                    return SqlServerDataType.IMAGE;
                }
                break;

            case 'm':
            case 'M':
                if (parseKeywordOrIdentifierIf(ctx, "MONEY")) {
                    return SqlServerDataType.MONEY;
                }
                break;

            case 'n':
            case 'N':
                if (parseKeywordOrIdentifierIf(ctx, "NCHAR")) {
                    return parseDataTypeLength(ctx, SqlServerDataType.NCHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "NTEXT")) {
                    return SqlServerDataType.NTEXT;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "NUMERIC")) {
                    return parseDataTypePrecisionScale(ctx, SqlServerDataType.NUMERIC);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "NVARCHAR")) {
                    return parseDataTypeLength(ctx, SqlServerDataType.NVARCHAR);
                }
                break;

            case 'r':
            case 'R':
                if (parseKeywordOrIdentifierIf(ctx, "REAL")) {
                    return SqlServerDataType.REAL;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "ROWVERSION")) {
                    return SqlServerDataType.ROWVERSION;
                }
                break;

            case 's':
            case 'S':
                if (parseKeywordOrIdentifierIf(ctx, "SMALLDATETIME")) {
                    return SqlServerDataType.SMALLDATETIME;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "SMALLINT")) {
                    return SqlServerDataType.SMALLINT;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "SMALLMONEY")) {
                    return SqlServerDataType.SMALLMONEY;
                }

                break;

            case 't':
            case 'T':
                if (parseKeywordOrIdentifierIf(ctx, "SQL_VARIANT")) {
                    return SqlServerDataType.SQL_VARIANT;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TEXT")) {
                    return SqlServerDataType.TEXT;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TIME")) {
                    return parseDataTypePrecision(ctx, SqlServerDataType.TIME);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "TINYINT")) {
                    return SqlServerDataType.TINYINT;
                }
                break;

            case 'v':
            case 'V':
                if (parseKeywordOrIdentifierIf(ctx, "VARCHAR")) {
                    return parseDataTypeLength(ctx, SqlServerDataType.VARCHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "VARBINARY")) {
                    return parseDataTypeLength(ctx, SqlServerDataType.VARBINARY);
                }
                break;

        }
        throw ctx.exception("Unknown data type");
    }

}
