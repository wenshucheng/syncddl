package com.huacloud.synctable;

import com.huacloud.synctable.mapping.datatype.DataType;
import com.huacloud.synctable.mapping.datatype.OracleDataType;

/**
 * @author Peng Xiaodong<https://github.com/shadon178>
 * @date 9/18/2019 11:38 AM
 */
public class OracleParserImpl extends ParserImpl {

    @Override
    public DataType parseDataType(ParserContext ctx) {

        char character = ctx.character();

        if (character == '"') {
            character = ctx.characterNext();
        }

        switch (character) {
            case 'b':
            case 'B':
                if (parseKeywordOrIdentifierIf(ctx, "BLOB")) {
                    return OracleDataType.BLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BINARY_DOUBLE")) {
                    return OracleDataType.BINARY_DOUBLE;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BINARY_FLOAT")) {
                    return OracleDataType.BINARY_FLOAT;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "BFILE")) {
                    return OracleDataType.BFILE;
                }
                break;

            case 'c':
            case 'C':
                if (parseKeywordOrIdentifierIf(ctx, "CHAR")) {
                    return parseDataTypeLength(ctx, OracleDataType.CHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "CLOB")) {
                    return OracleDataType.CLOB;
                }
                break;

            case 'd':
            case 'D':
                if (parseKeywordOrIdentifierIf(ctx, "DATE")) {
                    return OracleDataType.DATE;
                }
                break;

            case 'f':
            case 'F':
                if (parseKeywordOrIdentifierIf(ctx, "FLOAT")) {
                    return parseDataTypePrecision(ctx, OracleDataType.FLOAT);
                }
                break;

            case 'i':
            case 'I':
                if (parseKeywordOrIdentifierIf(ctx, "INTERVAL")) {
                    if (parseKeywordOrIdentifierIf(ctx, "YEAR")) {
                        int yearPrecision = parseDataTypePrecision(ctx);
                        if (parseKeywordOrIdentifierIf(ctx, "TO MONTH")) {
                            return OracleDataType.INTERVAL_YEAR_TO_MONTH(yearPrecision);
                        }

                    } else if (parseKeywordOrIdentifierIf(ctx, "DAY")) {
                        int dayPrecision = parseDataTypePrecision(ctx);
                        if (parseKeywordOrIdentifierIf(ctx, "TO SECOND")) {
                            int fractionalSecondsPrecision = parseDataTypePrecision(ctx);
                            return OracleDataType.INTERVAL_DAY_TO_SECOND(
                                    dayPrecision, fractionalSecondsPrecision);
                        }
                    }
                }
                break;

            case 'l':
            case 'L':
                if (parseKeywordOrIdentifierIf(ctx, "LONG")) {
                    if (parseKeywordOrIdentifierIf(ctx, "RAW")) {
                        return OracleDataType.LONG_RAW;
                    }
                    return OracleDataType.LONG;
                }
                break;

            case 'n':
            case 'N':
                if (parseKeywordOrIdentifierIf(ctx, "NCHAR")) {
                    return parseDataTypeLength(ctx, OracleDataType.NCHAR);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "NCLOB")) {
                    return OracleDataType.NCLOB;
                }
                else if (parseKeywordOrIdentifierIf(ctx, "NUMBER")) {
                    return parseDataTypePrecisionScale(ctx, OracleDataType.NUMBER);
                }
                else if (parseKeywordOrIdentifierIf(ctx, "NVARCHAR2")) {
                    return parseDataTypeLength(ctx, OracleDataType.NVARCHAR2);
                }
                break;

            case 'r':
            case 'R':
                if (parseKeywordOrIdentifierIf(ctx, "ROW")) {
                    return parseDataTypeLength(ctx, OracleDataType.RAW);
                } else if (parseKeywordOrIdentifierIf(ctx, "ROWID")) {
                    return OracleDataType.ROWID;
                }
                break;

            case 't':
            case 'T':
                if (parseKeywordOrIdentifierIf(ctx, "TIMESTAMP")) {
                    Integer precision = parseDataTypePrecision(ctx);
                    if (parseKeywordOrIdentifierIf(ctx, "WITH TIME ZONE")) {
                        return OracleDataType.TIMESTAMP_WITH_TIMEZONE(precision);
                    }
                    else if (parseKeywordOrIdentifierIf(ctx, "WITH LOCAL TIME ZONE")) {
                        return OracleDataType.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision);
                    }
                    else {
                        return OracleDataType.TIMESTAMP(precision);
                    }
                }
                break;

            case 'u':
            case 'U':
                if (parseKeywordOrIdentifierIf(ctx, "UROWID")) {
                    return parseDataTypeLength(ctx, OracleDataType.UROWID);
                }
                break;

            case 'v':
            case 'V':
                if (parseKeywordOrIdentifierIf(ctx, "VARCHAR2")) {
                    return parseDataTypeLength(ctx, OracleDataType.VARCHAR2);
                }
                break;
        }

        throw ctx.exception("Unknown data type");
    }

}
