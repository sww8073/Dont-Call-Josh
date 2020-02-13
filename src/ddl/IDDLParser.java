package ddl;

/**
 * Interface representing a DDL parser
 */

public interface IDDLParser {

    /**
     * This function will parse DDL statements
     * @param statement the DDL statement to parse
     * @throws DDLParserException any error in parsing
     */
    void parseDDLstatement(String statement) throws DDLParserException;
}
