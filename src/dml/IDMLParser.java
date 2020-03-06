package dml;

/**
 * Interface representing a DML parser
 */
public interface IDMLParser {

    /**
     * This function will parse DML statments that do not expect a return
     * @param statement the DML statement to parse
     * @throws DMLParserException any error in parsing
     */
    void parseDMLStatement(String statement) throws DMLParserException;

    /**
     * This will parse DML statements that expect a table as a return.
     * @param statement the DML statement to parse
     * @return the table of data returned. Size zero if empty
     * @throws DMLParserException any error in parsing
     */
    Object[][] parseDMLQuery(String statement) throws DMLParserException;
}
