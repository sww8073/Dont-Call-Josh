package ddl;

/**
 * This a a parser for DDL statements
 */

public class DDLParser implements IDDLParser {

    /**
     * This will create an instance of this parser and return it.
     * @return an instance of a IDDLParser
     */
    public static IDDLParser createParser(){
        return null;
    }

    public void parseDDLstatement(String statement) throws DDLParserException {
        String[] wordsInStatement = statement.split(" ");
        String option = wordsInStatement[0];
        String tableName = wordsInStatement[1];

        switch(option){
            case "create":
                createTable(statement);
                break;

            case "drop":
                dropTable(statement);
                break;

            case "alter":
                alterTable(statement);
                break;

            default:
                break;
        }
    }

    /**
     * This function will parse create table arguments, and use the storage manager
     * @param statement
     */
    public void createTable(String statement) throws DDLParserException {
        String[] wordsInStatement = statement.split(" ");
        String tableName = wordsInStatement[3];
    }

    public void dropTable(String statement){

    }

    public void alterTable(String statement){

    }
}
