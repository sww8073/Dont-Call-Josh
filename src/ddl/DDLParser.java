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
                createTable(tableName);
                break;

            case "drop":
                dropTable(tableName);
                break;

            case "alter":
                alterTable(tableName);
                break;
        }
    }

    public void createTable(String tableName){

    }

    public void dropTable(String tableName){

    }

    public void alterTable(String tableName){

    }
}
