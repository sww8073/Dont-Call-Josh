package ddl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This a a parser for DDL statements
 */

public class DDLParser implements IDDLParser {

    /**
     * This will create an instance of this parser and return it.
     * @return an instance of a IDDLParser
     */
    public static IDDLParser createParser(){
        return new DDLParser();
    }

    public void parseDDLstatement(String statement) throws DDLParserException {
        String[] wordsInStatement = statement.split(" ");
        String option = wordsInStatement[0].toLowerCase();

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
                throw new DDLParserException("Command not recognized.");
        }
    }

    /**
     * This function will parse create table arguments, and use the storage manager
     * @param statement create table statement
     */
    public void createTable(String statement) throws DDLParserException {String prefix = statement.substring(0, statement.indexOf("("));
        String[] wordsInPrefix = prefix.split("\\s+");

        // check for incorrect create table statement
        if (wordsInPrefix.length != 3)
            throw new DDLParserException("Invalid create table query");
        else if (wordsInPrefix[1].compareToIgnoreCase("table") != 0)
            throw new DDLParserException("Invalid create table query");

        String tableName = wordsInPrefix[2];
        ArrayList<String> dataTypes = new ArrayList<>(); // list that contains data types
        ArrayList<String> keyIndices = new ArrayList<>(); // list that contains data types

        //splits statement into attributes
        String attributes = statement.substring(statement.indexOf("(") + 1, statement.indexOf(");"));
        String[] attributesSplit = attributes.split(",");

        String[] keyWords = {"primarykey", "foreignkey"};
        List<String> keyWordsList = Arrays.asList(keyWords);
        String[] constraints = {"unique", "notnull", "primarykey"};
        List<String> constraintList = Arrays.asList(constraints);
        String[] types = {"double", "integer", "char", "varchar"};
        List<String> typesList = Arrays.asList(types);

        boolean keyConstraintsFirst = false;
        //get things for each attribute
        for (String attribute : attributesSplit) {
            String[] attributeTypes = attribute.split("\\s+");
            String first = attributeTypes[0].toLowerCase();
            first = first.replaceAll("[^a-zA-Z0-9]", ""); // remove all special characters

            if(keyWordsList.contains(first))    {
                keyConstraintsFirst = true;
            }
        }

        if(keyConstraintsFirst) {
            createTableKeysFirst(attributesSplit);
        }
        else    {
            createTableKeysLast(attributesSplit);
        }
    }

    /**
     * Create a table with the key constraints listed separately than data type.
     * ex: primarykey( bar baz )
     * @param attributes List of attributes
     */
    public void createTableKeysFirst(String[] attributes)   {

    }

    /**
     * Create a table with the key constraints on the same line as data types.
     * ex: foo char(5) primarykey
     * @param attributes List of attributes
     */
    public void createTableKeysLast(String[] attributes)   {
        // TODO Josh T
    }


    public void dropTable(String statement){

    }

    public void alterTable(String statement){

    }

}
