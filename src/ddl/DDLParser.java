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


        //get things for each attribute
        for (String attribute : attributesSplit) {
            String[] attributeTypes = attribute.split("\\s+");
            int attrSize = attributeTypes.length;

            String name = attributeTypes[0];
            if (name.contains("(")) {
                name = new String(name.substring(0, name.indexOf("(")));
            }
            if (keyWordsList.contains(name.toLowerCase())) { // contains a key
                switch (attributeTypes[attrSize - 1].toLowerCase()) {
                    case "primarykey":
                        System.out.println("primarykey");
                        break;

                    case "unique":
                        System.out.println("foriegnkey");
                        break;
                }
            } else { //
                if (constraintList.contains(attributeTypes[attrSize - 1].toLowerCase())) { // contains special constraints
                    for (int i = 0; i < attrSize; i++) {
                        switch (attributeTypes[i].toLowerCase()) {
                            case "notnull":
                                System.out.println("notnull");
                                break;
                            case "unique":
                                System.out.println("unique");
                                break;
                            case "primarykey":
                                System.out.println("unique");
                                break;
                        }
                    }
                } else if (attrSize > 2)
                    throw new DDLParserException("Invalid constraint");
                else if (!typesList.contains(attributeTypes[1].toLowerCase()))
                    throw new DDLParserException("Invalid data type");
                dataTypes.add(attributeTypes[1].toLowerCase());
            }
        }
    }

    public void dropTable(String statement){

    }

    public void alterTable(String statement){

    }

}
