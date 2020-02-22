package ddl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This a a parser for DDL statements
 */

public class DDLParser implements IDDLParser {

    public static int tableIdIncrement = 0; // this will be used to generate new table ids

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
    public void createTable(String statement) throws DDLParserException {
        String prefix = statement.substring(0, statement.indexOf("("));
        String[] wordsInPrefix = prefix.split("\\s+");

        // check for incorrect create table statement
        if (wordsInPrefix.length != 3)
            throw new DDLParserException("Invalid create table query");
        else if (wordsInPrefix[1].compareToIgnoreCase("table") != 0)
            throw new DDLParserException("Invalid create table query");

        String tableName = wordsInPrefix[2];

        //splits statement into attributes
        String attributes = statement.substring(statement.indexOf("(") + 1, statement.indexOf(");"));
        String[] attributesSplit = attributes.split(",");

        String[] keyWords = {"primarykey", "foreignkey"};
        List<String> keyWordsList = Arrays.asList(keyWords);

        boolean keyConstraintsFirst = false;
        for (String attribute : attributesSplit) {
            String[] attributeTypes = attribute.split("\\s+");
            String first = attributeTypes[0].toLowerCase();
            first = first.replaceAll("[^a-zA-Z0-9]", ""); // remove all special characters

            if(keyWordsList.contains(first))    {
                keyConstraintsFirst = true;
            }
        }

        tableIdIncrement++;
        Table table = new Table(tableIdIncrement, tableName);
        if(keyConstraintsFirst) {
            table = createTableKeysFirst(attributesSplit, table);
        }
        else    {
            table = createTableKeysLast(attributesSplit, table);
        }
        // TODO add table to database
    }

    /**
     * This function takes a attribute string parses it and creates an attribute
     * @param attr attr string
     * @return a populated Attribute object
     */
    public Attribute parseAttributeNoPrimary(String attr) throws DDLParserException   {
        String[] elements = attr.split("\\s+");
        if(elements.length < 2) {
            throw new DDLParserException("Invalid attribute");
        }

        Attribute attribute = new Attribute(elements[0], elements[1]);
        for(int i = 2;i < elements.length;i++)  {
            attribute.addConstraintNoPrimary(elements[i]); // checks for primary keys and errors if there is one
        }
        return attribute;
    }

    /**
     * Create a table with the key constraints listed separately than data type.
     * ex: primarykey( bar baz )
     * @param attributes List of attributes
     * @param table
     * @return updated Table object
     */
    public Table createTableKeysFirst(String[] attributes, Table table) throws DDLParserException   {
        for (String attr : attributes)    {
            String[] elements = attr.split("[(\\(\\)]");
            switch (elements[0].toLowerCase())  {
                case "notnull":
                    System.out.println("notnull");
                    break;
                case "unique":
                    System.out.println("unique");
                    break;
                case "primarykey":
                    System.out.println("primarykey");
                    break;
                case "foreignkey":
                    System.out.println("foriegnkey");
                    break;
                default:
                    Attribute attribute = parseAttributeNoPrimary(attr);
                    int i = 0;
                    i++;
                    break;
            }
        }
        return null;
    }

    /**
     * Create a table with the key constraints on the same line as data types.
     * ex: foo char(5) primarykey
     * @param attributes List of attributes
     * @param table
     * @return updated Table object
     */
    public Table createTableKeysLast(String[] attributes, Table table)   {
        // TODO Josh T
        return null;
    }


    public void dropTable(String statement){

    }

    public void alterTable(String statement){

    }

}
