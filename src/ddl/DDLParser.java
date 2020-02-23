package ddl;

import javafx.scene.control.Tab;
import storagemanager.StorageManager;

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

        tableIdIncrement++;
        Table table = new Table(tableIdIncrement, tableName);
        for (String attribute : attributesSplit)    {
            table = parseAttribute(attribute, table);
        }

        //TODO add table to storage manager
    }

    /**
     * This function parses a attribute string. The attribute is added to the table with all of its constraints
     * @param attribute the string
     * @param table the Table in which the attribute will be added
     * @return the Table with the added attribute
     * @throws DDLParserException
     */
    private Table parseAttribute(String attribute, Table table) throws DDLParserException  {

        return null;
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
                    table.addAttribute(attribute);
                    break;
            }
        }
        return table;
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
        return table;
    }


    public void dropTable(String statement) throws DDLParserException {
        String[] wordsInStatement = statement.split(" ");
        if(wordsInStatement.length != 3){
            throw new DDLParserException("Invalid Drop Table Statement");
        }
        else{
            String table = wordsInStatement[2];
            String tableName = table.substring(0, table.length() - 1);
            //TODO Given table name, get ID and call storagemanager
            //Integer tableID =
        }
    }

    public void alterTable(String statement) throws DDLParserException{
        String[] wordsInStatement = statement.split(" ");

        //if the word after alter is "table" continue, else throw an error
        if(wordsInStatement[1].toLowerCase().equals("table")) {

            String tableName = wordsInStatement[2];

            //table exists
            if (true) {

                //TODO get table from DB

                String addDropOption = wordsInStatement[3];

                switch (addDropOption){

                    case "add":
                        break;

                    case "drop":
                        break;

                    default:
                        throw new DDLParserException("Unknown option for alter table.");
                }
            }
            else{
                throw new DDLParserException("Table does not exist.");
            }
        }
        else{
            throw new DDLParserException("Incorrect syntax for alter statement.");
        }
    }

}
