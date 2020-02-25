package ddl;

import database.Catalog;
import storagemanager.StorageManager;
import java.util.Arrays;
import java.util.List;

/**
 * This a a parser for DDL statements
 */

public class DDLParser implements IDDLParser {

    public static int tableIdIncrement = 0; // this will be used to generate new table ids
    private static StorageManager storageManager;
    private static Catalog catalog;
    private List<String> types = Arrays.asList("integer", "char", "varchar", "double");

    public DDLParser(){}

    public DDLParser(StorageManager storageManager1, Catalog catalog1){
        storageManager = storageManager1;
        catalog = catalog1;
    }
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
        String[] elements = attribute.split("[\\(\\)\\s+]");
        switch (elements[0].toLowerCase()) {
            case "unique":
                table = prefixConstraint(attribute, table);
                break;
            case "primarykey":
                table = prefixConstraint(attribute, table);
                break;
            case "foreignkey":
                table = prefixConstraint(attribute, table);
                break;
            default:
                table = postfixConstraint(attribute, table);
                break;
        }
        return table;
    }

    public Table prefixConstraint(String attribute, Table table) throws DDLParserException  {

        String[] elements = attribute.split("[\\(\\)\\,\\s]");
        String option = elements[0];

        switch(option){
            case "unique":
                System.out.println(option);
                for (int i = 1; i < elements.length; i++){
                    if(!elements[i].equals(" ") && elements[i] != null){
                        table.addUnqiueAttribute(elements[i]);
                    }
                }
                break;

            case "primarykey":
                for (int i = 1; i < elements.length; i++){
                    if(!elements[i].equals(" ") && elements[i] != null){
                        table.addPrimaryKey(elements[i]);
                    }
                }
                break;

            case "foreignkey":
                break;

            default:
                throw new DDLParserException("Prefix constraint does not exist.");
        }

        return table;
    }

    /**
     * This will add attributes to a table with constraints
     * @param attributeStr the attribute string to be added to a table
     * @param table the Table Object
     * @return the updated Table Object
     * @throws DDLParserException
     */
    public Table postfixConstraint(String attributeStr, Table table) throws DDLParserException   {
        String[] elements = attributeStr.split("[\\(\\)\\s+]");

        if(elements.length < 2)     {
            throw new DDLParserException("Not enough attribute elements");
        }
        String name = elements[0];
        String type = elements[1];

        Attribute attribute = new Attribute(name, type);
        for(int i = 2;i < elements.length;i++)  {
            attribute.addConstraint(elements[i]); // all constraints are added

            // check for special constraint and add them to the table
            String elementStr = elements[i];
            elementStr = elementStr.toLowerCase();
            if(elementStr.equals("foreignkey")) {
                table.addPrimaryKey(name);
            }
            else if(elementStr.equals("unique"))    {
                table.addUnqiueAttribute(name);
            }

        }
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
