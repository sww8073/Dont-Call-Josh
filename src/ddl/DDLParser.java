package ddl;

import javafx.scene.control.ScrollBar;
import javafx.scene.control.Tab;
import storagemanager.StorageManager;

import java.util.Arrays;
import java.util.List;

/**
 * This a a parser for DDL statements
 */

public class DDLParser implements IDDLParser {

    public static int tableIdIncrement = 0; // this will be used to generate new table ids
    private static StorageManager storageManager;
    private List<String> types = Arrays.asList("integer", "char", "varchar", "double");

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
                table.printUniqueAttrs();
                break;

            case "primarykey":
                break;

            case "foreignkey":
                break;

            default:
                throw new DDLParserException("Prefix constraint does not exist.");
        }

        return table;
    }

    public Table postfixConstraint(String attribute, Table table) throws DDLParserException   {
        String[] elements = attribute.split("[\\(\\)\\s+]");

        if(elements.length < 2)     {
            throw new DDLParserException("Not enough attribute elements");
        }
        String name = elements[0];
        String type = elements[1];
        if(!isValidType(type))      {
            throw new DDLParserException("Invalid type");
        }

        //Attribute attribute =

        for(int i = 2;i < elements.length;i++)  {
            if(!isConstraintValid(elements[i])) {
                //throw new DDLParserException("Invalid constraint");
            }
        }

        return table;
    }

    /**
     * This function removes the () and number from a type and checks to see if it is a valid type
     * @param type a type string. ex varchar(4), integer
     * @return true if type is valid
     */
    private boolean isValidType(String type)    {
        type = type.toLowerCase();
        type = type.replaceAll("[^a-zA-Z0-9]", ""); // remove special characters
        type = type.replaceAll("\\d", ""); // removes numbers
        if(types.contains(type))
            return true;
        else
            return false;
    }

    /**
     * This Function checks if a constraint is valid
     * @param constraint constraint to be tested
     * @return true if constraint is valid
     */
    private boolean isConstraintValid(String constraint)  {
        List<String> constraints = Arrays.asList("primarykey", "foriegnkey", "unique", "notnull");
        constraint = constraint.toLowerCase();
        if(constraints.contains(constraint))
            return true;
        else
            return false;
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
