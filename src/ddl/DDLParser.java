package ddl;

import database.Catalog;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;

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

        catalog.addTable(table);
        int i = 0;
        i++;
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
                    if(!elements[i].equals("") && elements[i] != null){
                        table.addPrimaryKey(elements[i]);
                    }
                }
                break;
            case "foreignkey":
                if(catalog.tableExists(table.getName())){
                    if (elements[2].toLowerCase().equals("references")) {
                        if(catalog.tableExists(elements[3])){
                            Table referenceTable = catalog.getTable(elements[3]);
                            String referenceAttr = elements[4];
                            if(referenceTable.attributeExists(referenceAttr)){
                                //TODO add foreign key to table here
                            }
                            else{
                                throw new DDLParserException("Reference attribute " + referenceAttr + " does not" +
                                        "exist in the table " + referenceTable.toString() + ".");
                            }
                        }
                        else{
                            throw new DDLParserException("Reference table " + elements[3] + " does not exist.");
                        }
                    }
                    else{
                        throw new DDLParserException("Syntax error for foreign key statement, missing reference.");
                    }
                }
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
        String[] elements = attributeStr.split("[\\s+]");

        if(elements.length < 2)     {
            throw new DDLParserException("Not enough attribute elements");
        }
        String name = elements[0];
        String type = elements[1];

        Attribute attribute = new Attribute(name, type);
        for(int i = 2;i < elements.length;i++)  {
            // this check fore the "notnull" constraint written as "not null"
            if(elements[i].toLowerCase().equals("not")) {
                if(i + 1 < elements.length) {
                    if(elements[i+1].toLowerCase().equals("null"))  {
                        attribute.addConstraint("notnull"); // all constraints are added
                        i++;
                    }
                    else
                        throw new DDLParserException(elements[i] + " is an invalid constraint");
                }
                else
                    throw new DDLParserException(elements[i] + " is an invalid constraint");
            }
            else {
                attribute.addConstraint(elements[i]); // all constraints are added
            }

            // check for special constraint and add them to the table
            String elementStr = elements[i];
            elementStr = elementStr.toLowerCase();
            if(elementStr.equals("foreignkey")) {
                // table.addPrimaryKey(name);
                // TODO add forignkey
            }
            else if(elementStr.equals("primarykey")) {
                table.addPrimaryKey(name);
            }
            else if(elementStr.equals("unique"))    {
                table.addUnqiueAttribute(name);
            }

        }

        table.addAttribute(attribute);
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
            Table table1 = catalog.getTable(tableName);
            int tableIndex = table1.getId();
            try{
                storageManager.dropTable(tableIndex);
            }catch(StorageManagerException e){
                throw new DDLParserException("Error dropping table from storage manager");
            }
            catalog.dropTable(tableName);
        }
    }

    public void alterTable(String statement) throws DDLParserException{
        String[] wordsInStatement = statement.split(" ");

        //if the word after alter is "table" continue, else throw an error
        if(wordsInStatement[1].toLowerCase().equals("table")) {
            String tableName = wordsInStatement[2];
            //table exists
            if (catalog.tableExists(tableName)) {
                Table table = catalog.getTable(tableName);
                String addDropOption = wordsInStatement[3];
                switch (addDropOption){
                    case "add":
                        String attrName = wordsInStatement[4];
                        String attrType = wordsInStatement[5];
                        Object def = null;
                        Attribute attribute = new Attribute(attrName, attrType);
                        if(wordsInStatement.length > 5){
                            if(wordsInStatement[6].toLowerCase().equals("default")){
                                def = wordsInStatement[7];
                            }
                        }

                        break;
                    case "drop":
                        Table oldTable = catalog.getTable(tableName);
                        String attr = wordsInStatement[4];
                        Attribute oldAttr = oldTable.getAttribute(attr);
                        if (oldAttr.isPrimary()) {
                            throw new DDLParserException("Attribute " + attr + " is a primary key and cannot be dropped.");
                        }
                        oldTable.dropAttribute(attr);
                        Object[][] records = readTable(oldTable);
                        dropTable(oldTable);
                        // TODO modify table
                        // TODO create new table
                        // TODO add modified records
                        catalog.dropTable(tableName);
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

    /**
     * This function gets all the records from the table being altered. Inserts the new record
     * into the relations. Drops the old table and creates a new
     * @param table Table being changed
     * @param type the type being added
     * @param def the default value, possibly null
     */
    public void addAttr(Table table, String type, Object def) throws DDLParserException {
        try {
            Object[][] records = storageManager.getRecords(table.getId());

        }
        catch (StorageManagerException  e)      {
            throw new DDLParserException("unable to add attribute to table");
        }
    }


    /**
     * This function check if a tape is valid and returns the corresponding object form of the type/
     * @param type the type if the attribute
     * @param value the value of the attribute
     * @return an Object of the corresponding value
     * @throws DDLParserException
     */
    public Object isValidType(String type, String value) throws DDLParserException {
        String[] typeArr = {"double", "integer", "char", "varchar"};
        List<String> typeList = Arrays.asList(typeArr);

        type = type.toLowerCase();
        type = type.replaceAll("[^a-zA-Z0-9]", ""); // remove special characters
        type = type.replaceAll("\\d", ""); // remove digits

        try {
            if(type.equals("double"))
                return Double.parseDouble(value);
            else if(type.equals("integer"))
                return Integer.parseInt(value);
            else if(type.equals("char"))
                return value;
            else if(type.equals("varchar"))
                return value;
            else
                throw new DDLParserException(type + " is an invalid type");
        }
        catch (Exception e) {
            throw new DDLParserException("unable to add attribute " + value + " as a type");
        }
    }

    /**
     * Drops a table from the database using the storage manager
     * @param table the table to drop
     * @throws DDLParserException the table does not exist
     */
    private void dropTable(Table table) throws DDLParserException {
        try {
            storageManager.dropTable(table.getId());
        } catch (Exception e) {
            throw new DDLParserException("Table does not exist");
        }
    }

    /**
     * Gets all records from a table
     * @param table the table to get records from
     * @return the records
     * @throws DDLParserException the table does not exist
     */
    private Object[][] readTable(Table table) throws DDLParserException {
        int tableID = table.getId();
        try {
            return storageManager.getRecords(tableID);
        } catch (Exception e) {
            throw new DDLParserException("Table does not exist");
        }
    }
}
