package ddl;

import database.Catalog;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This a a parser for DDL statements
 */

public class DDLParser implements IDDLParser {

    public static int tableIdIncrement = 0; // this will be used to generate new table ids
    private static Catalog catalog;
    private static StorageManager storageManager;

    public DDLParser(){}

    public DDLParser(Catalog catalog, StorageManager storageManager)  {
        this.catalog = catalog;
        this.storageManager = storageManager;
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

        if(catalog.tableExists(tableName))  {
            throw new DDLParserException("Table already exists");
        }
        catalog.addTable(table);

        // create table in storage manager
        try {
            storageManager.addTable(table.getId(), table.getDataTypes(), table.getKeyIndices());
        }
        catch (StorageManagerException e) {
            throw new DDLParserException(e.getMessage());
        }
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

    /**
     * This function takes an attribute in a non-query and parses it
     * @param attribute un-parse non-query attribute
     * @param table the table in which the information needs to be added
     * @return the updated table
     * @throws DDLParserException
     */
    public Table prefixConstraint(String attribute, Table table) throws DDLParserException  {
        String[] elements = attribute.split("[\\(\\)\\,\\s]");
        String option = elements[0];

        switch(option){
            case "unique":
                for (int i = 1; i < elements.length; i++){
                    if(!elements[i].equals("") && elements[i] != null){
                        if(!table.attributeExists(elements[i]))
                            throw new DDLParserException("Cannot make an attribute unique that doesnt exist");
                        table.addUnqiueAttribute(elements[i]);
                    }
                }
                break;
            case "primarykey":
                for (int i = 1; i < elements.length; i++){
                    if(!elements[i].equals("") && elements[i] != null)  {
                        if(!table.attributeExists(elements[i]))
                            throw new DDLParserException("Cannot make a primary key on attribute that doesnt exist");
                        table.addPrimaryKey(elements[i]);
                    }
                }
                break;
            case "foreignkey":
                table = addForeignKey(attribute, table);
                break;
            default:
                throw new DDLParserException("Prefix constraint does not exist.");
        }
        return table;
    }

    /**
     * This function parses the foreign key statements and adds references to both tables.
     * @param attribute foreign key string
     * @param table the table the foreign key is being added to
     * @return the updated table
     * @throws DDLParserException
     */
    public Table addForeignKey(String attribute, Table table) throws DDLParserException  {
        attribute = attribute.replaceAll("\\(", " ");
        attribute = attribute.replaceAll("\\)", " ");
        String[] elements = attribute.split("\\s+");

        ArrayList<String> keyAttr = new ArrayList<>(); // key attributes for this table
        ArrayList<String> foreignKeyAttr = new ArrayList<>(); // key attributes for the table being referenced

        // skip first value and add attributes to list until references keyword found
        // or index is smaller than the max array index
        int i = 1;
        while(!elements[i].toLowerCase().equals("references") && i < elements.length)   {
            keyAttr.add(elements[i].toLowerCase());
            i++;
        }

        i++; // skip "references"
        String foreignTableName = elements[i]; // name of the table being referenced
        i++; // skip table name
        // adds the remaining attributes to the key indices of the other table
        while(i < elements.length)   {
            foreignKeyAttr.add(elements[i].toLowerCase());
            i++;
        }

        if(!catalog.tableExists(foreignTableName))
            throw new DDLParserException("Cannot make a foreign key to a table that does not exist");

        ForeignKey foreignKey = new ForeignKey(table.getName(), keyAttr, foreignTableName, foreignKeyAttr);
        table.addForeignKey(foreignKey); // adds foreign key to table that normally contains it

        // tells the table that is being referenced that there is a foreign key associated with it
        catalog.addForeignKeyReference(foreignKey);
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
            if(elementStr.equals("primarykey")) {
                table.addPrimaryKey(name);
            }
            else if(elementStr.equals("unique"))    {
                table.addUnqiueAttribute(name);
            }

        }

        table.addAttribute(attribute);
        return table;
    }

    /**
     * this function drops an entire table
     * @param statement un-parsed statement
     * @throws DDLParserException
     */
    public void dropTable(String statement) throws DDLParserException {
        String[] wordsInStatement = statement.split(" ");
        if(wordsInStatement.length != 3){
            throw new DDLParserException("Invalid Drop Table Statement");
        }
        else{
            String table = wordsInStatement[2];
            String tableName = table.substring(0, table.length() - 1);
            Table table1 = catalog.getTable(tableName);
            if(table1 == null){
                throw new DDLParserException("Table does not exist");
            }
            int tableIndex = table1.getId();
            try{
                storageManager.dropTable(tableIndex);
            }catch(StorageManagerException e){
                throw new DDLParserException("Error dropping table from storage manager");
            }
            catalog.dropTable(tableName);
        }
    }

    /**
     * This function parses alter table statements
     * @param statement un-parsed statement
     * @throws DDLParserException
     */
    public void alterTable(String statement) throws DDLParserException{
        String[] wordsInStatement = statement.split(" ");

        //if the word after alter is "table" continue, else throw an error
        if(wordsInStatement[1].toLowerCase().equals("table")) {
            String tableName = wordsInStatement[2];
            //table exists
            if (catalog.tableExists(tableName)) {

                Table table = catalog.getTable(tableName);

                String addDropOption = wordsInStatement[3].toLowerCase();

                switch (addDropOption){
                    case "add":

                        String attrName = wordsInStatement[4].toLowerCase();
                        String attrType = wordsInStatement[5].toLowerCase();

                        if(wordsInStatement.length > 6){
                            if(wordsInStatement[6].toLowerCase().equals("default")){
                                //TODO add attribute to catalog
                                String defaultValue = wordsInStatement[7].toLowerCase();// the default value
                                Attribute attribute = new Attribute(attrName, attrType);
                                makeNewTable(table, defaultValue, attrType);
                            }
                        }
                        else{
                            attrType = attrType.substring(0, attrType.length() - 1);
                            //TODO add atribute to catalog
                            Attribute attribute = new Attribute(attrName, attrType);//new attr to add
                            makeNewTable(table,null,attrType);
                        }
                        break;

                    case "drop":
                        Table oldTable = catalog.getTable(tableName);
                        String attr = wordsInStatement[4];
                        attr = new String(attr.substring(0,attr.length()-1));
                        if (!oldTable.attributeExists(attr)) {
                            throw new DDLParserException("Attribute "+ attr + " does not exist.");
                        }
                        Attribute oldAttr = oldTable.getAttribute(attr);
                        if (oldAttr.isPrimary()) {
                            throw new DDLParserException("Attribute " + attr + " is a primary key and cannot be dropped.");
                        }
                        dropAttribute(oldTable, attr);
                        break;

                    default:
                        throw new DDLParserException("Unknown option for alter table.");
                }
            }
            else{
                throw new DDLParserException("Table " + tableName + " does not exist.");
            }
        }
        else{
            throw new DDLParserException("Incorrect syntax for alter statement.");
        }
    }

    /**
     * Drops an attribute from a table
     * @param table the table to modify
     * @param attr the table to drop
     * @throws DDLParserException The attribute is a key
     */
    private void dropAttribute(Table table, String attr) throws DDLParserException {
        int attrLoc = table.indexOfAttribute(attr);
        table.dropAttribute(attr);
        Object[][] oldRecords = readTable(table);

        String[] dataTypes = table.getDataTypes();
        Integer[] keyIndices = table.getKeyIndices();
        int tableID = table.getId();

        dropTable2(table);
        int newAttrNum = oldRecords[0].length - 1;
        int oldAttrNum  = oldRecords[0].length;
        int relationNum = oldRecords.length;
        // This is our new object array of records.
        Object[][] newRecords = new Object[relationNum][newAttrNum];
        for (int i = 0; i < relationNum; i++) {
            int newJ = 0;
            for (int j = 0; j < oldAttrNum; j++) {
                if (j == attrLoc) {
                    newJ--;
                } else {
                    newRecords[i][newJ] = oldRecords[i][j];
                }
                newJ++;
            }
        }
        try {
            storageManager.addTable(tableID, dataTypes, keyIndices);
            for (Object[] record: newRecords) {
                storageManager.insertRecord(tableID, record);
            }
        } catch (StorageManagerException e) {}
        catalog.dropTable(table.getName());
    }

    private void makeNewTable(Table table, String value, String attrType){
        try{

            Object[][] oldtable = readTable(table);

            String[] dataTypes = table.getDataTypes();
            Integer[] keyIndices = table.getKeyIndices();
            int tableID = table.getId();

            dropTable2(table);

            int newAttrNum = oldtable[0].length + 1;
            int relationNum = oldtable.length;
            Object[][] newTable = new Object[relationNum][newAttrNum];

            if(value == null) {//has no default value
                switch(attrType){
                    case "double":
                        Double doubleVal = null;
                        for (int i = 0; i < relationNum; i++) {
                            for (int j = 0; j < newAttrNum - 1; j++) {
                                newTable[i][j] = oldtable[i][j];
                            }
                            newTable[i][newAttrNum-1] = doubleVal;
                        }
                        break;
                    case "integer":
                        Integer integerVal = null;
                        for (int i = 0; i < relationNum; i++) {
                            for (int j = 0; j < newAttrNum - 1; j++) {
                                newTable[i][j] = oldtable[i][j];
                            }
                            newTable[i][newAttrNum-1] = integerVal;
                        }
                        break;
                    case "char":
                        String charVal = null;
                        for (int i = 0; i < relationNum; i++) {
                            for (int j = 0; j < newAttrNum - 1; j++) {
                                newTable[i][j] = oldtable[i][j];
                            }
                            newTable[i][newAttrNum-1] = charVal;
                        }
                        break;
                    case "varchar":
                        String varcharVal = null;
                        for (int i = 0; i < relationNum; i++) {
                            for (int j = 0; j < newAttrNum - 1; j++) {
                                newTable[i][j] = oldtable[i][j];
                            }
                            newTable[i][newAttrNum-1] = varcharVal;
                        }
                        break;
                }
            }
            else{
                switch(attrType){
                    case "double":
                        Double doubleVal = Double.parseDouble(value);
                        for (int i = 0; i < relationNum; i++) {
                            for (int j = 0; j < newAttrNum - 1; j++) {
                                newTable[i][j] = oldtable[i][j];
                            }
                            newTable[i][newAttrNum-1] = doubleVal;
                        }
                        break;
                    case "integer":
                        value = new String(value.substring(0,value.length()-1));
                        Integer integerVal = Integer.parseInt(value);
                        for (int i = 0; i < relationNum; i++) {
                            for (int j = 0; j < newAttrNum - 1; j++) {
                                newTable[i][j] = oldtable[i][j];
                            }
                            newTable[i][newAttrNum-1] = integerVal;
                        }
                        break;
                    case "char":
                        for (int i = 0; i < relationNum; i++) {
                            for (int j = 0; j < newAttrNum - 1; j++) {
                                newTable[i][j] = oldtable[i][j];
                            }
                            newTable[i][newAttrNum-1] = value;
                        }
                        break;
                    case "varchar":
                        for (int i = 0; i < relationNum; i++) {
                            for (int j = 0; j < newAttrNum - 1; j++) {
                                newTable[i][j] = oldtable[i][j];
                            }
                            newTable[i][newAttrNum-1] = value;
                        }
                        break;
                }
            }

            try {
                storageManager.addTable(tableID, dataTypes, keyIndices);
                for (Object[] record: newTable) {
                    storageManager.insertRecord(tableID, record);
                }
            }
            catch(StorageManagerException e){}

        }
        catch (DDLParserException e){}

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
    private void dropTable2(Table table) throws DDLParserException {
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
