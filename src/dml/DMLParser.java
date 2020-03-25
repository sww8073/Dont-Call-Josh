package dml;

import database.Catalog;
import ddl.Attribute;
import ddl.DDLParserException;
import ddl.Table;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;

import java.util.ArrayList;

public class DMLParser implements IDMLParser {

    private static Catalog catalog;
    private static StorageManager storageManager;

    public DMLParser(){}

    public DMLParser(Catalog catalog, StorageManager storageManager)  {
        DMLParser.catalog = catalog;
        DMLParser.storageManager = storageManager;
    }

    /**
     * This will create an instance of this parser and return it.
     * @return an instance of a IDMLParser
     */
    public static IDMLParser createParser(){
        return new DMLParser();
    }

    @Override
    public void parseDMLStatement(String statement) throws DMLParserException {
        String[] wordsInStatement = statement.split(" ");
        String option = wordsInStatement[0].toLowerCase();

        switch(option){
            case "insert":
                insertTable(statement);
                break;

            case "update":
                updateTable(statement);
                break;

            case "delete":
                deleteTable(statement);
                break;

            default:
                throw new DMLParserException("Command not recognized.");
        }
    }
    
    @Override
    public Object[][] parseDMLQuery(String statement) throws DMLParserException{
        //TODO THIS WILL BE IMPLEMENTED IN A LATER PHASE (Prob phase 4)
        return null;
    }

    /**
     * This function parses insert DML Statements and adds a relation to a table.
     * @param statement An insert statement as a string
     * @throws DMLParserException
     */
    public void insertTable(String statement) throws DMLParserException {
        String prefix = statement.substring(0, statement.indexOf("("));
        String[] wordsInPrefix = prefix.split("\\s+");

        if(wordsInPrefix.length != 4)
            throw new DMLParserException("Illegal insert statement prefix");
        else if(!wordsInPrefix[0].toLowerCase().equals("insert")
                || !wordsInPrefix[1].toLowerCase().equals("into")
                || !wordsInPrefix[3].toLowerCase().equals("values"))   {
            throw new DMLParserException("Illegal insert statement prefix");
        }

        String tableName = wordsInPrefix[2].toLowerCase();
        catalog.tableExists(tableName);

        // get the suffix starting with the first "(" and ending with last ")" skipping the ";"
        String suffix = statement.substring(statement.indexOf("(") - 1, statement.length() -1);

        insertRelations(tableName, suffix);
    }

    /**
     * This function inserts the each tuple into its corresponding table
     * @param tableName the name of the table being inserted into
     * @param relationString the unparsed string containing 1 or more tuple
     * @throws DMLParserException
     */
    public void insertRelations(String tableName, String relationString)  throws DMLParserException {
        relationString = relationString.replaceAll("[\\(\\)]", "");
        String[] relations = relationString.trim().split(",");

        Table table = catalog.getTable(tableName);
        if(table == null)   {
            throw new DMLParserException("Table does not exist");
        }
        String types[] = table.getDataTypes();

        for (String relation: relations) {
            try {
                int tableNum = table.getId(); // get table id

                String[] attrs = relation.trim().split(" ");
                Object[] convertedAttrs = convertTupleType(types, attrs); // converts attrs to correct Object types
                storageManager.insertRecord(tableNum, convertedAttrs);
            }
            catch (StorageManagerException e)   {
                throw new DMLParserException(e.getMessage());
            }
        }
    }

    /**
     * This function converts a array of attributes to their corresponding types
     * @param types array list of lowercase strings representing Object types
     * @param attrs array of attributes to be converted
     * @return converted array of objects
     * @throws DMLParserException
     */
    private Object[] convertTupleType(String[] types, String[] attrs)  throws DMLParserException {
        int typesLength = types.length;
        int attrLength = attrs.length;

        if(typesLength != attrLength)
            throw new DMLParserException("Incorrect amount of attributes");

        Object convertedAttrs[] = new Object[typesLength];
        for(int i = 0;i < typesLength;i++)  {
            convertedAttrs[i] = convertAttrType(types[i], attrs[i]);
        }
        return convertedAttrs;
    }

    /**
     * This function converts a single string that represents an attribute to its corresponding
     * object type
     * @param type lowercase string representing an object type
     * @param value the type needing to be converted
     * @return converted Object
     * @throws DMLParserException
     */
    private Object convertAttrType(String type, String value)  throws DMLParserException {
        try {
            if (type.equals("double"))
                return Double.parseDouble(value);
            else if (type.equals("integer"))
                return Integer.parseInt(value);
            else if (type.equals("char"))
                return value;
            else if (type.startsWith("varchar"))
                return value;
            else if (type.equals("boolean"))
                return value;
            else
                throw new DMLParserException("Could not convert " + type + " to a " + type);
        }
        catch (Exception e) {
            throw new DMLParserException("Could not convert " + type + " to a " + type);
        }
    }

    /**
     * update: All DML statements that start with this will be considered to be trying to update data in a table.
     * <name>: is the name of the table to update in. All table names are unique.
     * set<column1>=<value>, ...,<columnN>=<value> Sets the columns to the provided values.
     * <value>: attribute name, constant value, or a mathematical operation.
     * where<condition>: A condition where a tuple should updated. If this evaluates to true  the  tuple  is  updated;
     * otherwise  it remains  the  same.  See  below  for  evaluating conditionals. If there is no where clause it is
     * considered to be a where true and all tuples get updated.
     * @param statement the update statement sent to the database
     * @throws DMLParserException
     */
    public void updateTable(String statement) throws DMLParserException{
        String[] wordsInStatment = statement.split(" ");
        String tableName = wordsInStatment[1];
        String setValue = wordsInStatment[3];
        Table table;
        try{
            table = catalog.getTable(tableName);
        }
        catch(Exception e){
            throw new DMLParserException("Table \"tableName\" does not exist");
        }

        //where clause
        if(statement.contains("where")){
            //where <value> = <value>
            if(statement.contains("and") || statement.contains("or")){
                handleCondtional(statement.substring(statement.indexOf("where")));
            }
            else{
                String whereClause = statement.substring(statement.indexOf("where"));
                String[] wordsInWhereClause = whereClause.split(" ");
                String value = wordsInWhereClause[1];
                String checkValue = wordsInStatment[3];
                if(checkValue.contains(";")){
                    checkValue = new String(checkValue.substring(0,checkValue.length()-1));
                }

                //check if attribute exists
                Attribute attribute;

                try{
                    attribute = table.getAttribute(value);
                }
                catch (Exception e){
                    throw new DMLParserException("Attribute \"value\" does not exist.");
                }

                //update attribute here
            }
        }
        else{//all tuples considered to change

        }

    }

    private void handleCondtional(String statement){

    }

    public void deleteTable(String statement) throws DMLParserException{
        String[] wordsInStatement = statement.split(" ");
        String table = wordsInStatement[2];
        Table table1 = catalog.getTable(table);
        if( table1 == null){
            throw new DMLParserException("Table does not exist");
        }

        if(wordsInStatement.length == 3){
            // Where cause is true, delete all tuples table
        }
        else if(wordsInStatement.length == 7){
            // only one attribute to check
            String attribute = wordsInStatement[4];
            Attribute attribute1 = table1.getAttribute(attribute);
            if( attribute1 == null){
                throw new DMLParserException("Attribute does not exist");
            }
            String op = wordsInStatement[5];
            String value = wordsInStatement[6];
        }
        else{
            for( int i = 4; i < wordsInStatement.length; i++){
                if( (i % 4) == 0){
                    //Attribute name
                }
            }
        }
    }

}
