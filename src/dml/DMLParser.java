package dml;

import database.Catalog;
import ddl.Attribute;
import ddl.Table;
import storagemanager.StorageManager;

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

        for (String relation: relations) {
            String[] elements = relation.trim().split(" ");
             int i = 0;
             i++;

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
