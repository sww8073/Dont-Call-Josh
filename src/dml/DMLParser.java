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
        // TODO check if the table is in the DB

        // get the suffix starting with the first "(" and ending with last ")" skipping the ";"
        String suffix = statement.substring(statement.indexOf("(") - 1, statement.length() -1);

        String[] relations = suffix.split(",");
    }

    public void updateTable(String statement) throws DMLParserException{}

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
