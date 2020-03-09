package dml;

import database.Catalog;
import storagemanager.StorageManager;

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
    public void parseDMLStatement(String statement) throws DMLParserException {}
    
    @Override
    public Object[][] parseDMLQuery(String statement) throws DMLParserException{
        //TODO THIS WILL BE IMPLEMENTED IN A LATER PHASE (Prob phase 4)
        return null;
    }

}
