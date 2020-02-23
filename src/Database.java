import ddl.DDLParser;
import ddl.DDLParserException;
import ddl.IDDLParser;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;

import java.io.File;

/**
 * Class to create and access a database.
 */

public class Database implements IDatabase{

    private static AStorageManager storageManager;
    private static IDDLParser iddlParser;
    /**
     * Static function that will create/restart and return a database
     * @param dbLoc the location to start/restart the database in
     * @param pageBufferSize the size of the page buffer; max number of pages allowed in the buffer at any given time
     * @param pageSize the size of a page in bytes
     * @return an instance of an IDatabase.
     */
    public static IDatabase getConnection(String dbLoc, int pageBufferSize, int pageSize ) throws StorageManagerException {
        String temp = dbLoc + "\\database.txt";
        File restart = new File(temp);
        if(restart.exists()){
            storageManager = new StorageManager(dbLoc, pageBufferSize, pageSize, true);
        }
        else{
            storageManager = new StorageManager(dbLoc, pageBufferSize, pageSize, false);
        }
        iddlParser = DDLParser.createParser();
        return new Database();
    }

    /**
     * This method will be used when executing database statements that do not return anything.
     *
     * @param statement the statement to execute
     */
    public void executeNonQuery(String statement) {
        // TODO In later phases this needs to be updated to differentiate between ddl and dml
        try{
            iddlParser.parseDDLstatement(statement);
        }catch(DDLParserException e){
            //TODO change this exception maybe?
            e.printStackTrace();
        }
    }

    /**
     * This method will be used when executing database queries that return tables of data.
     *
     * @param query the query to execute
     * @return the table of data returned. Size zero if empty
     * THIS SHOULD NOT BE IMPLEMENTED IN PHASE2 KEEP THIS EMPTY
     */
    public Object[][] executeQuery(String query) {
        return new Object[0][];
    }

    /**
     * This method will be used to safely shutdown the database.
     * It will store any needed data needed to restart the database to physical hardware.
     */
    public void terminateDatabase() {
        try {
            storageManager.purgeBuffer();
            storageManager.terminateDatabase();
        }
        catch(StorageManagerException e){
            //TODO change this exception maybe?
            e.printStackTrace();
        }
    }
}
