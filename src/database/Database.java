package database;
import ddl.DDLParser;
import ddl.DDLParserException;
import dml.DMLParser;
import dml.DMLParserException;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;
import java.io.*;

/**
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */

public class Database implements IDatabase {

    private static StorageManager storageManager;
    private static DDLParser iddlParser;
    private static DMLParser idmlParser;
    private static Catalog catalog;
    private static String db;
    /**
     * Static function that will create/restart and return a database
     * @param dbLoc the location to start/restart the database in
     * @param pageBufferSize the size of the page buffer; max number of pages allowed in the buffer at any given time
     * @param pageSize the size of a page in bytes
     * @return an instance of an database.IDatabase.
     */
    @SuppressWarnings("unchecked")
    public static IDatabase getConnection(String dbLoc, int pageBufferSize, int pageSize ) {
        String temp = dbLoc + "sm.dat";
        File restart = new File(temp);
        db = dbLoc;
        String catalogLoc = dbLoc + "catalog.txt";
        try{
            if(restart.exists()){
                storageManager = new StorageManager(dbLoc, pageBufferSize, pageSize, true);
            }
            else{
                storageManager = new StorageManager(dbLoc, pageBufferSize, pageSize, false);
            }
        }catch(StorageManagerException e){
            e.printStackTrace();
        }
        File f = new File(catalogLoc);
        if( f.exists() && !f.isDirectory()){
            try{
                FileInputStream in = new FileInputStream(catalogLoc);
                ObjectInputStream ois = new ObjectInputStream(in);
                catalog = (Catalog) ois.readObject();
            }
            catch(IOException e){
                System.out.println("File not found");
            }
            catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        else{
            catalog = new Catalog();
        }
        iddlParser = new DDLParser(catalog, storageManager);
        idmlParser = new DMLParser(catalog, storageManager);
        return new Database();
    }

    /**
     * This method will be used when executing database statements that do not return anything.
     *
     * @param statement the statement to execute
     */
    public void executeNonQuery(String statement) {
        try{
            String[] wordsInStatement = statement.split(" ");
            String option = wordsInStatement[0].toLowerCase();

            if( option.equals("create") || option.equals("drop") || option.equals("alter")){
                iddlParser.parseDDLstatement(statement);
            }
            else if( option.equals("insert") || option.equals("delete") || option.equals("update")){
                idmlParser.parseDMLStatement(statement);
            }

        }catch(DDLParserException | DMLParserException e){
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
            FileOutputStream out = new FileOutputStream(db + "catalog.txt");
            ObjectOutputStream objectOut = new ObjectOutputStream(out);
            objectOut.writeObject(catalog);
            objectOut.flush();
            objectOut.close();
        }
        catch(StorageManagerException e){
            e.printStackTrace();
        }
        catch(IOException e){
            System.out.println("File cannot be created");
        }
    }
}
