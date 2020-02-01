/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class StorageManager extends AStorageManager {

    // private instance variables
    private Map<Integer, String[]> dataTypes; // key is table id, value is the data types
    private Map<Integer, Integer[]> keyIndices; // key is table id, vale is keyIndices
    private Map<Integer, Integer> maxPageSize; // key is table id, value is tha max size of a page

    private int pageBufferSize;
    private int pageSize;

    private final int INTSIZE = 4;
    private final int DOUBLESIZE = 8;
    private final int BOOLSIZE = 1;
    private final int CHARSIZE = 2;

    /**
     * Creates an instance of the database. Tries to restart, if requested, the database at the provided location.
     * 
     * You can add code to this but cannot change the types and number of parameters. Testers will be called using
     * this constructor.
     * 
     * @param dbLoc the location to start/restart the database in
     * @param pageBufferSize the size of the page buffer; max number of pages allowed in the buffer at any given time
     * @param pageSize the size of a page in bytes
     * @param restart restart the database in the location if true; start a new database otherwise
     * @throws StorageManagerException database fails to restart or start
     */
    public StorageManager(String dbLoc, int pageBufferSize, int pageSize, boolean restart) throws StorageManagerException {
        super(dbLoc, pageBufferSize, pageSize, restart);
    }

    /**
     * Gets all of the records for the given table name
     * @param table the number of the table
     * @return A 2d array of objects representing the data in the table.
     *         Basically any array of records containing attribute values.
     * @throws StorageManagerException if the table does not exist
     */
    @Override
    public Object[][] getRecords(int table) throws StorageManagerException {
        // TODO buffer management

        return new Object[0][];
    }

    /**
     * Gets a record for the provided table name.
     * @param table the number of the table
     * @param keyValue an array representing the key to find
     * @return an 1d array of objects representing the data in the record, or NULL if no such record exists.
     * @throws StorageManagerException if the table does not exist
     */
    @Override
    public Object[] getRecord(int table, Object[] keyValue) throws StorageManagerException {
        // TODO buffer management table check

        // for now I'll assume that the table is in the buffer.
        
        return new Object[0];
    }

    /**
     * Inserts the record in the given table. If the record already exists it throws an exception. It finds the page
     * where it belongs, adds it in its proper location. If the page becomes overfull it will make a new page.
     * @param table the number of the table
     * @param record the record to insert; an 1d array of objects representing the data in the record
     * @throws StorageManagerException if the table does not exist, if the record already exists
     */
    @Override
    public void insertRecord(int table, Object[] record) throws StorageManagerException {

    }

    @Override
    public void updateRecord(int table, Object[] record) throws StorageManagerException {

    }

    @Override
    public void removeRecord(int table, Object[] keyValue) throws StorageManagerException {

    }

    @Override
    public void dropTable(int table) throws StorageManagerException {

    }

    @Override
    public void clearTable(int table) throws StorageManagerException {

    }

    /**
     * Adds an empty table with the provided name
     * @param table number of the table
     * @param dataTypes ArrayList of Strings representing the data types stored in the table
     *                  The order of the types must match the order of the records
     * @param keyIndices ArrayList containing the indices of the primary key attributes of the table.
     *                   Order is important. (1,2) and (2,1) are different.
     * @throws StorageManagerException if the table already exists
     */
    @Override
    public void addTable(int table, String[] dataTypes, Integer[] keyIndices) throws StorageManagerException {
        if(this.dataTypes.containsKey(table))   { // check to see if table exists
            throw new StorageManagerException("Table already exist");
        }
        this.dataTypes.put(table, dataTypes);
        this.keyIndices.put(table, keyIndices);

        for(int i = 0;i < dataTypes.length;i++)

    }

    @Override
    public void purgeBuffer() throws StorageManagerException {

    }

    @Override
    public void terminateDatabase() throws StorageManagerException {

    }

    @Override
    protected void restartDatabase(String dbLoc) throws StorageManagerException {
        //TODO use read object
    }

    /**
     * Starts a brand new database at the given location. Assumes the directory already exists. Any data in that
     * directory will be deleted.
     * @param dbLoc the location to start the database in
     * @param pageBufferSize the size of the page buffer; max number of pages allowed in the buffer at any given time
     * @param pageSize the size of a page in kilobytes
     * @throws StorageManagerException fails to create a database at the provided location
     */
    @Override
    protected void newDatabase(String dbLoc, int pageBufferSize, int pageSize) throws StorageManagerException {
        File dbDirectory = new File(dbLoc);
        for (File file: dbDirectory.listFiles()) {
            deleteFile(file.getAbsolutePath());//delete everything in file
        }

        this.dataTypes = new HashMap<Integer, String[]>();
        this.keyIndices = new HashMap<Integer, Integer[]>();
        this.maxPageSize = new HashMap<Integer, Integer>();

        this.pageBufferSize = pageBufferSize;
        this.pageSize = pageSize;
    }

    /**
     * Deletes the files/ directories in the file location given
     * @param fileLoc the path of the file to be deleted
     * @throws StorageManagerException
     */
    private void deleteFile(String fileLoc) throws StorageManagerException{
        File directory = new File(fileLoc);
        for (File file: directory.listFiles()) {
            if(file.isDirectory()){//file is a directory
                deleteFile(file.getAbsolutePath());
            }
            else{
                if(!file.delete()){//cant delete file
                    throw new StorageManagerException("Could not delete file at: " + file.getAbsolutePath());
                }
            }
        }
    }

}
