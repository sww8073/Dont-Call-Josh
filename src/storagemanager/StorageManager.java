/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;
import java.io.File;

public class StorageManager extends AStorageManager {

    // private instance variables
    private String dbLoc;
    private int pageBufferSize;
    private int pageSize;
    private Buffer buffer;

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

    @Override
    public Object[][] getRecords(int table) throws StorageManagerException {
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
        // it says these are ArrayLists but they are being passed in as an array?
        // are tables going to get bigger and smaller
        //
        // find a page and add table into it, if there isn't enough room then split data on page to two pages?
        // should i make a page object that keeps track of the amount of dat in it, just a big int array?
    }

    @Override
    public void purgeBuffer() throws StorageManagerException {

    }

    @Override
    public void terminateDatabase() throws StorageManagerException {

    }

    @Override
    protected void restartDatabase(String dbLoc) throws StorageManagerException {
        //TODO retrieve pageBufferSize and pageSize and set them equal to the instance variables
        // we will probly store this somewhere in a file

        //TODO grab buffer info from file
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
