package storagemanager;

import java.util.ArrayList;

/**
 * Abstract class for the Storage Manager.
 *
 * Any errors must throw a new StorageManagerException.
 *
 * Example Errors:
 *
 * 1. Table does not exist
 * 2. Error in reading and converting data types.
 */

public abstract class AStorageManager {

    /**
     * Creates an instance of the database. Tries to restart, if requested, the database at the provided location.
     * @param dbLoc the location to start/restart the database in
     * @param pageBufferSize the size of the page buffer; max number of pages allowed in the buffer at any given time
     * @param pageSize the size of a page in bytes
     * @param restart restart the database in the location if true; start a new database otherwise
     * @throws StorageManagerException database fails to restart or start
     */
    protected AStorageManager(String dbLoc, int pageBufferSize, int pageSize, boolean restart) throws StorageManagerException{
        if(restart)
            this.restartDatabase(dbLoc);
        else
            this.newDatabase(dbLoc, pageBufferSize, pageSize);
    }

    /**
     * Gets all of the records for the given table name
     * @param table the number of the table
     * @return A 2d array of objects representing the data in the table.
     *         Basically any array of records containing attribute values.
     * @throws StorageManagerException if the table does not exist
     */
    public abstract Object[][] getRecords(int table) throws StorageManagerException;

    /**
     * Gets a record for the provided table name.
     * @param table the number of the table
     * @param keyValue an array representing the key to find
     * @return an 1d array of objects representing the data in the record, or NULL if no such record exists.
     * @throws StorageManagerException if the table does not exist
     */
    public abstract Object[] getRecord(int table, Object[] keyValue) throws StorageManagerException;

    /**
     * Inserts the record in the given table. If the record already exists it throws an exception. It finds the page
     * where it belongs, adds it in its proper location. If the page becomes overfull it will make a new page.
     * @param table the number of the table
     * @param record the record to insert; an 1d array of objects representing the data in the record
     * @throws StorageManagerException if the table does not exist, if the record already exists
     */
    public abstract void insertRecord(int table, Object[] record) throws StorageManagerException;

    /**
     * Updates the record in the given table if the record already exists it overwrites it. If it does not
     * exist it finds a page with a empty spot to add it to; makes new pages as needed.
     * @param table the number of the table
     * @param record the record to insert; an 1d array of objects representing the data in the record
     * @throws StorageManagerException if the table does not exist
     */
    public abstract void updateRecord(int table, Object[] record) throws StorageManagerException;

    /**
     * removes a record for the provided table name. If a page becomes empty it frees it.
     * @param table the number of the table
     * @param keyValue an array representing the key to find
     * @throws StorageManagerException if the table or record does exist
     */
    public abstract void removeRecord(int table, Object[] keyValue) throws StorageManagerException;

    /**
     * Will delete all entries in this table. Including clearing and freeing all pages. Will also remove the table
     * from the database
     * @param table: the number of the table to delete
     * @throws StorageManagerException if the table does not exist
     */
    public abstract void dropTable(int table) throws StorageManagerException;

    /**
     * Will delete all entries in this table. Including clearing and freeing all pages. Will not remove the
     * table from the database
     * @param table: the number of the table to clear
     * @throws StorageManagerException if the table does not exist
     */
    public abstract void clearTable(int table) throws StorageManagerException;

    /**
     * Adds an empty table with the provided name
     * @param table number of the table
     * @param dataTypes ArrayList of Strings representing the data types stored in the table
     *                  The order of the types must match the order of the records
     * @param keyIndices ArrayList containing the indices of the primary key attributes of the table.
     *                   Order is important. (1,2) and (2,1) are different.
     * @throws StorageManagerException if the table already exists
     */
    public abstract void addTable(int table, String[] dataTypes, Integer[] keyIndices) throws StorageManagerException;

    /**
     * Will purge any pages in the buffer to the physical hardware.
     * @throws StorageManagerException any failure to write the buffer to hardware
     */
    public abstract void purgeBuffer() throws StorageManagerException;

    /**
     * Will purge the page buffer and write any needed data to the physical hardware needed to restart the database.
     * @throws StorageManagerException any failure to write the buffer or database information to hardware
     */
    public abstract void terminateDatabase() throws StorageManagerException;

    /**
     * Load in any needed data to restart the database; if not starting for the first time.
     * @param dbLoc the location of the database to restart
     * @throws StorageManagerException if there is no database at the location or database at that location fails
     *                                 to restart.
     */
    protected abstract void restartDatabase(String dbLoc) throws StorageManagerException;

    /**
     * Starts a brand new database at the given location. Assumes the directory already exists. Any data in that
     * directory will be deleted.
     * @param dbLoc the location to start the database in
     * @param pageBufferSize the size of the page buffer; max number of pages allowed in the buffer at any given time
     * @param pageSize the size of a page in kilobytes
     * @throws StorageManagerException fails to create a database at the provided location
     */
    protected abstract void newDatabase(String dbLoc, int pageBufferSize, int pageSize) throws StorageManagerException;
}
