/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class StorageManager extends AStorageManager {

    // private instance variables
    private Map<Integer, String[]> dataTypes; // key is table id, value is the data types
    private Map<Integer, Integer[]> keyIndices; // key is table id, vale is keyIndices
    private Map<Integer, Integer> maxRecordsPerPage; // key is table id

    // key is table id, value is ArrayList pages sorted in order form lowest to highest
    private Map<Integer, ArrayList<Integer>> tablePages;
    private BufferManager bufferManager;

    private int pageSize;
    private String dbLoc;

    private final int INTSIZE = 4;
    private final int DOUBLESIZE = 8;
    private final int BOOLSIZE = 1;
    private final int CHARSIZE = 2;

    private static Integer pageId = 0; // this is used to generate unique page ids

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
        // check if table exists
        if(!doesTableExist(table))    {
            throw new StorageManagerException("The table does not exist");
        }

        ArrayList<Integer> pageIdList = tablePages.get(table); // ordered list of page ids
        int pageCount = pageIdList.size();

        // get total # of records in table
        int totalRecordCount = 0;
        for (Integer id: pageIdList) {
            Page page = bufferManager.getPage(id);
            totalRecordCount += page.getRecordList().size();
        }

        // get the amount of values in each record
        int recordIndexCount = dataTypes.get(table).length;

        Object[][] recordsOfTable = new Object[totalRecordCount][recordIndexCount];

        int index = 0;
        for(int i = 0;i < pageCount;i++)    {
            Page page = bufferManager.getPage(pageIdList.get(i));
            ArrayList<Object[]> records = page.getRecordList();

            for(Object[] record : records)    {
                recordsOfTable[index] = record;
                index++;
            }
        }
        return recordsOfTable;
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
        if(!doesTableExist(table)) {
            throw new StorageManagerException("The table does not exist");
        }

        ArrayList<Integer> pageIdsList = tablePages.get(table);
        for (Integer id: pageIdsList) {
            Page page = bufferManager.getPage(id);
            ArrayList<Object[]> records = page.getRecordList();
            for(Object[] record : records)  {
                if(page.compareRecordToKeyIndices(record, keyValue) == 0)
                    return record;
            }
        }
        return null;
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
        // check to see if the table exists
        if(!doesTableExist(table)) {
            throw new StorageManagerException("The table does not exist");
        }

        // check if table contains any pages
        if(!this.tablePages.containsKey(table)) {
            // table does not have any pages, so ADD NEW PAGE
            pageId += 1; // increment page id each time

            // create new page and ADD record
            Page page = new Page(pageId, table, maxRecordsPerPage.get(table), record, dataTypes.get(table), keyIndices.get(table));

            ArrayList<Integer> newPageList = new ArrayList<>();
            newPageList.add(page.getPageId());
            tablePages.put(table, newPageList); // add the ordered list of table ids to map
            bufferManager.addPage(page);
        }
        else {
            ArrayList<Integer> orderedPageIds = tablePages.get(table);
            for (int i = 0; i < orderedPageIds.size(); i++) {
                Page page = bufferManager.getPage(orderedPageIds.get(i));

                // record exists between(inclusive) min and max record, if so add/split
                if (page.isRecBetweenMaxAndMin(record)) {
                    addRecOrSplitAndAddRec(table, page, record);
                    return;
                } else {
                    if (i + 1 < orderedPageIds.size()) { // there is a next page
                        Page nextPage = bufferManager.getPage(orderedPageIds.get(i));
                        if (nextPage.smallerThanMinRecOnPg(record)) { // record is smaller than the smallest record in the next page
                            addRecOrSplitAndAddRec(table, page, record);
                            return;
                        }
                        // if reached here, record must exist on one of the next pages
                    } else { // there is NOT a next page, record must belong in this page
                        addRecOrSplitAndAddRec(table, page, record);
                        return;
                    }
                }
            }
        }
    }

    /**
     * this fucntion will add a record to a page and split the page if necessary
     * @param table table id
     * @param page page record is being added to
     * @param record Object array representing record
     * @throws StorageManagerException
     */
    private void addRecOrSplitAndAddRec(int table, Page page, Object[] record) throws StorageManagerException {
        if (page.pageFull()) {
            splitPageAndRec(table, page, record); // split table!!!!
        } else {
            page.addRecordToPage(record);
        }
    }

    /**
     * This function removes the upper half of the records in pgToBeSplit and
     * adds that upper half of the records to a new page. A new record is added
     * to whatever table it belongs in.
     * @param table table id in which the page belongs
     * @param pgToBeSplit the full page that needs to be split
     * @param record the new record that needs to be added
     */
    private void splitPageAndRec(int table, Page pgToBeSplit,  Object[] record) throws StorageManagerException   {
        // create new unique page id
        pageId += 1; // increment page id each time

        Page botHalfPg = pgToBeSplit;
        Page topHalfPg = botHalfPg.splitPage(pageId);

        // add new record to page
        if(topHalfPg.smallerThanMinRecOnPg(record))
            botHalfPg.addRecordToPage(record);
        else
            topHalfPg.addRecordToPage(record);

        // add new page buffer
        bufferManager.addPage(topHalfPg);

        // new page id to the tables ordered list of tables
        int botHalfPgIndex = tablePages.get(table).indexOf(pgToBeSplit.getPageId());
        tablePages.get(table).add(botHalfPgIndex + 1, pageId);
    }

    /**
     * Updates the record in the given table if the record already exists it overwrites it. If it does not
     * exist it finds a page with a empty spot to add it to; makes new pages as needed.
     * @param table the number of the table
     * @param record the record to insert; an 1d array of objects representing the data in the record
     * @throws StorageManagerException if the table does not exist
     */
    @Override
    public void updateRecord(int table, Object[] record) throws StorageManagerException {
        if(!doesTableExist(table)) {
            throw new StorageManagerException("The table does not exist");
        }

        boolean recordUpdated = false;

        ArrayList<Integer> pageIdsList = tablePages.get(table);
        for (Integer id: pageIdsList) {
            Page page = bufferManager.getPage(id);

            // You can use this function with the record with the new values since
            // only the key indices are being used to compare and search.
            // KeyIndices will should never change.
            if(page.isRecBetweenMaxAndMin(record))  {
                page.updateRecord(record, record);
                recordUpdated = true;
            }
        }

        if(!recordUpdated)  {
            throw new StorageManagerException("The records you are trying to update does not exist");
        }
    }

    /**
     * removes a record for the provided table name. If a page becomes empty it frees it.
     * @param table the number of the table
     * @param keyValue an array representing the key to find
     * @throws StorageManagerException if the table or record does exist
     */
    @Override
    public void removeRecord(int table, Object[] keyValue) throws StorageManagerException {
        if(!doesTableExist(table)) {
            throw new StorageManagerException("The table does not exist");
        }

        boolean recordDeleted = false;

        ArrayList<Integer> pageIdsList = tablePages.get(table);
        for (Integer id: pageIdsList) {
            Page page = bufferManager.getPage(id);
            ArrayList<Object[]> records = page.getRecordList();
            for(int i = 0;i < records.size();i++)    {
                if(page.compareRecordToKeyIndices(records.get(i), keyValue) == 0)   {
                    page.removeRecord(keyValue);
                    recordDeleted = true;

                    if(page.isEmpty())  { // page is empty, it must be destroyed
                        // remove page form id map
                        ArrayList<Integer> pageIds = tablePages.get(table);
                        pageIds.remove(page.getPageId());

                        // remove page from buffer // TODO eventually replace with buffer call
                        bufferManager.deletePage(page.getPageId());
                    }
                }
            }
        }

        if(!recordDeleted)  {
            throw new StorageManagerException("The records you are trying to update does not exist");
        }
    }

    /**
     * Will delete all entries in this table. Including clearing and freeing all pages. Will also remove the table
     * from the database
     * @param table: the number of the table to delete
     * @throws StorageManagerException if the table does not exist
     */
    @Override
    public void dropTable(int table) throws StorageManagerException {
        if(!doesTableExist(table)) {
            throw new StorageManagerException("The table does not exist");
        }
        clearTable(table);
        tablePages.remove(table);
        dataTypes.remove(table);
        keyIndices.remove(table);
        maxRecordsPerPage.remove(table);
    }

    /**
     * Will delete all entries in this table. Including clearing and freeing all pages. Will not remove the
     * table from the database
     * @param table: the number of the table to clear
     * @throws StorageManagerException if the table does not exist
     */
    @Override
    public void clearTable(int table) throws StorageManagerException {
        if(!doesTableExist(table)) {
            throw new StorageManagerException("The table does not exist");
        }

        ArrayList<Integer> pageIds = tablePages.get(table);

        for(Integer id : pageIds)   {
            bufferManager.deletePage(id);
        }

        tablePages.replace(table,  new ArrayList<Integer>());
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
        if(doesTableExist(table)) {
            throw new StorageManagerException("The table already exists");
        }
        this.dataTypes.put(table, dataTypes);
        this.keyIndices.put(table, keyIndices);

        // calculate the size of a record
        int recordSize = 0;
        for(int i = 0;i < dataTypes.length;i++) {
            if(dataTypes[i].equals("integer"))
                recordSize += INTSIZE;
            else if(dataTypes[i].equals("double"))
                recordSize += DOUBLESIZE;
            else if(dataTypes[i].equals("boolean"))
                recordSize += BOOLSIZE;
            else if(dataTypes[i].contains("varchar(")) {
                int startIndex = dataTypes[i].indexOf("(") + 1;
                int endIndex = dataTypes[i].indexOf(")") - 1;
                String numString = dataTypes[i].substring(startIndex, endIndex);
                recordSize += (Integer.parseInt(numString) * CHARSIZE);
            }
            else if(dataTypes[i].contains("char(")) {
                int startIndex = dataTypes[i].indexOf("(") + 1;
                int endIndex = dataTypes[i].indexOf(")") - 1;
                String numString = dataTypes[i].substring(startIndex, endIndex);
                recordSize += (Integer.parseInt(numString) * CHARSIZE);
            }
        }

        // calculate the number of records that can fit on a page
        int recordsPerPage = pageSize / recordSize;
        this.maxRecordsPerPage.put(table, recordsPerPage);
    }

    /**
     * Will purge any pages in the buffer to the physical hardware.
     * @throws StorageManagerException any failure to write the buffer to hardware
     */
    @Override
    public void purgeBuffer() throws StorageManagerException {
        bufferManager.purge();
    }

    /**
     * Will purge the page buffer and write any needed data to the physical hardware needed to restart the database.
     * @throws StorageManagerException any failure to write the buffer or database information to hardware
     */
    @Override
    public void terminateDatabase() throws StorageManagerException {
        try{

            FileOutputStream out = new FileOutputStream(this.dbLoc + "\\database.txt");
            ObjectOutputStream objectOut = new ObjectOutputStream(out);
            objectOut.writeObject(this.dataTypes);
            objectOut.writeObject(this.keyIndices);
            objectOut.writeObject(this.maxRecordsPerPage);
            objectOut.writeObject(this.tablePages);
            objectOut.writeObject(this.pageId);

            objectOut.writeObject(this.bufferManager.getBufferSize());
            objectOut.writeObject(this.bufferManager.getPageSize());
            objectOut.writeObject(this.bufferManager.getBufLoc());

            objectOut.flush();
            objectOut.close();
        }catch(IOException e){
            System.out.println("File not found");
        }
    }

    /**
     * Load in any needed data to restart the database; if not starting for the first time.
     * @param dbLoc the location of the database to restart.
     * @throws StorageManagerException if there is no database at the location or database at that location fails
     *                                 to restart.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void restartDatabase(String dbLoc) throws StorageManagerException {
        String sizes = dbLoc + "\\database.txt";
        try{
            FileInputStream in = new FileInputStream(sizes);
            ObjectInputStream ois = new ObjectInputStream(in);
            this.dataTypes = (HashMap<Integer, String[]>) ois.readObject();
            this.keyIndices = (HashMap<Integer, Integer[]>) ois.readObject();
            this.maxRecordsPerPage = (HashMap<Integer, Integer>) ois.readObject();
            this.tablePages = (HashMap<Integer, ArrayList<Integer>>) ois.readObject();
            this.pageId = (Integer) ois.readObject();
            int pageSize = (int) ois.readObject();
            int pageBufferSize = (int) ois.readObject();
            String dbloc = (String) ois.readObject();
            this.bufferManager = new BufferManager(pageSize, pageBufferSize, dbloc);
            ois.close();
        }catch(IOException e){
            System.out.println("File not found");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
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
        if(dbDirectory.exists() == false)   { // check if directory exists
            throw new StorageManagerException("directory does not exist");
        }
        for (File file: dbDirectory.listFiles()) {
            deleteFile(dbLoc); //delete everything in file
        }

        this.dataTypes = new HashMap<Integer, String[]>();
        this.keyIndices = new HashMap<Integer, Integer[]>();
        this.maxRecordsPerPage = new HashMap<Integer, Integer>();
        this.tablePages = new HashMap<Integer, ArrayList<Integer>>();
        this.pageSize = pageSize;
        this.dbLoc = dbLoc;

        new File(dbLoc + "\\pages").mkdirs(); // create a new folder where buffer pages will be stored
        this.bufferManager = new BufferManager(pageSize, pageBufferSize, dbLoc + "\\pages");
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

    /**
     * Checks to see if a given table exists.
     * @param table the table to check.
     * @return whether the table exists.
     */
    private boolean doesTableExist(int table) {
        // check to see if the table exists
        Integer[] indicies = this.keyIndices.get(table);
        if(indicies == null) {
            return false;
        }
        return true;
    }
}
