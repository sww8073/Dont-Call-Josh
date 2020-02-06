/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;

import java.io.File;
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
    
    private ArrayList<Page> buffer; // TODO Initialize buffer, find way to add pages
    private BufferManager bufferManager;

    private int pageSize;

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

        ArrayList<Integer> pages = tablePages.get(table);
        Object[][] recordsOfTable = new Object[pages.size()][]; // I dont know if this will work, would pages.size() just tell you how many pages there are?
        ArrayList<Object[]> records;
        int x = 0;
        for (Integer pageNum: pages) {
            records = buffer.get(pageNum).getRecordList();
            for(int i =0; i < records.size(); i++){
                recordsOfTable[x][i] = records.get(i);
            }
            x++;
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
        
        // check to see if table exists
        if(!doesTableExist(table)) {
            throw new StorageManagerException("The table does not exist");
        }

        ArrayList<Integer> pages = tablePages.get(table);
        Object[] record = null;
        for (Integer pageNum: pages) {
            // attempt to get the page
            Page page = buffer.get(pageNum);
            if (page != null) {
                // if the page is not null, we can look for the record in the page
                record = page.getRecordFromPage(keyValue);
                if (record != null) {
                    return record;
                }
            } else {
                // we need to read the page into the buffer from memory
                bufferManager.addPage(pageNum);
                page = buffer.get(pageNum);
                record = page.getRecordFromPage(keyValue);
                if (record != null) {
                    return record;
                }
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
        Integer[] indicies = this.keyIndices.get(table);
        if(indicies == null)    {
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
            buffer.add(page); // add page to the buffer
        }
        // a page exists
        else    {
            // grab construct list of pages we need to look through to insert
            ArrayList<Integer> pageIdList = this.tablePages.get(table); // get page ids of tables in order
            ArrayList<Page> requiredPages = new ArrayList<>(); // list of all page for table

            // this for loop assumes all pages are in the buffer
            for(int pageid : pageIdList)    {
                for (Page pageRecord: this.buffer) { // grab all th pages form buffer
                    if( pageRecord.getPageId() == pageid){
                        requiredPages.add(pageRecord);
                    }
                }
            }

            // if there is only one page we must insert record on that page
            if(requiredPages.size() == 1) {
                if (requiredPages.get(0).pageFull()) {
                    splitPageAndRec(table, requiredPages.get(0), record); // split table!!!!
                } else {
                    requiredPages.get(0).addRecordToPage(record);
                }
            }
            else {

                int pgInTable = requiredPages.size();
                for(int i = 0;i < pgInTable;i++)    {
                    Page page = requiredPages.get(i); // get current page

                     if(i == pgInTable - 1) { // last page in table
                        // the last page has been reached so the record must belong in here
                        if (page.pageFull()) {
                            splitPageAndRec(table, page, record); // page is full so split!!!
                        } // page is full so split!!!
                        else {
                            page.addRecordToPage(record);
                        }
                        return;
                    }
                    else    {
                        Page nextPage = requiredPages.get(i + 1); // get next page

                        // record is smaller than the smallest record in that table, thus it belongs in current table
                        if(nextPage.smallerThanMinRecOnPg(record))  {
                            if (page.pageFull()) {
                                splitPageAndRec(table, page, record); // page is full so split!!!
                            }
                            else {
                                page.addRecordToPage(record);
                            }
                            return;
                        }
                    }
                }
            }
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
    public void splitPageAndRec(int table, Page pgToBeSplit,  Object[] record) throws StorageManagerException   {
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
        buffer.add(topHalfPg);

        // new page id to the tables ordered list of tables
        int botHalfPgIndex = tablePages.get(table).indexOf(table);
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
        if(keyIndices.get(table) == null){throw new StorageManagerException("table " + table + " does not exist");}
        else {
            Integer[] keyInd = keyIndices.get(table); //the keyIndices from the table
            Object[] recordKeyInd = new Object[keyInd.length]; //the keyValue pair based off the keyIndices
            int keyIndValue;
            //making the keyValue pair
            for (int i = 0; i < keyInd.length; i++) {
                keyIndValue = keyInd[i];
                recordKeyInd[i] = record[keyIndValue];
            }

            if (getRecord(table, recordKeyInd) != null) {
                // grab all pages so that we can look for our record
                ArrayList<Integer> pageIdList = this.tablePages.get(table); // get page ids of tables in order

                // this for loop assumes all pages are in the buffer
                for(int pageid : pageIdList)    {
                    for (Page pageRecord: this.buffer) { // grab all the pages from buffer
                        if(pageRecord.getPageId() == pageid) {
                            if (pageRecord.updateRecordFromPage(record, recordKeyInd) != null) {
                                return; // this might not work and seems too complicated to me
                                        // I'll test and work on this more tomorrow, right now I need to sleep.
                            }
                        }
                    }
                }

            } else {
                // call insert record
                insertRecord(table, record);
            }
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
        ArrayList<Integer> pages = tablePages.get(table);
        boolean pageIsEmpty;
        for (Integer pageNum: pages) {
            Page page = buffer.get(pageNum);
            pageIsEmpty = page.tryToRemoveRecord(keyValue);
            if(pageIsEmpty){//page is empty and has to be removed
                pages.remove(pageNum);
            }
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

    }

    /**
     * Will delete all entries in this table. Including clearing and freeing all pages. Will not remove the
     * table from the database
     * @param table: the number of the table to clear
     * @throws StorageManagerException if the table does not exist
     */
    @Override
    public void clearTable(int table) throws StorageManagerException {
        
        ArrayList<Integer> pages = new ArrayList<>();
        ArrayList<Object[]> records;
        Integer[] keyInd = keyIndices.get(table);
        for (Integer pageNum: pages) {
            records = buffer.get(pageNum).getRecordList();
            for (Object[] record: records) {
                Object[] keyValue = new Object[keyInd.length];
                for (int i = 0; i < keyValue.length; i++) {
                    keyValue[i] = record[keyInd[i]];
                }
                removeRecord(table, keyValue);
            }
        }
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
        if(dbDirectory.exists() == false)   { // check if directory exists
            throw new StorageManagerException("directory does not exist");
        }
        for (File file: dbDirectory.listFiles()) {
            deleteFile(file.getAbsolutePath());//delete everything in file
        }

        this.dataTypes = new HashMap<Integer, String[]>();
        this.keyIndices = new HashMap<Integer, Integer[]>();
        this.maxRecordsPerPage = new HashMap<Integer, Integer>();
        this.tablePages = new HashMap<Integer, ArrayList<Integer>>();
        this.pageSize = pageSize;
        this.buffer = new ArrayList<>();

        File file = new File(dbLoc + "buffer");
        this.bufferManager = new BufferManager(pageSize, pageBufferSize, dbLoc + "buffer");
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
