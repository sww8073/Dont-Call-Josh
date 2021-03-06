/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;

import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable {

    private Integer pageId;
    private Integer maxRecordsPerPage;
    private Integer table;
    private ArrayList<Object[]> recordList; // Array list of all the record stored on a page
    private String[] dataTypes;
    private Integer[] keyIndices;

    /**
     * Page constructor
     * @param pageId unique id of the page
     * @param table id of the table the page belongs in
     * @param maxRecordsPerPage max number of records per page
     * @param record1 1st record to be added to the page
     * @param dataTypes data type that belong in each record
     * @param keyIndices key indices of each record
     */
    public Page(Integer pageId, int table, int maxRecordsPerPage, Object[] record1, String[] dataTypes,
                Integer[] keyIndices) {
        this.pageId = pageId;
        this.recordList = new ArrayList<Object[]>();
        this.table = table;
        this.dataTypes = dataTypes;
        this.recordList.add(padRecord(record1)); // add the first record to the beginning of the arrayList
        this.maxRecordsPerPage = maxRecordsPerPage;
        this.keyIndices = keyIndices;
    }

    /**
     * This constructor is used to created a new slitted page
     * @param pageId unique id
     * @param table id of the table the page belongs in
     * @param maxRecordsPerPage max number of records per page
     * @param recordList populated list of record
     * @param dataTypes data types in each record
     * @param keyIndices key indices of each record
     */
    private Page(Integer pageId, int table, int maxRecordsPerPage, ArrayList<Object[]> recordList, String[] dataTypes,
                Integer[] keyIndices) {
        this.pageId = pageId;
        this.recordList = new ArrayList<Object[]>();
        this.table = table;
        this.recordList = recordList;
        this.maxRecordsPerPage = maxRecordsPerPage;
        this.dataTypes = dataTypes;
        this.keyIndices = keyIndices;
    }

    /**
     * Checks to see if the page is in the table passed in.
     * @param table The table to find.
     * @return Whether the page is in that table.
     */
    public boolean inTable(int table) {
        if (this.table == table) {
            return true;
        }
        return false;
    }

    /**
     * Gets the id of the page
     */
    public Integer getPageId(){
        return this.pageId;
    }

    /**
     * gets ArrayList of records(unpadded)
     * @return
     */
    public ArrayList<Object[]> getRecordList(){
        ArrayList<Object[]> unPaddedRecList = new ArrayList<>();
        for(int i = 0;i < recordList.size();i++)    {
            unPaddedRecList.add(unpadRecord(recordList.get(i)));
        }
        return unPaddedRecList;
    }


    /**
     * Checks to see if the current page is full
     * @return Whether the page is full
     */
    public boolean pageFull() {
        if (maxRecordsPerPage <= recordList.size()) {
            return true;
        }
        return false;
    }

    /**
     * add a new record to a page
     * @param recordToAdd and object array
     * @prcondition the page must have been check to have room for a record*******
     * @return true if record added
     */
    public boolean addRecordToPage(Object[] recordToAdd) throws StorageManagerException
    {
        recordToAdd = padRecord(recordToAdd);

        Object[] firstRec = recordList.get(0);
        Object[] lastRec = recordList.get(recordList.size() - 1); // last value in record


        if(compareRecords(recordToAdd, firstRec) == -1) { // record belongs in the beginning
            recordList.add(0, recordToAdd);
            return true;
        }
        else if(compareRecords(recordToAdd, lastRec) == 1)  { // record belongs in the end
            recordList.add(recordList.size(), recordToAdd);
            return true;
        }
        else if(compareRecords(recordToAdd, lastRec) == 0)  {
            throw new StorageManagerException("Cannot add duplicate record");
        }
        else {
            // insert record between 2 values
            for(int j = 0;j < recordList.size() - 1;j++)    { // loop through all records
                Object[] currRecord = recordList.get(j); // current record to check
                Object[] nextRecord = recordList.get(j + 1); // record after that record

                if (isRecBetweenRecs(recordToAdd, currRecord, nextRecord)) {
                    // inserted record belongs between theses two records
                    recordList.add(j + 1, recordToAdd); // insert after current record
                    return true;
                }
                else if(compareRecords(recordToAdd, currRecord) == 0)
                    throw new StorageManagerException("Cannot add duplicate record");
            }
        }
        return false;
    }



    /**
     * This function checks to see if a record has a char type, and
     * pads that char type;
     * @param record the record to be added
     * @return
     */
    public Object[] padRecord(Object[] record)   {

        for(int j = 0;j < dataTypes.length;j++)    {
            String type = dataTypes[j];
            String subType = type.substring(0, 4);
            if(subType.compareTo("char") == 0)  {
                int startIndex = type.indexOf("(") + 1;
                int endIndex = type.indexOf(")");
                String numString = type.substring(startIndex, endIndex);
                int stringSize = Integer.parseInt(numString);
                char paddedS[] = new char[stringSize];
                for(int i = 0;i < stringSize;i++)   {
                    paddedS[i] = '*';
                }
                for(int i = 0;i < ((String)record[j]).length();i++)    {
                    paddedS[i] = ((String)record[j]).charAt(i);
                }
                record[j] = new String(paddedS);
            }
        }
        return record;
    }

    /**
     * this function removes the padding of any char types
     * @param record
     * @return record no padding
     */
    public Object[] unpadRecord(Object[] record)   {
        Object[] unpaddedRec = new Object[record.length];

        for(int j = 0;j < dataTypes.length;j++)    {
            unpaddedRec[j] = record[j];

            String type = dataTypes[j];
            String subType = type.substring(0, 4);
            if(subType.compareTo("char") == 0)  {
                String unpadS = (String)record[j];
                for(int i = ((String)record[j]).length() - 1; i >= 0;i--)   {
                    if(unpadS.charAt(i) == '*') {
                        unpadS = unpadS.substring(0, i);
                    }
                }
                unpaddedRec[j] = new String(unpadS);
            }
        }
        return unpaddedRec;
    }

    /**
     * record to be added is smaller than 1st rec on page
     * @param record Object array
     * @return
     */
    public boolean smallerThanMinRecOnPg(Object[] record)  {
        record = padRecord(record);
        // insert record between 2 values
        Object[] firstRec = recordList.get(0);
        if(compareRecords(record, firstRec) == -1) { // record is smaller than first val on this page
            return true;
        }
        return false;
    }

    /**
     * This function checks to see if the page has no records
     * @return
     */
    public boolean isEmpty()    {
        if(recordList.size() == 0)
            return true;
        return false;
    }

    /**
     * record see if the record exists between the max record and
     * min record on the page
     * @param record Object array
     * @return
     */
    public boolean isRecBetweenMaxAndMin(Object[] record)  {
        record = padRecord(record);

        Object[] firstRec = recordList.get(0);
        Object[] lastRec = recordList.get(recordList.size() - 1);

        // record >= firstRec AND record <= lastRec
        if(compareRecords(record, firstRec) > -1  && compareRecords(record, lastRec) < 1)
        {
            return true;
        }
        return false;
    }

    /**
     * this function compares records on a page. It compare the values of all indices
     * in order.
     * @param rec1 record on this page
     * @param rec2 record on this page
     * @return 1 if rec1 > rec2
     *         -1 if rec1 < rec2
     *         0 if rec1 == rec2
     *         -2 if error
     */
    public int compareRecords(Object[] rec1, Object[] rec2) {

        // loop though and check each key indices
        for(int i = 0;i < keyIndices.length;i++)    {
            // compares key indices that corresponds to i
            Object rec1Key = rec1[keyIndices[i]];
            Object rec2Key = rec2[keyIndices[i]];

            int result = compareIndices(rec1Key, rec2Key);

            // a difference was found from compareIndices()
            if(result != 0)
                return result;
        }

        // no differences were found
        return 0;
    }

    /**
     * this function compares a record to search key indices
     * @param rec record on this page
     * @param searchKeyInd key indices to be compared
     * @return 1 if rec1 > rec2
     *         -1 if rec1 < rec2
     *         0 if rec1 == rec2
     *         -2 if error
     */
    public int compareRecordToKeyIndices(Object[] rec, Object[] searchKeyInd) {
        // loop though and check each key indices
        int searchKeyIndex = 0;
        for(int i = 0;i < keyIndices.length;i++)    {
            // compares key indices that corresponds to i
            Object rec1Key = rec[keyIndices[i]];
            Object rec2Key = searchKeyInd[searchKeyIndex];
            searchKeyIndex++;

            int result = compareIndices(rec1Key, rec2Key);

            // a difference was found from compareIndices()
            if(result != 0)
                return result;
        }

        // no differences were found
        return 0;
    }

    /**
     * this function check if a record exists between two records
     * @param midRec record on page
     * @param lowerRec record on page
     * @param upperRec record on page
     * @return treu if midRec belongs between lowerRec and upperRec
     */
    public boolean isRecBetweenRecs(Object[] midRec, Object[] lowerRec, Object[] upperRec)  {
        if(compareRecords(lowerRec, midRec) == -1 && compareRecords(midRec, upperRec) == -1)
            return true;
        return false;
    }

    /**
     * This function compares two indices.
     * @param val1 an object to be compared
     * @param val2 an object to be compared
     * @precondition both val1 and val2 must be same type
     * @return if val1 > val2, return 1
     *          if val1 < val2, return -1
     *          if val1 == val2, return 0
     *          return -2 if error
     */
    private int compareIndices(Object val1, Object val2)    {
        // compare Integer
        if(val1 instanceof Integer) {
            if((Integer)val1 > (Integer)val2)
                return 1;
            else if((Integer)val1 < (Integer)val2)
                return -1;
            else
                return 0;
        }
        // compare Doubles
        if(val1 instanceof Double) {
            if((Double)val1 > (Double) val2)
                return 1;
            else if((Double)val1 < (Double) val2)
                return -1;
            else
                return 0;
        }
        // compare Booleans
        if(val1 instanceof Boolean) {
            if((Boolean)val1 == (Boolean)val2)
                return 0;
            else
                return 1;
        }
        // compare Strings
        if(val1 instanceof String) {
            if(val1.toString().compareTo(val2.toString()) > 0)
                return 1;
            else if(val1.toString().compareTo(val2.toString()) < 0)
                return -1;
            else
                return 0;
        }
        return -2;
    }

    /**
     * This function will split a page.
     * The 1st half of the values will remain in this page and the 2nd half will be
     * be placed on the new page. ***new page belongs after old Page***
     * @param newPageId the new unique id of a page
     * @return new page half full page
     */
    public Page splitPage(Integer newPageId) {
        ArrayList<Object[]> botHalfRecList = new ArrayList<Object[]>();
        ArrayList<Object[]> topHalfRecList = new ArrayList<Object[]>();

        int centerIndex = recordList.size() / 2;

        // populates botHalfRecList with bottom half values of record List
        for(int i = 0;i < centerIndex;i++)  {
            botHalfRecList.add(recordList.get(i));
        }

        // populates topHalfRecList with top half values of record List
        for(int i = centerIndex;i < recordList.size();i++)  {
            topHalfRecList.add(recordList.get(i));
        }

        // current page now has bottom half of its previous record list
        recordList = botHalfRecList;

        // creates new page with upper half of records
        Page newPage = new Page(newPageId, this.table, maxRecordsPerPage, topHalfRecList, dataTypes, keyIndices);
        return newPage;
    }

    private boolean areRecordsEqual(Object[] record, Object[] recordToCompare){
        int keyInd;
        for(int i = 0; i < keyIndices.length; i++) {
            keyInd = keyIndices[i];
            if (compareIndices(record[keyInd],recordToCompare[keyInd]) !=0){
                return false;
            }
        }
        return true;
    }

    public Object[] getRecordFromPage(Object[] keyValue){
        for (Object[] record: recordList) {
            if(areRecordsEqual(record, keyValue)){
                return record;
            }
        }
        return null;
    }

    /**
     * Finds the old record and replaces it with the new record
     * @param oldRec old record data.
     * @param newRec new record data.
     * @return The new record, null if the record wasn't found.
     */
    public void updateRecord(Object[] oldRec, Object[] newRec) {
        oldRec = padRecord(oldRec);
        newRec = padRecord(newRec);

        // this class currently doesnt change the order of the records if any of the keyValues are changed
        for(int i = 0;i < recordList.size();i++)    {
            if(compareRecords(recordList.get(i), oldRec) == 0){
                recordList.remove(i);
                recordList.add(i, padRecord(newRec));
            }
        }
    }

    /**
     * This function removes a record.
     * @precondition record being removes must be verified to exist
     * @param keyValue key value to search by
     */
    public void removeRecord(Object[] keyValue) {
        for(int i = 0;i < recordList.size();i++)    {
            if(compareRecordToKeyIndices(recordList.get(i), keyValue) == 0)
                recordList.remove(i);
        }
    }
}
