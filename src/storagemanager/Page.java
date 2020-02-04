/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;

import org.omg.CORBA.OBJ_ADAPTER;

import java.util.ArrayList;

public class Page {

    private Integer pageId;
    private int maxRecordsPerPage;
    private int table;
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
    public Page(Integer pageId, int table, int maxRecordsPerPage, Object[] record1, String[] dataTypes, Integer[] keyIndices) {
        this.pageId = pageId;
        this.recordList = new ArrayList<Object[]>();
        this.table = table;
        this.recordList.add(record1); // add the first record to the beginning of the arrayList
        this.maxRecordsPerPage = maxRecordsPerPage;
        this.dataTypes = dataTypes;
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
    public ArrayList<Object[]> getRecordList(){
        return this.recordList;
    }

    /**
     * Checks to see if the current page is full
     * @return Whether the page is full
     */
    public boolean pageFull() {
        if (maxRecordsPerPage == recordList.size()) {
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
    public boolean addRecordToPage(Object[] recordToAdd)
    {
        int recIndSize = recordList.size() - 1;

        // insert record between 2 values
        for(int i = 0;i < keyIndices.length - 1;i++)   { // search by indices in order skipping last value
            Object insertRecCompVal = recordToAdd[i]; // value we are comparing to see if we should insert

            // comparison value for first record
            Object[] firstRec = recordList.get(0);
            Object firstRecCompVal = firstRec[i];

            // comparison value for last record
            Object[] lastRec = recordList.get(recIndSize); // last value in record
            Object lastRecCompVal = lastRec[i];

            // record belongs in the beginning
            if(compareIndices(insertRecCompVal, firstRecCompVal) == -1) {
                recordList.add(0, recordToAdd); // insert record at beginning
                return true;
            }
            else if(compareIndices(insertRecCompVal, lastRecCompVal) == 1)  {
                recordList.add(recordList.size(), recordToAdd); // insert record at end
                return true;
            }
            else {
                // insert record between 2 values
                for(int j = 0;j < recordList.size();j++)    { // loop through all records
                    Object[] record = recordList.get(j); // current record to check
                    Object[] nextRecord = recordList.get(j + 1); // record after that record

                    Object recCompVal = record[i];
                    Object nextRecCompVal = nextRecord[i];

                    if (compareIndices(recCompVal, insertRecCompVal) == 1 ||
                            compareIndices(insertRecCompVal, nextRecCompVal) == -1) {
                        // inserted record belongs between theses two records
                        recordList.add(j + 1, recordToAdd); // insert after current record
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * this function checks to see if record belongs on page
     * @param recordToAdd Object array
     * @return true if record belongs on page
     */
    public boolean shouldRecordBeOnPage(Object[] recordToAdd)   {
        int recListSize = recordList.size() - 1;

        // insert record between 2 values
        for(int i = 0;i < keyIndices.length;i++) { // loop through indices
            Object[] firstRec = recordList.get(0);
            Object[] lastRec = recordList.get(recListSize);

            Object firstRecCompVal = firstRec[keyIndices[i]];
            Object lastRecCompVal = lastRec[keyIndices[i]];
            Object recToAddCompVal = recordToAdd[keyIndices[i]];

            // record is between first and last value or equal
            if (compareIndices(recToAddCompVal, firstRecCompVal) > -1 &&
                    compareIndices(recToAddCompVal, lastRecCompVal) < 1) {
                return true;
            }
        }
        return false;
    }

    /**
     * record to be added is smaller than 1st rec on page
     * @param recordToAdd Object array
     * @return
     */
    public boolean smallerThanMinRecOnPg(Object[] recordToAdd)  {

        // insert record between 2 values
        for(int i = 0;i < keyIndices.length;i++) { // loop through indices
            Object[] firstRec = recordList.get(0);

            Object firstRecCompVal = firstRec[keyIndices[i]];
            Object recToAddCompVal = recordToAdd[keyIndices[i]];

            // record to be added is smaller than 1st rec on page
            if (compareIndices(recToAddCompVal, firstRecCompVal) == -1) {
                return true;
            }
            else if (compareIndices(recToAddCompVal, firstRecCompVal) == 1) {
                return false;
            }

            // only continue loop if values are equal, then you must check next index
        }
        return false;
    }

    /**
     * record to add is larger than max record on page
     * @param recordToAdd Object array
     * @return
     */
    public boolean largerThanMaxRecOnPg(Object[] recordToAdd)  {
        int recListSize = recordList.size() - 1;

        // insert record between 2 values
        for(int i = 0;i < keyIndices.length;i++) { // loop through indices
            Object[] lastRec = recordList.get(recListSize);

            Object lastRecCompVal = lastRec[keyIndices[i]];
            Object recToAddCompVal = recordToAdd[keyIndices[i]];

            // record to add is larger than max record on page
            if (compareIndices(recToAddCompVal, lastRecCompVal) == 1) {
                return true;
            }
        }
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

}
