/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;
import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;

import java.util.ArrayList;

public class Page {

    private long pageId;
    private int maxRecordsPerPage;
    private int table;
    private ArrayList<Object[]> recordList; // Array list of all the record stored on a page
    private String[] dataTypes;
    private Integer[] keyIndices;

    /**
     * Page Constructor
     * @param pageId the page id
     * @param record1 the 1st record to be added to the page
     */
    public Page(long pageId, int table, int maxRecordsPerPage, Object[] record1, String[] dataTypes, Integer[] keyIndices) {
        this.pageId = pageId;
        this.recordList = new ArrayList<Object[]>();
        this.table = table;
        this.recordList.add(record1); // add the first record to the beginning of the arrayList
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
    public long getPageId(){
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
        for(int i = 0;i < keyIndices.length - 1;i++)   { // search by indices es in order
            for(Object[] record : recordList)   { // loop through all records
                if(compareIndices(record[keyIndices[i]], recordToAdd[keyIndices[i]]) == 0)
                    return false; // record already exists so return false

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
            if((Integer)val1 > (Integer)val1)
                return 1;
            else if((Integer)val1 < (Integer)val1)
                return -1;
            else
                return 0;
        }
        // compare Doubles
        if(val1 instanceof Double) {
            if((Double)val1 > (Double) val1)
                return 1;
            else if((Double)val1 < (Double) val1)
                return -1;
            else
                return 0;
        }
        // compare Booleans
        if(val1 instanceof Boolean) {
            if((Boolean)val1 == (Boolean)val1)
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
}
