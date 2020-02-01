/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;
import java.util.ArrayList;

public class Page {

    private int pageId;
    private int maxRecordsPerPage;
    private int table;
    private ArrayList<Object[]> recordList; // Array list of all the record stored on a page
    private String[] dataTypes;

    /**
     * Page Constructor
     * @param pageId the page id
     * @param record1 the 1st record to be added to the page
     */
    public Page(int pageId, int table, int maxRecordsPerPage, Object[] record1, String[] dataTypes) {
        this.pageId = pageId;
        this.recordList = new ArrayList<Object[]>();
        this.table = table;
        this.recordList.add(record1); // add the first record to the beginning of the arrayList
        this.maxRecordsPerPage = maxRecordsPerPage;
        this.dataTypes = dataTypes;
    }

    /**
     * 
     * @param record
     * @param primaryKeys
     * @param dataTypes
     * @return
     */
    public boolean insertRecord(Object record, int[] primaryKeys, String[] dataTypes) {
        
        // Check if record is full
        if (recordList.size() == maxRecordsPerPage) {
            return false;
        }

        // TODO check if record exists

        for (Object[] pageRecord : recordList) {
            for (int key : primaryKeys) {

            }
        }

        return true;
    }


    /**
     * 
     * @param table
     * @return
     */
    public boolean inTable(int table) {
        if (this.table == table) {
            return true;
        }
        return false;
    }

}
