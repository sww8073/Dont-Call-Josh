/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;
import java.util.ArrayList;

public class Page {

    private int pageId;
    private ArrayList<Object[]> recordList; // Array list of all the record stored ona page

    /**
     * Page Constructor
     * @param pageId the page id
     * @param record1 the 1st record to be added to the page
     */
    public Page(int pageId, Object[] record1) {
        this.pageId = pageId;
        this.recordList = new ArrayList<Object[]>();
        this.recordList.add(record1); // add the first record to the beginning of the arrayList
    }
}
