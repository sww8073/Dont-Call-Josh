/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */

package storagemanager;

public class Record {
    //private instance variables
    private int tableNum;
    private Object[] record;

    /**
     * Record Constructor
     * @param tableNum the number of the table the record is in
     * @param record keyValue an array representing the key to find
     */
    public Record(int tableNum, Object[] record) {
        this.tableNum = tableNum;
        this.record = record;
    }
}
