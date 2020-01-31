/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;

import java.util.Dictionary;

/**
 * this class contains the logic for managing and maintianing tables
 */
public class Table {

    // private instance variables
    private int table;
    private String[] dataTypes;
    private Integer[] keyIndices;
    private Record[] records;

    /**
     * Table constructor that creates an empty table
     * @param table number of the table
     * @param dataTypes ArrayList of Strings representing the data types stored in the table
     *                  The order of the types must match the order of the records
     * @param keyIndices ArrayList containing the indices of the primary key attributes of the table.
     *                   Order is important. (1,2) and (2,1) are different.
     */
    public Table(int table, String[] dataTypes, Integer[] keyIndices) {
        this.table = table;
        this.dataTypes = dataTypes;
        this.keyIndices = keyIndices;
    }
}
