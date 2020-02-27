package database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import ddl.Table;

/**
 * Catalog class manages table data for the database
 */
public class Catalog {
    private Map<String, Table> tables;

    /**
     * default constructor
     */
    public Catalog() {
        this.tables = new HashMap<String, Table>();
    }

    /**
     * Adds a table to the catalog
     * @param table Table to add to the catalog
     */
    public void addTable(Table table) {
        tables.put(table.getName(), table);
    }

    /**
     * Checks to see if the table is in the catalog
     * @param table the table to check
     * @return whether the table exists
     */
    public boolean tableExists(String table) {
        return tables.containsKey(table);
    }

    /**
     * Gets table of given ID
     * @param tableName string name of the table
     * @return the table
     */
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    /**
     * Removes the given table from the catalog
     * @param tableName 
     * @return
     */
    public void dropTable(String tableName) {
        tables.remove(tableName);
    }

    /**
     * This function adds a foreign key
     * @param table1Name the table containing the foreign key
     * @param table1KeyAttr the key attributes, by name not index
     * @param table2Name the table that the foreign key is referencing
     * @param table2KeyAttr the key attributes, by name not index
     */
    public void addForeignKey(String table1Name, ArrayList[] table1KeyAttr, String table2Name,
                              ArrayList[] table2KeyAttr) {

    }
}
