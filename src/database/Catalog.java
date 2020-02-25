package database;

import java.util.Map;

import ddl.Table;

/**
 * Catalog class manages table data for the database
 */
public class Catalog {
    private Map<String, Table> tables;
    private Map<String, Integer> tableSizes;
    
    /**
     * Adds a table to the catalog
     * @param table Table to add to the catalog
     * @param tableSize size of the table
     */
    public void addTable(Table table, Integer tableSize) {
        tables.put(table.getName(), table);
        tableSizes.put(table.getName(), tableSize);
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
     * Gets the size of the table based on the given ID
     * @param tableID 
     * @return the size of the table
     */
    public int getTableSize(String tableName) {
        return tableSizes.get(tableName);
    }

    /**
     * Removes the given table from the catalog
     * @param tableName 
     * @return
     */
    public void dropTable(String tableName) {
        tables.remove(tableName);
        tableSizes.remove(tableName);
    }
}
