package database;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import ddl.DDLParserException;
import ddl.ForeignKey;
import ddl.Table;

/**
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */

/**
 * Catalog class manages table data for the database
 */
public class Catalog implements Serializable {
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
        boolean answer = tables.containsKey(table);
        return answer;
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
     * This functions adds a foreign key reference form another table
     * @param foreignKey
     */
    public void addForeignKeyReference(ForeignKey foreignKey)  throws DDLParserException {
        String foreignTableName = foreignKey.getForeignTableName();
        if(tables.containsKey(foreignTableName))    {
            Table foreignTable = tables.get(foreignTableName); // gets the foreign table
            foreignTable.addForeignKeyReference(foreignKey); // adds the foreign key reference
            tables.replace(foreignTableName, foreignTable); // updates the table in the map
        }
        else
            throw new DDLParserException(foreignTableName + " does not exist");
    }
}
