package ddl;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
public class ForeignKey implements Serializable {
    private String tableName; // name of the table that has the foreign key
    private ArrayList<String> keyIndices;

    private String foreignTableName; // name of the table that is being referenced by the foreign key
    private ArrayList<String> foreignKeyIndices;

    public ForeignKey(String tableName, ArrayList<String> keyIndices, String foreignTableName, ArrayList<String> foreignKeyIndices) {
        this.tableName = tableName;
        this.keyIndices = keyIndices;
        this.foreignTableName = foreignTableName;
        this.foreignKeyIndices = foreignKeyIndices;
    }

    public String getForeignTableName() {
        return foreignTableName;
    }

    public String getTableName() {
        return tableName;
    }
}
