package ddl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Table {
    private int id;
    private String name;
    private ArrayList<Attribute> attributes;
    private ArrayList<String> primaryKeys;
    private ArrayList<String> unique;

    // foreign keys that this table uses to reference other tables
    private Map<String, ForeignKey> foreignKeys; // key - table name, value - ForeignKey Object

    // foreign keys that other tables have that reference this table
    private Map<String, ForeignKey> foreignKeysRefThisTable; // key - table name, value - ForeignKey Object
    private int size;

    public Table(int id, String name) {
        this.id = id;
        this.name = name;
        this.attributes = new ArrayList<>();
        this.primaryKeys = new ArrayList<>();
        this.unique = new ArrayList<>();
        this.foreignKeys = new HashMap<String, ForeignKey>();
        this.foreignKeysRefThisTable = new HashMap<String, ForeignKey>();
        this.setSize(0);
    }

    public int getId() {
        return this.id;
    }

    /**
     * Adds a attribute to the primary key in order. Order matters.
     * @param name primary key Attribute name
     */
    public void addPrimaryKey(String name) throws DDLParserException {
        if(!primaryKeys.contains(name)) {
            name = name.toLowerCase();
            primaryKeys.add(name);
        }
        else{
            throw new DDLParserException(name + " is already primary key");
        }
    }

    /**
     * This function adds a foreign key to this table.
     * @param foreignKey foreign key object
     */
    public void addForeignKey(ForeignKey foreignKey)    {
        // adds foreign key to table that normally contains it
        foreignKeys.put(foreignKey.getForeignTableName(), foreignKey);
    }

    /**
     * This function adds a reference foreign key to this table.
     * @param foreignKey foreign key object
     */
    public void addForeignKeyReference(ForeignKey foreignKey)    {
        // tells the table that is being referenced that there is a foreign key associated with it
        foreignKeysRefThisTable.put(foreignKey.getTableName(), foreignKey);
    }

    /**
     * Adds a attribute to the attribute list. Order matters.
     * @param attribute Attribute Object
     */
    public void addAttribute(Attribute attribute) {
        attributes.add(attribute);
    }

    /**
     * drops an attribute to the attribute list. Order matters.
     * @param attribute Attribute Object
     */
    public void dropAttribute(String attribute) {
        for (Attribute atr : attributes) {
            if (atr.getName() == attribute) {
                attributes.remove(atr);
            }
        }
    }

    /**
     * This function adds a unique attribute
     * @param name the name of the new attribute
     * @throws DDLParserException
     */
    public void addUnqiueAttribute(String name) throws DDLParserException {
        if(!unique.contains(name)) {
            name = name.toLowerCase();
            unique.add(name);
        }
        else {
            throw new DDLParserException(name + " is already unique");
        }
    }

    public void printUniqueAttrs(){
        for (String attr: unique) {
            System.out.println(attr);
        }
    }

    public void printAttrs(){
        for (Attribute attr: attributes) {
            System.out.println(attr.getName());
        }
    }

    public String getName() {
        return name;
    }

    /**
     * Gets the size of the table.
     */
    public int getSize() {
        return size;
    }

    /**
     * Sets the size of the table.
     * @param size the size of the table.
     */
    public void setSize(int size) {
        this.size = size;
    }

    /**
     * Returns the attribute of a given name
     * @param attrName name of the attribute to return
     * @return the attribute
     */
    public Attribute getAttribute(String attrName) {
        for (Attribute attribute: attributes) {
            if(attribute.getName().equals(attrName)){
                return attribute;
            }
        }
        return null;
    }

    /**
     * returns a true if the table has this attribute
     * @param attrName
     */
    public boolean attributeExists(String attrName){
        for (Attribute attribute: attributes) {
            if(attribute.getName().equals(attrName)){
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString(){
        return this.name;
    }

    /**
     * gives the keyIndices of the table
     * @return
     */
    public Integer[] getKeyIndices(){
        Integer[] keyIndices = new Integer[primaryKeys.size()];
        int index = 0;
        for (String keyName: primaryKeys) {
            for (Attribute attribute : attributes) {
                if(keyName.equals(attribute.getName())){
                    keyIndices[index] = attributes.indexOf(attribute);
                    index++;
                }
            }
        }
        return keyIndices;
    }

    /**
     * gives the dataTypes of the table
     * @return
     */
    public String[] getDataTypes(){
        String[] dataTypes = new String[attributes.size()];
        int index = 0;
        for (Attribute attribute: attributes) {
            dataTypes[index] = attribute.getType();
            index++;
        }
        return dataTypes;
    }
}
