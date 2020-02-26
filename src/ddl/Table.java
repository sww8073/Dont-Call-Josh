package ddl;

import java.util.ArrayList;

public class Table {
    private int id;
    private String name;
    private ArrayList<Attribute> attributes;
    private ArrayList<String> primaryKeys;
    private ArrayList<String> unique;
    private int size;
    // TODO foreign key tbd

    public Table(int id, String name) {
        this.id = id;
        this.name = name;
        this.attributes = new ArrayList<>();
        this.primaryKeys = new ArrayList<>();
        this.unique = new ArrayList<>();
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
}
