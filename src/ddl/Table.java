package ddl;

import java.util.ArrayList;

public class Table {
    private int id;
    private String name;
    private ArrayList<Attribute> attributes;
    private ArrayList<Attribute> primaryKeys;
    private ArrayList<Attribute> unique;
    // TODO foreign key tbd

    public Table(int id, String name) {
        this.id = id;
        this.name = name;
        this.attributes = new ArrayList<>();
        this.primaryKeys = new ArrayList<>();
    }

    public int getId(){
        return this.id;
    }

    /**
     * Adds a attribute to the primary key in order. Order matters.
     * @param attribute primary key Attribute Object
     */
    public void addPrimaryKey(Attribute attribute) {
        primaryKeys.add(attribute);
    }

    /**
     * Adds a attribute to the attribute list. Order matters.
     * @param attribute Attribute Object
     */
    public void addAttribute(Attribute attribute) {
        attributes.add(attribute);
    }

    public void addUnqiueAttribute(String name){
        Attribute uniqueAttr = attributes.get(attributes.indexOf(name));

    }
}
