package ddl;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * this class represents an attribute within a relation
 */
public class Attribute implements Serializable {
    private String name;
    private String type;
    private ArrayList<String> constraints;

    public Attribute(String name, String type) throws DDLParserException {
        name = name.toLowerCase();
        type = type.toLowerCase();
        type = type.replaceAll("[^a-zA-Z0-9]", ""); // remove special characters
        type = type.replaceAll("\\d", ""); // remove digits
        String[] typeArr = {"double", "integer", "char", "varchar", "boolean"};
        List<String> typeList = Arrays.asList(typeArr);

        if(typeList.contains(type)) { // checks for valid type
            this.name = name;
            this.type = type;
            this.constraints = new ArrayList<>();
        }
        else {
            throw new DDLParserException("Invalid type: " + type);
        }
    }

    /**
     * This function adds a constraint to an attribute. If an invalid constraint is added
     * an exception will be thrown. A primary constraint will be considered in valid
     * @param constraint
     * @throws DDLParserException
     */
    public void addConstraintNoForeign(String constraint) throws DDLParserException  {
        constraint = constraint.toLowerCase();
        if(constraint.equals("foreignkey"))    { // valid constraint
            throw new DDLParserException("Invalid constraint");
        }
        else {
            addConstraint(constraint);
        }
    }

    /**
     * This function adds a constraint to an attribute. If an invalid constraint is added
     * an exception will be thrown.
     * @param constraint
     * @throws DDLParserException
     */
    public void addConstraint(String constraint) throws DDLParserException  {
        constraint = constraint.toLowerCase();
        String[] allValidConstraintsArr = {"unique", "notnull", "primarykey", "foreignkey"};
        List<String> allValidConstraintsList = Arrays.asList(allValidConstraintsArr);

        if(constraint.contains(constraint))    { // valid constraint
            constraints.add(constraint);
        }
        else {
            throw new DDLParserException("Invalid constraint");
        }
    }

    public String getName(){
        return this.name;
    }

    @Override
    public boolean equals(Object o){
        if(o == this){
            Attribute attr = (Attribute) o;
            if(attr.name.equals(this.name)){
                return true;
            }
        }
        return false;
    }
    
    /**
     * Checks to see if the attribute is a primary key
     * @return Whether the attribute is a primary key
     */
    public boolean isPrimary() {
        if (constraints.contains("primarykey")) {
            return true;
        }
        return false;
    }

    public String getType(){
        return type;
    }

}
