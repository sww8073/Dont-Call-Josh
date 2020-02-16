package ddl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This a a parser for DDL statements
 */

public class DDLParser implements IDDLParser {

    /**
     * This will create an instance of this parser and return it.
     * @return an instance of a IDDLParser
     */
    public static IDDLParser createParser(){
        return new DDLParser();
    }

    public void parseDDLstatement(String statement) throws DDLParserException {
        String[] wordsInStatement = statement.split(" ");
        String option = wordsInStatement[0].toLowerCase();

        switch(option){
            case "create":
                createTable(statement);
                break;

            case "drop":
                dropTable(statement);
                break;

            case "alter":
                alterTable(statement);
                break;

            default:
                throw new DDLParserException("Command not recognized.");
        }
    }

    /**
     * This function will parse create table arguments, and use the storage manager
     * @param statement create table statement
     */
    public void createTable(String statement) throws DDLParserException {String prefix = statement.substring(0, statement.indexOf("("));
        String[] wordsInPrefix = prefix.split("\\s+");

        // check for incorrect create table statement
        if (wordsInPrefix.length != 3)
            throw new DDLParserException("Invalid create table query");
        else if (wordsInPrefix[1].compareToIgnoreCase("table") != 0)
            throw new DDLParserException("Invalid create table query");

        String tableName = wordsInPrefix[2];
        ArrayList<String> dataTypes = new ArrayList<>(); // list that contains data types
        ArrayList<String> keyIndices = new ArrayList<>(); // list that contains data types

        //splits statement into attributes
        String attributes = statement.substring(statement.indexOf("(") + 1, statement.indexOf(");"));
        String[] attributesSplit = attributes.split(",");

        String[] keyWords = {"primarykey", "foreignkey"};
        List<String> keyWordsList = Arrays.asList(keyWords);
        String[] constraints = {"unique", "notnull", "primarykey"};
        List<String> constraintList = Arrays.asList(constraints);
        String[] types = {"double", "integer", "char", "varchar"};
        List<String> typesList = Arrays.asList(types);


        //get things for each attribute
        for (String attribute : attributesSplit) {
            String[] attributeTypes = attribute.split("\\s+");
            int attrSize = attributeTypes.length;

            String name = attributeTypes[0];
            if (name.contains("(")) {
                name = new String(name.substring(0, name.indexOf("(")));
            }
            if (keyWordsList.contains(name.toLowerCase())) { // contains a key
                switch (attributeTypes[attrSize - 1].toLowerCase()) {
                    case "primarykey":
                        System.out.println("primarykey");
                        break;

                    case "unique":
                        System.out.println("foriegnkey");
                        break;
                }
            } else { //
                if (constraintList.contains(attributeTypes[attrSize - 1].toLowerCase())) { // contains special constraints
                    for (int i = 0; i < attrSize; i++) {
                        switch (attributeTypes[i].toLowerCase()) {
                            case "notnull":
                                System.out.println("notnull");
                                break;
                            case "unique":
                                System.out.println("unique");
                                break;
                            case "primarykey":
                                System.out.println("unique");
                                break;
                        }
                    }
                } else if (attrSize > 2)
                    throw new DDLParserException("Invalid constraint");
                else if (!typesList.contains(attributeTypes[1].toLowerCase()))
                    throw new DDLParserException("Invalid data type");
                dataTypes.add(attributeTypes[1].toLowerCase());
            }
        }
        int i = 0;
        i++;
//        String[] wordsInStatement = statement.split(" ");
//        String tableName = wordsInStatement[2].substring(0,wordsInStatement[2].length()-1);
//        //TODO make table with this name
//
//        //splits statement into attributes
//        String attributes = statement.substring(statement.indexOf("(")+1,statement.indexOf(");"));
//        String[] attributesSplit = attributes.split(",");
//        //TODO add attributes to table
//
//        //TODO actually do something with this info
//        //get things for each attribute
//        for (String attribute: attributesSplit) {
//
//            String[] attributeTypes = attribute.split(" ");
//
//            String name = attributeTypes[0];//name of attribute
//            String type = attributeTypes[1].toLowerCase();//type for attribute
//            int charVariable = 0;
//
//            String[] keys;
//            String[] references;
//
//            System.out.println();
//            System.out.println(name);
//
//            //name is primarykey(...), foreignkey(...), or unique(...)
//            if(name.contains("(")){
//                name = new String(name.substring(0,name.indexOf("(")));
//                //TODO get keys and references
//            }
//
//            //is a var(x) or varchar(x)
//            if(type.contains("(") && type.contains(")")){
//                charVariable = Integer.parseInt(type.substring(type.indexOf("(")+1,type.indexOf(")")));
//                type = new String(type.substring(0,type.indexOf("(")));
//            }
//
//            //types can be integer, double, boolean, char(x), and varchar(x)
//            switch (type){
//                case "integer":
//                    System.out.println("integer");
//                    break;
//
//                case "double":
//                    System.out.println("double");
//                    break;
//
//                case "boolean":
//                    System.out.println("boolean");
//                    break;
//
//                case "char":
//                    System.out.println("char(" + charVariable + ")");
//                    break;
//
//                case "varchar":
//                    System.out.println("varchar(" + charVariable + ")");
//                    break;
//            }
//
//            //attribute has constraints
//            if(attributeTypes.length > 2){
//                //get constraints
//                String[] constraints = new String[attributeTypes.length-2];
//                for (int i = 0; i < attributeTypes.length-2; i++) {
//                    constraints[i] = attributeTypes[i+2];
//                }
//                for (String constraint: constraints) {
//                    switch (constraint){
//                        case "notnull":
//                            System.out.println("notnull");
//                            break;
//
//                        case "primarykey":
//                            System.out.println("primarykey");
//                            break;
//
//                        case "unique":
//                            System.out.println("unique");
//                            break;
//                    }
//                }
//            }
//
//            //name is primarykey, foreignkey, or unique
//            switch(name){
//                case "primarykey":
//                    System.out.println("primarykey");
//                    break;
//
//                case "unique":
//                    System.out.println("unique");
//                    break;
//
//                case "foreignkey":
//                    System.out.println("foreiignkey");
//                    break;
//            }
//        }
    }

    public void dropTable(String statement){

    }

    public void alterTable(String statement){

    }

}
