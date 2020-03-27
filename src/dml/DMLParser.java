package dml;
import database.Catalog;
import ddl.Attribute;
import ddl.ForeignKey;
import ddl.Table;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;

import java.util.*;

public class DMLParser implements IDMLParser {

    private static Catalog catalog;
    private static StorageManager storageManager;

    public DMLParser(){}

    public DMLParser(Catalog catalog, StorageManager storageManager)  {
        DMLParser.catalog = catalog;
        DMLParser.storageManager = storageManager;
    }

    /**
     * This will create an instance of this parser and return it.
     * @return an instance of a IDMLParser
     */
    public static IDMLParser createParser(){
        return new DMLParser();
    }

    @Override
    public void parseDMLStatement(String statement) throws DMLParserException {
        String[] wordsInStatement = statement.split(" ");
        String option = wordsInStatement[0].toLowerCase();

        switch(option){
            case "insert":
                insertTable(statement);
                break;

            case "update":
                updateTable(statement);
                break;

            case "delete":
                deleteTable(statement);
                break;

            default:
                throw new DMLParserException("Command not recognized.");
        }
    }
    
    @Override
    public Object[][] parseDMLQuery(String statement) throws DMLParserException{
        //TODO THIS WILL BE IMPLEMENTED IN A LATER PHASE (Prob phase 4)
        return null;
    }

    /**
     * This function parses insert DML Statements and adds a relation to a table.
     * @param statement An insert statement as a string
     * @throws DMLParserException
     */
    public void insertTable(String statement) throws DMLParserException {
        String prefix = statement.substring(0, statement.indexOf("("));
        String[] wordsInPrefix = prefix.split("\\s+");

        if(wordsInPrefix.length != 4)
            throw new DMLParserException("Illegal insert statement prefix");
        else if(!wordsInPrefix[0].toLowerCase().equals("insert")
                || !wordsInPrefix[1].toLowerCase().equals("into")
                || !wordsInPrefix[3].toLowerCase().equals("values"))   {
            throw new DMLParserException("Illegal insert statement prefix");
        }

        String tableName = wordsInPrefix[2].toLowerCase();
        catalog.tableExists(tableName);

        // get the suffix starting with the first "(" and ending with last ")" skipping the ";"
        String suffix = statement.substring(statement.indexOf("(") - 1, statement.length() -1);

        insertRelations(tableName, suffix);
    }

    /**
     * This function inserts the each tuple into its corresponding table
     * @param tableName the name of the table being inserted into
     * @param relationString the unparsed string containing 1 or more tuple
     * @throws DMLParserException
     */
    public void insertRelations(String tableName, String relationString)  throws DMLParserException {
        relationString = relationString.replaceAll("[\\(\\)]", "");
        String[] relations = relationString.trim().split(",");

        Table table = catalog.getTable(tableName);
        if(table == null)   {
            throw new DMLParserException("Table does not exist");
        }
        String types[] = table.getDataTypes();

        for(String relation: relations) {
            try {
                int tableNum = table.getId(); // get table id

                String[] attrs = relation.trim().split(" ");
                Object[] convertedAttrs = convertTupleType(types, attrs); // converts attrs to correct Object types
                if(checkConstraints(table, convertedAttrs)) { // check to make sure all constraints are followed
                    storageManager.insertRecord(tableNum, convertedAttrs);
                }
            }
            catch (StorageManagerException e)   {
                throw new DMLParserException(e.getMessage());
            }
        }
    }

    /**
     * This function checks all the constraints associated with the tuple. If any constraints are violated
     * then an exception will be thrown
     * @param table the table the relation is being added to
     * @param relation Object array representing the relation
     * @return true if all the constraint rules are followed
     * @throws DMLParserException
     */
    private boolean checkConstraints(Table table, Object[] relation) throws DMLParserException   {
        int tableId = table.getId();
        ArrayList<Attribute> attributes = table.getAttrs();
        ArrayList<String> uniqueAttrNames = table.getUniqueAttrs();

        for(int i = 0;i < attributes.size();i++)    {
            Attribute currentAttr = attributes.get(i);

            // check if attribute is unique
            if(uniqueAttrNames.contains(currentAttr.getName())) {
                try {
                    // loop through all record and check for a duplicate
                    Object[][] records = storageManager.getRecords(tableId);
                    for(int j = 0;j < records.length;j++)   {
                        if(records[j][i].equals(relation[i]))
                            throw new DMLParserException( relation[i] + " is not unique");
                    }
                }
                catch (StorageManagerException e)   { throw new DMLParserException(e.getMessage()); }
            }

            // check not null constraint
            ArrayList<String> constraints = currentAttr.getConstraints(); // list aon constraint strings
            if((constraints.contains("notnull") || constraints.contains("not null"))
                    && relation[i] == null)    {
                // attr has not null constraint and the attribute is null
                throw new DMLParserException(currentAttr.getName() + " cannot be null");
            }
        }

        if(!doesForeignAttrExist(relation, table))  {
            throw new DMLParserException("Cannot insert foreign key that does not exits");
        }
        return true;
    }

    /**
     * This function checks if the table has any foreign keys. If it does it it check to make sure the value being
     * references exists within the foreign table.
     * @param relation Relation being inserted
     * @param table The table with possible foreign key
     * @return true if all foreign keys are valid
     * @throws DMLParserException
     */
    private boolean doesForeignAttrExist(Object[] relation, Table table) throws DMLParserException   {
        ArrayList<ForeignKey> fkList = new ArrayList<>(table.getForeignKeys().values());

        if(fkList.size() == 0) // there are no foreign keys to check, everything is good
            return true;

        for(int i = 0;i < fkList.size();i++)  { // loop through all foreign keys
            ForeignKey fk = fkList.get(i);
            ArrayList<String> kIndicesNames =  fk.getKeyIndices();
            ArrayList<String> fkIndicesNames = fk.getForeignKeyIndices();

            ArrayList<Integer> kIndexes = getNameIndexes(table, kIndicesNames);

            String foreignTableName = fk.getForeignTableName();
            Table foreignTable = catalog.getTable(foreignTableName);
            ArrayList<Integer> fkIndexes = getNameIndexes(foreignTable, fkIndicesNames);

            try {
                // get all the record in the foreign table, loop through all of them and check to see if the referenced
                //  relation exists
                Object records[][] = storageManager.getRecords(foreignTable.getId());
                for(int j = 0;j < records.length;j++)   {
                    if(compareTupleIndexes(relation, kIndexes, records[j], fkIndexes))
                        return true;
                }
                return false; // no matching relation was found
            }
            catch (StorageManagerException e)   { throw new DMLParserException(e.getMessage()); }
        }
        return true;
    }

    /**
     * This function compares the index of two different tuples
     * @param tuple1 tuple being compared
     * @param tuple1Indexes indexes being compared
     * @param tuple2 tuple being compared
     * @param tuple2Indexes indexes being compares
     * @return true if tuple indexes match
     */
    private boolean compareTupleIndexes(Object[] tuple1, ArrayList<Integer> tuple1Indexes,
                                        Object[] tuple2, ArrayList<Integer> tuple2Indexes)   {
        if(tuple1Indexes.size() != tuple2Indexes.size())
            return false;
        for(int i = 0;i < tuple1Indexes.size();i++) {
            Object tuple1Attr = tuple1[tuple1Indexes.get(i)];
            Object tuple2Attr = tuple2[tuple2Indexes.get(i)];
            if(!tuple1Attr.equals(tuple2Attr))
                return false;
        }
        return true;
    }

    /**
     * This function takes foreign key attribute names and gets the corresponding indexes in a
     * given table
     * @param table the table in which you need the indexes of the names
     * @param names the names that you want the indexes of
     * @return ArrayList of indexes corresponding to the names
     */
    private ArrayList<Integer> getNameIndexes(Table table, ArrayList<String> names) {
        ArrayList<Integer> attrNameIndexes = new ArrayList<>();
        ArrayList<Attribute> attrNameList = table.getAttrs();

        for(int i = 0;i < attrNameList.size();i++)  {
            String currentAttrName = attrNameList.get(i).getName();
            for(int j = 0;j < names.size();j++) {
                if(names.get(j).equals(currentAttrName))    {
                    attrNameIndexes.add(i);
                }
            }
        }
        return attrNameIndexes;
    }

    /**
     * This function converts a array of attributes to their corresponding types
     * @param types array list of lowercase strings representing Object types
     * @param attrs array of attributes to be converted
     * @return converted array of objects
     * @throws DMLParserException
     */
    private Object[] convertTupleType(String[] types, String[] attrs)  throws DMLParserException {
        int typesLength = types.length;
        int attrLength = attrs.length;

        if(typesLength != attrLength)
            throw new DMLParserException("Incorrect amount of attributes");

        Object convertedAttrs[] = new Object[typesLength];
        for(int i = 0;i < typesLength;i++)  {
            convertedAttrs[i] = convertAttrType(types[i], attrs[i]);
        }
        return convertedAttrs;
    }

    /**
     * This function converts a single string that represents an attribute to its corresponding
     * object type
     * @param type lowercase string representing an object type
     * @param value the type needing to be converted
     * @return converted Object
     * @throws DMLParserException
     */
    private Object convertAttrType(String type, String value)  throws DMLParserException {
        try {
            if(value.equals("null"))    {
                return null;
            }
            else if (type.equals("double"))
                return Double.parseDouble(value);
            else if (type.equals("integer"))
                return Integer.parseInt(value);
            else if (type.equals("char"))
                return value;
            else if (type.startsWith("varchar"))
                return value.replaceAll("\"","");
            else if (type.equals("boolean"))
                return Boolean.parseBoolean(value);
            else
                throw new DMLParserException("Could not convert " + type + " to a " + type);
        }
        catch (Exception e) {
            throw new DMLParserException("Could not convert " + type + " to a " + type);
        }
    }

    /**
     * update: All DML statements that start with this will be considered to be trying to update data in a table.
     * <name>: is the name of the table to update in. All table names are unique.
     * set<column1>=<value>, ...,<columnN>=<value> Sets the columns to the provided values.
     * <value>: attribute name, constant value, or a mathematical operation.
     * where<condition>: A condition where a tuple should updated. If this evaluates to true  the  tuple  is  updated;
     * otherwise  it remains  the  same.  See  below  for  evaluating conditionals. If there is no where clause it is
     * considered to be a where true and all tuples get updated.
     * @param statement the update statement sent to the database
     * @throws DMLParserException
     */
    public void updateTable(String statement) throws DMLParserException{
        String[] wordsInStatment = statement.split(" ");
        String tableName = wordsInStatment[1];
        String setValue = wordsInStatment[3];
        Table table;
        try{
            table = catalog.getTable(tableName);
        }
        catch(Exception e){
            throw new DMLParserException("Table \"tableName\" does not exist");
        }

        //where clause
        if(statement.contains("where")){
            //where <value> = <value>
            if(statement.contains("and") || statement.contains("or")){

                handleCondtional(table, statement.substring(statement.indexOf("where")));

            }
            //no "and" or "or"
            else {

                String attrToChange = wordsInStatment[3];
                String newValue = wordsInStatment[5];
                if(newValue.contains("\"")) {
                    newValue = newValue.substring(1, newValue.length() - 1);
                }

                String whereClause = statement.substring(statement.indexOf("where"));
                String[] whereClauseWords = whereClause.split(" ");

                String attr = whereClauseWords[1];
                String value = whereClauseWords[3];
                value = value.substring(0,value.length()-1);

                System.out.println(value);
                if(table.attributeExists(attr)){//ex: table has "id"
                    String[] dataTypes = table.getDataTypes();
                    Attribute attribute = table.getAttribute(attr);
                    Attribute attributeToChange = table.getAttribute(attrToChange);
                    int indexOfAttr = table.getAttrs().indexOf(attribute);
                    int indexOfAttrToChange = table.getAttrs().indexOf(attributeToChange);
                    String attrType = dataTypes[indexOfAttr];

                    Object attrObject = convertAttrType(attrType.toLowerCase(),value);

                    try {
                        for (Object[] record : storageManager.getRecords(table.getId())) {
                            if(record[indexOfAttr].equals(attrObject)){
                                //update record
                                record[indexOfAttrToChange] = newValue;
                                storageManager.updateRecord(table.getId(),record);
                            }
                        }
                    }catch(StorageManagerException e){
                        throw new DMLParserException("Table " + table.getName() + " does not exist.");
                    }
                }


            }
        }
        else { // there is no where clause
            try {
                int tableId = table.getId();
                Object[][] relations = storageManager.getRecords(tableId);

            }
            catch (StorageManagerException e) { throw new DMLParserException(e.getMessage()); }
        }

    }

    /**
     * This function gets the index of an attribute name from a table
     * @param tableName String table name
     * @param attrName the name of the attribute being looked for
     * @return the index of that attribute
     * @throws DMLParserException
     */
    private int getIndexFromTable(String tableName, String attrName) throws DMLParserException   {
        Table table = catalog.getTable(tableName);

        if(table == null)
            throw new DMLParserException("Table does not exist");

        ArrayList<Attribute> attrs = table.getAttrs();
        for(int i = 0;i < attrs.size();i++) {
            String attrIndexName = attrs.get(i).getName();
            if(attrName.equals(attrIndexName))  {
                return i;
            }
        }
        return -1;
    }

    private void handleCondtional(Table table, String statement){

        ArrayList<ArrayList<Object[]>> orlists = new ArrayList<>();

        if(statement.contains("and") && !statement.contains("or")){

        }
        else if(statement.contains("or") && !statement.contains("and")){

        }
        else if(statement.contains("and") && statement.contains("or")){
            String[] ors = statement.split("or");

            for (String orStatment: ors) {

                if(orStatment.contains("and")){

                    ArrayList<ArrayList<Object[]>> andLists = new ArrayList<>();
                    String[] ands = orStatment.split("and");

                    for (String and: ands) {
                        String[] wordsInAnd = and.split(" ");
                        String condition = wordsInAnd[2];

                        if(condition.contains("\"")){
                            condition.replace("\"", "");
                        }

                        String attributeName = wordsInAnd[0];
                        Attribute attribute = table.getAttribute(attributeName);
                        int indexOfAttribute = table.getAttrs().indexOf(attribute);

                        ArrayList<Object[]> recordsForAnd = new ArrayList<>();

                        try {
                            for (Object[] record : storageManager.getRecords(table.getId())) {
                                if(record[indexOfAttribute].equals(condition)){
                                    recordsForAnd.add(record);
                                }
                            }
                        }catch(StorageManagerException e){}

                        andLists.add(recordsForAnd);

                    }

                    for (ArrayList<Object[]> list: andLists) {
                        andLists.get(0).retainAll(list);
                    }

                }

                else{

                }
            }

        }

    }

    public void deleteTable(String statement) throws DMLParserException{
        String[] wordsInStatement = statement.split(" ");
        String table = wordsInStatement[2];
        Table table1 = catalog.getTable(table);
        int tableid = table1.getId();
        if( table1 == null){
            throw new DMLParserException("Table does not exist");
        }

        if(wordsInStatement.length == 3){
            try {
                Object[][] records = storageManager.getRecords(tableid);
                for(int j = 0; j<records.length; j++){
                    Object rec[] = records[j];
                    storageManager.removeRecord(tableid,rec);
                }
            }catch (StorageManagerException e) {
                throw new DMLParserException("Could not retrieve records");
            }
        }
        else{
            int count = 0;
            String attribute = "";
            String conditional = "";
            String value = "";
            String valueType = "";
            int index = 0;
            Attribute attribute1;

            boolean and = false;
            boolean or = false;
            boolean loop = false;

            try {
                Object[][] records = storageManager.getRecords(tableid);
                Object[][] newRecords = records;
                Object[][] orArray;
                for(int i = 4; i < wordsInStatement.length; i++){
                    if (count == 0) {
                        attribute = wordsInStatement[i];
                        attribute1 = table1.getAttribute(attribute);
                        if (attribute1 == null) {
                            throw new DMLParserException("Attribute does not exist");
                        }
                        index = table1.getIndex(attribute);
                        count++;
                        continue;
                    }
                    else if (count == 1){
                        conditional = wordsInStatement[i];
                        count++;
                        continue;
                    }
                    else if (count == 2){
                        if( i == wordsInStatement.length - 1){
                            value = wordsInStatement[i].substring(0, wordsInStatement[i].length() - 1);
                        }
                        else{
                            value = wordsInStatement[i];
                        }
                        valueType = checkType(value);
                        attribute1 = table1.getAttribute(attribute);
                        String type = attribute1.getType();
                        if (!valueType.equals(type)){
                            throw new DMLParserException("Type does not match");
                        }
                    }
                    if(and){
                        and = false;
                        newRecords = acquireRecords(newRecords, attribute, index, conditional, value, valueType, table1);
                        loop = true;
                        if( i == wordsInStatement.length - 1){
                            break;
                        }
                    }
                    else if(or){
                        or = false;
                        orArray = acquireRecords(records, attribute, index, conditional, value, valueType, table1);
                        newRecords = mergeArray(newRecords,orArray);
                        loop = true;
                        if( i == wordsInStatement.length - 1){
                            break;
                        }
                    }
                    if( i == wordsInStatement.length - 1){
                        newRecords = acquireRecords(records, attribute, index, conditional, value, valueType, table1);
                    }
                    else{
                        i++;
                        count = 0;
                        if(!loop){
                            newRecords = acquireRecords(records, attribute, index, conditional, value, valueType, table1);
                        }
                        if (wordsInStatement[i].equals("and")){
                            and = true;
                        }
                        else{
                            or = true;
                        }
                    }
                    loop = false;

                }
                records = newRecords;
                for (Object[] rec : records) {
                    storageManager.removeRecord(tableid, rec);
                }
            } catch (StorageManagerException e) {
                throw new DMLParserException("Could not retrieve records");
            }

        }
    }

    private Object[][] mergeArray(Object[][] a, Object[][] b){
        Object[][] result = new Object[a.length + b.length][];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    /**
     * Helper function to determine type of value for delete
     * @param value String value
     * @return String type
     */
    private String checkType(String value){
        String blank;
        try{
            Integer.parseInt(value);
            blank = "integer";
            return blank;
        }catch (Exception e){}
        if( value.equals("true") || value.equals("false")){
            blank = "boolean";
            return blank;
        }
        try{
            double d = Double.parseDouble(value);
            blank = "double";
            return blank;
        }catch (NumberFormatException e){}

        return "char";
    }

    /**
     * acquireRecords filters out records in the 2d array that corresponds to the where clause given
     * @param records
     * @param attribute
     * @param index
     * @param conditional
     * @param value
     * @param type
     * @return
     */
    private Object[][] acquireRecords(Object[][] records, String attribute, int index, String conditional, String value, String type, Table table1){
        Object[][] newRecords = new Object[1][]; // This is wrong, needs to be size of filtered array
        int count = 0;
        int size = 1;
        boolean num = false, doub = false, bool = false, charac = false;
        switch(type){
            case "integer":
                num = true;
                break;
            case "double":
                doub = true;
                break;
            case "char":
                charac = true;
                break;
            case "boolean":
                bool = true;
                break;
        }
        if(num){
            int val = Integer.parseInt(value);
            for(int i=0; i<records.length; i++) {
                boolean add = false;
                Object[] record = records[i];
                int rec = (Integer) record[index];
                switch(conditional){
                    case "=":
                        if(rec == val){
                            add = true;
                        }
                        break;
                    case ">":
                        if(rec > val){
                            add = true;
                        }
                        break;
                    case "<":
                        if(rec < val){
                            add = true;
                        }
                        break;
                    case ">=":
                        if(rec >= val){
                            add = true;
                        }
                        break;
                    case "<=":
                        if(rec <= val){
                            add = true;
                        }
                        break;
                }
                if(add){
                    newRecords[count] = record;
                    size++;
                    newRecords = Arrays.copyOf(newRecords, size);
                }
            }
        }
        else if(doub){
            double val = Double.parseDouble(value);
            for(int i=0; i<records.length; i++) {
                boolean add = false;
                Object[] record = records[i];
                double rec = (Double) record[index];
                switch(conditional){
                    case "=":
                        if(rec == val){
                            add = true;
                        }
                        break;
                    case ">":
                        if(rec > val){
                            add = true;
                        }
                        break;
                    case "<":
                        if(rec < val){
                            add = true;
                        }
                        break;
                    case ">=":
                        if(rec >= val){
                            add = true;
                        }
                        break;
                    case "<=":
                        if(rec <= val){
                            add = true;
                        }
                        break;
                }
                if(add){
                    newRecords[count] = record;
                    size++;
                    newRecords = Arrays.copyOf(newRecords, size);
                }
            }
        }
        else if(bool){
            //TODO Check how true/false is being stored as
            if(value.equals("true")){
                bool = true;
            }
            else{
                bool = false;
            }
            for(int i=0; i<records.length; i++) {
                boolean add = false;
                Object[] record = records[i];
                boolean rec = (Boolean) record[index];
                if(rec == bool){
                    add = true;
                }
                if(add){
                    newRecords[count] = record;
                    size++;
                    newRecords = Arrays.copyOf(newRecords, size);
                }
            }
        }
        else if(charac){
            if(value.contains("\"")){
                //Compare String values
                String val = value.replace("\"", "");
                for(int i=0; i<records.length; i++) {
                    boolean add = false;
                    Object[] record = records[i];
                    String rec = (String) record[index];
                    int compare = val.compareTo(rec);
                    switch(conditional){
                        case "=":
                            if(compare == 0){
                                add = true;
                            }
                            break;
                        case ">":
                            if(compare > 0){
                                add = true;
                            }
                            break;
                        case "<":
                            if(compare < 0){
                                add = true;
                            }
                            break;
                    }
                    if(add){
                        newRecords[count] = record;
                        size++;
                        newRecords = Arrays.copyOf(newRecords, size);
                    }
                }
            }
            else{
                //Compare two columns
                int index2 = table1.getIndex(value);
                for(int i=0; i<records.length; i++) {
                    boolean add = false;
                    Object[] record = records[i];
                    Object o1 = record[index];
                    Object o2 = record[index2];
                    if( o1 == o2){
                        add = true;
                    }
                    if(add){
                        newRecords[count] = record;
                        size++;
                        newRecords = Arrays.copyOf(newRecords, size);
                    }
                }
            }
        }
        return newRecords;
    }

}
