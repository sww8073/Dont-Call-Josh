/**
 * Phase 4
 * Team: Dont Tell Josh
 * 04/05/20
 */
package dml;

import database.Catalog;
import ddl.Attribute;
import ddl.Table;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;

import java.util.*;

public class Select {
    // static instance variables, same variables from DMLParser
    private static Catalog catalog;
    private static StorageManager storageManager;

    // private instance variables
    private String selectSubString; // ex: "select attr1, att2, table1.att3"
    private String fromSubString; // ex: "from table1, table2"
    private String whereSubString; // ex: "where attr1 = 1 and and attr2 = true"
    private String orderBySubString; // ex "order by attr3, attr1"

    private HashMap<Table, ArrayList<String>> selectFromHash; // key -> Table,  value -> ArrayList of selected attrs
    private HashMap<String, ArrayList<Object[]>> separatedSelects; // key -> tableName, value -> result of select

    private String[] tableOrder; // the order in which tables will be ordered
    /**
     * Constructor.
     */
    public Select(Catalog catalog, StorageManager storageManager, String selectString) throws DMLParserException    {
        this.catalog = catalog;
        this.storageManager = storageManager;
        this.separatedSelects = new HashMap<>();
        
        parseQuery(selectString); // call helper function to parse select statement
    }

    // getters
    public String getOrderBySubString() { return orderBySubString; }
    public HashMap<Table, ArrayList<String>> getSelectFromHash() { return selectFromHash; }
    public HashMap<String, ArrayList<Object[]>> getSeparatedSelects() { return separatedSelects; }

    /**
     * This is a helper function for the constructor. This parses the select statement and initializes
     * the subString instance variable.
     * @param selectString The select statement.
     * @throws DMLParserException
     */
    private void parseQuery(String selectString) throws DMLParserException   {
        selectString = selectString.toLowerCase().replace(";", "");
        if(!selectString.contains("select") || !selectString.contains("from"))
            throw new DMLParserException("Query must contain select and from");

        int beginOfFrom = selectString.indexOf("from"); // -1 if there is no "from"
        int beginOfWhere = selectString.indexOf("where"); // -1 if there is no "where"
        int beginOfOrderBy = selectString.indexOf("order by"); // -1 if there is no "order by"

        selectSubString = selectString.substring(0, beginOfFrom).trim();
        fromSubString = "";
        whereSubString = "";
        orderBySubString = "";

        if(beginOfWhere == -1 && beginOfOrderBy == -1) // no "where" and no "oder by"
            fromSubString = selectString.substring(beginOfFrom).trim();
        else if(beginOfWhere != -1) {// there is a "where" clause
            fromSubString = selectString.substring(beginOfFrom, beginOfWhere).trim();
            if (beginOfOrderBy == -1) // there is a "where" and no "order by"
                whereSubString = selectString.substring(beginOfWhere).trim();
            else { // there is a "where" and a "order by"
                whereSubString = selectString.substring(beginOfWhere, beginOfOrderBy).trim();
                orderBySubString = selectString.substring(beginOfOrderBy).trim();
            }
        }
        else if(beginOfOrderBy != -1) { // there is no "where" but there is an "oder by"
            fromSubString = selectString.substring(beginOfFrom, beginOfOrderBy).trim();
            orderBySubString = selectString.substring(beginOfOrderBy).trim();
        }

        // key - tableName, value - ArrayList of attributes
        this.selectFromHash = parseSelectAndFrom(selectSubString, fromSubString);

        // get the order of the tables for the cartesian product
        String tablesStr = fromSubString.toLowerCase().replace("from", "").trim();
        String[] tablesArr = tablesStr.split(",");
        for(int i = 0;i < tablesArr.length;i++) {
            tablesArr[i] = tablesArr[i].trim(); // trim extra spaces
        }
        tableOrder = tablesArr;
    }

    /**
     * This function parses the "select" and "from" part pf the query.
     * @param selectString "select attribute1, attribute2"
     * @param fromString "from table1, table2"
     * @return HashMap key table - table name, value - ArrayList of attributes
     * @throws DMLParserException
     */
    private HashMap parseSelectAndFrom(String selectString, String fromString) throws DMLParserException  {
        HashMap<Table, ArrayList<String>> tables = new HashMap<>(); // key - tableName, value - ArrayList of attributes

        fromString = fromString.replace("from", "").trim(); // remove "from"
        String tableNames[] = fromString.split(",");

        selectString = selectString.replace("select", "").trim(); // remove "select"
        String attrNames[] = selectString.split(",");

        int attrFoundCount = 0;

        for(int i = 0;i < tableNames.length;i++)    {
            String currTableName = tableNames[i].trim();
            ArrayList<String> currAttrList = new ArrayList<>();
            if(!catalog.tableExists(currTableName))
                throw new DMLParserException("Table " + currTableName + " does not exist");
            Table currTable = catalog.getTable(currTableName);

            for(int j = 0;j < attrNames.length;j++) {
                String curAttrName = attrNames[j].trim();
                if(currTable.attributeExists(curAttrName)) {
                    currAttrList.add(curAttrName);
                    attrFoundCount++;
                }
                else if(curAttrName.contains("."))  { // try to parse dot notation, ie "foo.id"
                    String splitAttr[] = curAttrName.split("[\\s.\\s]"); // split by spaces and "."

                    // table name matches AND attribute exists in table
                    if(splitAttr[0].equals(currTable.getName()) && currTable.attributeExists(splitAttr[1])) {
                        currAttrList.add(splitAttr[1]);
                        attrFoundCount++;
                    }
                }
                else if(curAttrName.equals("*"))    { // include all attributes
                    if(attrNames.length != 1) // the * and attributes together is an error
                        throw new DMLParserException("Cannot have * and other attributes together.");

                    // add all attributes form the table as selected attributes
                    ArrayList<Attribute> attrList = currTable.getAttrs();
                    for(Attribute attr : attrList)
                        currAttrList.add(attr.getName());
                    attrFoundCount = 1;
                }
            }
            tables.put(currTable, currAttrList);
        }
        if(attrFoundCount != attrNames.length) // all the attribute in the attribute list were not found
            throw new DMLParserException("Invalid Attributes");
        return tables;
    }

    /**
     * This functions runs the select from part of the query as separate parts.
     * ex: "select table1.attr1, table1.attr2, table2.attr1 form table1, table2;" will bw run as
     * "select table1.attr1, table1.attr2 form table1;" and "select table2.attr1 form table2;".
     * The results are added to the separateSelects Hash.
     * @throws DMLParserException
     */
    public void separateSelect() throws DMLParserException {
        Object[] tables = selectFromHash.keySet().toArray();
        for (Object tableObject: tables) { // loops through all tables covered by the og select statement
            Table currTable = (Table)tableObject;
            ArrayList<String> attributes = selectFromHash.get(currTable); // the selected attribute from each table

            // 2d records array containing tuples with only the selected attributes
            ArrayList<Object[]> recordsWithSelectedAttrs = new ArrayList<>();

            try {
                // gets all the records from the table
                Object[][] allRecords = storageManager.getRecords(currTable.getId());
                for(int i = 0;i < allRecords.length;i++) { // loop through all th records within currTable

                    // gets all the selected attributes from record i
                    Object[] selectedAttr = new Object[attributes.size()];
                    for (int j = 0; j < attributes.size(); j++) {
                        int attrIndex = currTable.getIndex(attributes.get(j));
                        selectedAttr[j] = allRecords[i][attrIndex];
                    }
                    recordsWithSelectedAttrs.add(selectedAttr); // adds selected attributes too solution data structure
                }
                separatedSelects.put(currTable.getName(), recordsWithSelectedAttrs);
            }
            catch(StorageManagerException e)    { throw new DMLParserException(e.getMessage()); }
        }
        int i = 0;
        i++;
    }

    /**
     * This function takes the cartesian product of everything in the separatedSelects hash.
     * @return A 2d Object array of the cartesian product
     * @throws DMLParserException
     */
    public Object[][] cartesianProduct() throws DMLParserException {
        // Collection<ArrayList<Object[]>> lists = separatedSelects.values();
        Collection<ArrayList<Object[]>> lists = new ArrayList<>();
        for(String tableName : tableOrder) {
            lists.add(separatedSelects.get(tableName));
        }

        // source https://codereview.stackexchange.com/questions/67804/generate-cartesian-product-of-list-in-java
        List<List<Object[]>> combinations = Arrays.asList(Arrays.asList());
        for (ArrayList<Object[]> list : lists) {
            List<List<Object[]>> extraColumnCombinations = new ArrayList<>();
            for (List<Object[]> combination : combinations) {
                for (Object[] element : list) {
                    List<Object[]> newCombination = new ArrayList<>(combination);
                    newCombination.add(element);
                    extraColumnCombinations.add(newCombination);
                }
            }
            combinations = extraColumnCombinations;
        }
        return convert2dListTo2dObject(combinations); // convert the 2d list to 2d Object array
    }

    /**
     * This function converts the 2d list to a 2d object array
     * @throws DMLParserException
     */
    private Object[][] convert2dListTo2dObject(List<List<Object[]>> list) throws DMLParserException  {
        if(list.size() < 1)
            throw new DMLParserException("Cartesian product is empty");

        // the second dim size is the sum of the length of all the individual object arrays
        int secondDimSize = 0;
        List<Object[]> firstList = list.get(0);
        for(int i = 0;i < firstList.size();i++)   {
            secondDimSize += firstList.get(i).length;
        }

        // iterate through 2d List and populate 2d Object Array
        Object[][] result = new Object[list.size()][secondDimSize];
        for(int i = 0;i < list.size();i++)  {
            int colIndex = 0;
            for(int j = 0;j < list.get(i).size();j++)   {
                for(int k = 0;k < list.get(i).get(j).length;k++)    {
                    result[i][colIndex] = list.get(i).get(j)[k];
                    colIndex++;
                }
            }
        }
        return result;
    }

    /**
     * This function gets the indexes that the cartesian product of all the select statements will be ordered by.
     * @param orderByString The unparsed order by string
     * @param selects key -> tableName, value -> ArrayList<String> attribute names
     * @return ArrayList of indexes for the cartesian product. Sorted in ascending order base on how the cartesian
     * product should be sorted.
     */
    public ArrayList<Integer> indexesToSortCartesianProd(
            String orderByString, HashMap<String, ArrayList<Object[]>> selects) throws DMLParserException {
        // this order must match the cartesian product or this wont work!!!
//        Object[] tableNameObjArr = selects.keySet().toArray();
//        String[] tableNameArr = new String[tableNameObjArr.length];
//        System.arraycopy(tableNameObjArr, 0, tableNameArr, 0, tableNameObjArr.length);
        String[] tableNameArr = tableOrder;

        orderByString = orderBySubString.replace("order by", "").trim(); // remove "order by"
        String[] orderByAttrs = orderByString.split(","); // retrieve all the attributes that will be ordered by

        ArrayList<Integer> orderedByIndexes = new ArrayList<>(); // the indexes the cartesian product will be ordered by

        int attrFoundCount = 0; // this is used for error checking double counting or missing attributes
        for(int i = 0;i < orderByAttrs.length;i++)    { // loop through all ordering attributes
            String orderedAttrName = orderByAttrs[i].trim();
            int cartesianProdIndex = 0;
            for(int j = 0;j < tableNameArr.length;j++)    { // loop through tables
                Table currTable = catalog.getTable(tableNameArr[j]);
                ArrayList<String> attrList = selectFromHash.get(currTable);

                for(int k = 0;k < attrList.size();k++)  { // loop through tables attributes
                    String currTableAttrName = attrList.get(k); // curr attr from the table

                    if(currTableAttrName.equals(orderedAttrName))   {
                        orderedByIndexes.add(cartesianProdIndex); // ordered attr matches this attr in this table
                        attrFoundCount++;
                    }
                    else if(orderedAttrName.contains("."))    {
                        String splitAttr[] = orderedAttrName.split("[\\s.\\s]"); // split by spaces and "."

                        if(splitAttr[0].equals(currTable.getName()) && currTableAttrName.equals(splitAttr[1]))    {
                            orderedByIndexes.add(cartesianProdIndex); // ordered attr matches this table and this attr
                            attrFoundCount++;
                        }
                    }
                    cartesianProdIndex++;
                }
            }
        }
        if(attrFoundCount != orderByAttrs.length) // check for missing attributes or double counted attributes
            throw new DMLParserException("Incorrect order by attributes");
        return orderedByIndexes;
    }

    /**
     * Uses bubble sort to sort tuples by the ordering indexes
     * @return sorted array of tuples
     * @throws DMLParserException
     */
    public Object[][] sortRelations(Object[][] relations, ArrayList<Integer> orderingIndexes) throws DMLParserException  {
        // https://www.geeksforgeeks.org/bubble-sort/
        Object[][] solution = relations;
        int n = solution.length;
        for(int i = 0;i < n - 1;i++)    {
            for(int j = 0;j < n - i - 1;j++)    {
                  if(compareTuple(solution[j], solution[j + 1], orderingIndexes) > 0)    {
                    // swap solutions[j+1] and solutions[i]
                    Object[] temp = solution[j];
                    solution[j] = solution[j + 1];
                    solution[j + 1] = temp;
                }
            }
        }
        return solution;
    }

    /**
     * This function gets the attribute names of the cartesian product result array
     * @param selects hash of the form: key -> tableName, value -> result of select
     * @return Array of attribute names in the form: tableName.attrName
     * @throws DMLParserException
     */
    public String[] getAttrNames(HashMap<String, ArrayList<Object[]>> selects) throws DMLParserException    {
        // this order must match the cartesian product or this wont work!!!
        // Object[] tableNameObjArr = selects.keySet().toArray();
//        String[] tableNameArr = new String[tableNameObjArr.length];
//        System.arraycopy(tableNameObjArr, 0, tableNameArr, 0, tableNameObjArr.length);
        String[] tableNameArr = tableOrder;

        ArrayList<String> cartesianAttrNames = new ArrayList<>();

        // loop through tables in cartesian product order, then loop through each tables selected attributes
        for(String tableName : tableNameArr)    {
            Table table = catalog.getTable(tableName);
            ArrayList<String> selectedAttrs = selectFromHash.get(table);
            for(String attrName : selectedAttrs)    {
                cartesianAttrNames.add(tableName + "." + attrName);
            }
        }

        Object[] tempResult = cartesianAttrNames.toArray();
        String[] result = new String[tempResult.length];
        System.arraycopy(tempResult, 0, result, 0, tempResult.length);
        return result;
    }

    /**
     * This function compares tuples based on specified comparison indices,
     * @param t1 tuple one
     * @param t2 tuple 2
     * @param orderingIndexes List of indices you are comparing in ascending order
     * @return if t1 > t2, return 1
     *          if t1 < t2, return -1
     *          if t1 == t2, return 0
     *          return -2 if error
     * @throws DMLParserException
     */
    private int compareTuple(Object[] t1, Object[] t2, ArrayList<Integer> orderingIndexes) throws DMLParserException    {
        for(Integer index : orderingIndexes)    {
            int result = compareObjects(t1[index], t2[index]);
            if(result == -2)    {
                throw new DMLParserException("Order by comparison error.");
            }
            else if(result != 0)    {
                return result; // a difference was found
            }
        }
        return 0; // no differences found
    }

    /**
     * Compares two attributes based on given list of attributes to compare
     * @return
     */
    private int compareAttributes(ArrayList<String> compAttrs, Object[] record, Object[] secondRecord, String[] cartAttr) {
        String compAttr = compAttrs.remove(0);
        int attrIndex = 0;
        int comparison;

        // find index of attr in record
        for (int i = 0; i < cartAttr.length; i++) {
            if (cartAttr[i] == compAttr) {
                attrIndex = i;
            }
        }

        comparison = compareObjects(record[attrIndex], secondRecord[attrIndex]);

        if (comparison == 0 && compAttrs.size() != 0) {
            comparison = compareAttributes(compAttrs, record, secondRecord, cartAttr);
        } else {
            return comparison;
        }
        return comparison;
    }

    /**
     * This function compares two indices.
     * @param val1 an object to be compared
     * @param val2 an object to be compared
     * @precondition both val1 and val2 must be same type
     * @return if val1 > val2, return 1
     *          if val1 < val2, return -1
     *          if val1 == val2, return 0
     *          return -2 if error
     */
    private int compareObjects(Object val1, Object val2)    {
        if(val1 == null || val2 == null)    {
            if(val1 != null && val2 == null)
                return 1;
            else if(val1 == null && val2 != null)
                return -1;
            else
                return 0; // they are both null
        }
        // compare Integer
        if(val1 instanceof Integer) {
            if((Integer)val1 > (Integer)val2)
                return 1;
            else if((Integer)val1 < (Integer)val2)
                return -1;
            else
                return 0;
        }
        // compare Doubles
        if(val1 instanceof Double) {
            if((Double)val1 > (Double) val2)
                return 1;
            else if((Double)val1 < (Double) val2)
                return -1;
            else
                return 0;
        }
        // compare Booleans
        if(val1 instanceof Boolean) {
            if((Boolean)val1 == (Boolean)val2)
                return 0;
            else
                return 1;
        }
        // compare Strings
        if(val1 instanceof String) {
            if(val1.toString().compareTo(val2.toString()) > 0)
                return 1;
            else if(val1.toString().compareTo(val2.toString()) < 0)
                return -1;
            else
                return 0;
        }
        return -2;
    }
}
