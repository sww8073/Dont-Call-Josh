package dml;

import database.Catalog;
import ddl.Table;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.HashMap;

public class Select {

    private static Catalog catalog;
    private static StorageManager storageManager;
    private String selectSubString;
    private String fromSubString;
    private String whereSubString;
    private String orderBySubString;

    private HashMap<String, ArrayList<String>> selectFromHash;


    /**
     * Constructor.
     */
    public Select(Catalog catalog, StorageManager storageManager, String selectString) throws DMLParserException    {
        this.catalog = catalog;
        this.storageManager = storageManager;

        parseQuery(selectString); // call helper function to parse select statement
    }

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
    }

    /**
     * This function parses the "select" and "from" part pf the query.
     * @param selectString "select attribute1, attribute2"
     * @param fromString "from table1, table2"
     * @return HashMap key table - table name, value - ArrayList of attributes
     * @throws DMLParserException
     */
    private HashMap parseSelectAndFrom(String selectString, String fromString) throws DMLParserException  {
        HashMap<String, ArrayList<String>> tables = new HashMap<>(); // key - tableName, value - ArrayList of attributes

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
            }
            tables.put(currTableName, currAttrList);
        }

        if(attrFoundCount != attrNames.length) // all the attribute in the attribute list were not found
            throw new DMLParserException("Invalid Attributes");
        return tables;
    }
}
