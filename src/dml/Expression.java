package dml;
import database.Catalog;
import ddl.Attribute;
import ddl.Table;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Stack;

/**
 * This class uses stacks to evaluate a where expression.
 */
public class Expression {
    Stack<Object> stack;
    Catalog catalog;
    StorageManager storageManager;
    Table table;

    public Expression(Catalog catalog, StorageManager storageManager, String exprStr, Table table) {
        this.catalog = catalog;
        this.storageManager = storageManager;
        this.stack = new Stack<>();
        this.table = table;

        exprStr = exprStr.toLowerCase().replace("where", "").trim();
        exprStr = exprStr.replace(";","");

        // split by "and", if there are not ands no split will happen and a single string will be in the array
        String splitAndArr[] = exprStr.split("\\s+and\\s+");

        // Todo calculate a single vale with no ands or ors

        for(int i = 0;i < splitAndArr.length;i += 2)  {
            if(i + 1 >= splitAndArr.length)  { // there is 1, x == y left, that is separated by a "and"
                parseAndPushAnd(splitAndArr[i]);
                stack.push("and");
            }
            else { // there are 2 or more, x == y left, that is separated by a "and"
                parseAndPushAnd(splitAndArr[i]);
                parseAndPushAnd(splitAndArr[i + 1]);
                stack.push("and");
            }
        }
        int l = 0;
        l++;
    }

    /**
     * This function evaluates the stack in prefix format
     * @return
     */
    public ArrayList<Objects[]> evaluate()  {
        Object top = stack.pop();
        Object onDeck = stack.pop();
        if(onDeck.equals("and") || onDeck.equals("or"))   { // must be an and or or
            stack.push(onDeck);
            evaluate();
        }
        else    {
            System.out.print(onDeck + " ");
            System.out.print(top + " ");
        }

        Object inTheHole = stack.pop();
        if(inTheHole.equals("and") || inTheHole.equals("or"))   { // must be an and or or
            stack.push(inTheHole);
            evaluate();
        }
        else    {
            System.out.print(onDeck + " ");
        }



        return null;
    }

    /**
     * This function parses strings that have been split by "and". If string contains "or" then
     * string is parsed and pushed onto stack. If no "or" in string the expression is pushed
     * directly onto the stack
     * @param andExpr string containing no "and".
     */
    private void parseAndPushAnd(String andExpr)   {
        if(andExpr.contains("or"))  { // contains an "or"
            parseAndPushOr(andExpr);
        }
        else { // does not contain an "or:
            stack.push(andExpr); // this must have no more conditional statements
        }
    }

    /**
     * This function parses "or" strings, and pushes them onto the stack
     * @param orExpr string containing at least one or
     */
    private void parseAndPushOr(String orExpr)    {
        String splitOrArr[] = orExpr.split("\\s+or\\s+");
        for (int j = 0; j < splitOrArr.length; j += 2) {
            if(j + 1 >= splitOrArr.length)  { // there is 1, x == y left
                stack.push(splitOrArr[j]);
                stack.push("or");
            }
            else { // there are 2 or more, x == y left
                stack.push(splitOrArr[j]);
                stack.push(splitOrArr[j + 1]);
                stack.push("or");
            }
        }
    }

    private ArrayList<Object[]> converteEquiv()  {

        return null;
    }

    /**
     * This function computes a where clause. Consider where foo >= 1000; The following variables would be...
     * @param attrName "foo"
     * @param equivalency ">="
     * @param var parsed object representing 1000
     * @return ArrayList that follows condition in where
     */
    private ArrayList<Object[]> computeEquiv(String attrName, String equivalency, Object var, String tableName)
            throws DMLParserException   {
        Table table = catalog.getTable(tableName);
        int indexOfAttr = getIndexFromTable(tableName, attrName);
        Object relations[][];
        try {
            relations = storageManager.getRecords(table.getId());
        }
        catch (StorageManagerException e) { throw new DMLParserException(e.getLocalizedMessage()); }

        ArrayList<Object[]> result = new ArrayList<>(0);

        equivalency = equivalency.trim();
        switch (equivalency)    {
            case "=":
                for(int i = 0;i < relations.length;i++) {
                    if(compare(relations[i][indexOfAttr], var) == 0)
                        result.add(relations[i]);
                }
                break;
            case ">":
                for(int i = 0;i < relations.length;i++) {
                    if(compare(relations[i][indexOfAttr], var) > 0)
                        result.add(relations[i]);
                }
                break;
            case "<":
                for(int i = 0;i < relations.length;i++) {
                    if(compare(relations[i][indexOfAttr], var) < 0)
                        result.add(relations[i]);
                }
                break;
            case ">=":
                for(int i = 0;i < relations.length;i++) {
                    if(compare(relations[i][indexOfAttr], var) >= 0)
                        result.add(relations[i]);
                }
                break;
            case "<=":
                for(int i = 0;i < relations.length;i++) {
                    if(compare(relations[i][indexOfAttr], var) <= 0)
                        result.add(relations[i]);
                }
                break;
            default:
                throw new DMLParserException("cannot compare by " + equivalency);

        }
        return null;
    }

    /**
     * This function compares two objects by trying to parse them to the same type
     * @param o1
     * @param o2
     * @return negative num if o1 < o2, 0 if o1 == o2, and positive num if o1 > o2.
     * @throws DMLParserException
     */
    private int compare(Object o1, Object o2) throws DMLParserException   {
        if(o1 instanceof String && o2 instanceof String)    {
            return ((String) o1).compareTo((String)o2);
        }
        else if(o1 instanceof Double && o2 instanceof Double)    {
            return ((Double) o1).compareTo((Double)o2);
        }
        else if(o1 instanceof Integer && o2 instanceof Integer)    {
            return ((Integer) o1).compareTo((Integer)o2);
        }
        else if(o1 instanceof Boolean && o2 instanceof Boolean)    {
            return ((Boolean) o1).compareTo((Boolean)o2);
        }
        else if(o1 == null && o2 == null)
            return 0;
        else
            throw new DMLParserException("Cannot compare invalid types.");
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
}
