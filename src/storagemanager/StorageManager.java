package storagemanager;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class StorageManager extends AStorageManager {

    private String dbLoc;
    private int pageSize;
    private int pageNum;

    private Map<Integer, String[]> tableDataTypes;
    private Map<Integer, Integer[]> tableKeyIndices;
    private Map<Integer, Integer> tableEntriesPerPage;

    private Map<Integer, ArrayList<Integer>> pages;

    private Map<String,Object[][]> pageBuffer;
    private ArrayList<String> pageBufferOrder;
    private int pageBufferSize;

    /**
     * Creates an instance of the database. Tries to restart, if requested, the database at the provided location.
     *
     * You can add code to this but cannot change the types and number of parameters. testers will be called using
     * this constructor.
     *
     * @param dbLoc the location to start/restart the database in
     * @param pageBufferSize the size of the page buffer; max number of pages allowed in the buffer at any given time
     * @param pageSize the size of a page in bytes
     * @param restart restart the database in the location if true; start a new database otherwise
     * @throws StorageManagerException database fails to restart or start
     */
    public StorageManager(String dbLoc, int pageBufferSize, int pageSize, boolean restart) throws StorageManagerException {
        super(dbLoc, pageBufferSize, pageSize, restart);
    }

    @Override
    public Object[][] getRecords(int table) throws StorageManagerException {
        if(!this.pages.containsKey(table))
            throw new StorageManagerException("Table does not exist: " + table);

        ArrayList<Object[]> temp = new ArrayList<>();

        ArrayList<Integer> p = new ArrayList<>(pages.get(table));
        for(Integer s: p){
            if(!this.pages.get(table).contains(s)){
                continue;
            }
            for(Object[] record: readPage(table, s)){
                if(record != null){
                    temp.add(record);
                }
            }
        }

        Object[][] data = new Object[temp.size()][];
        int counter = 0;
        for(Object[] o: temp){
            data[counter] = o;
            counter++;
        }
        return data;
    }

    @Override
    public Object[] getRecord(int table, Object[] keyValues) throws StorageManagerException {
        if(!this.pages.containsKey(table))
            throw new StorageManagerException("Table does not exist: " + table);

        ArrayList<Integer> p = new ArrayList<>(pages.get(table));
        for(Integer s: p){
            for(Object[] record: readPage(table, s)){
                if(record != null) {
                    int compareKey = compareKey(table, record, keyValues);
                    if (compareKey == 0) {
                        return record;
                    } else if (compareKey > 0) {
                        return null;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void insertRecord(int table, Object[] record) throws StorageManagerException {
        if(!this.pages.containsKey(table))
            throw new StorageManagerException("Table does not exist: " + table);

        ArrayList<Integer> p = new ArrayList<>(pages.get(table));
        for(int j = 0; j < p.size(); j++){
            Integer s = p.get(j);
            Object[][] page = readPage(table, s);
            for(int i = 0; i < page.length; i++ ){
                Object[] r = page[i];
                if(r == null){
                    if(j == p.size()-1){
                        insertIntoPage(table, s, record, i);
                        return;
                    }

                    Object[][] nextPage = readPage(table, p.get(j+1));
                    if(compareRecords(table, record, nextPage[0]) < 0){
                        insertIntoPage(table, s, record, i);
                        return;
                    }
                    break;
                }
                int compareKey = compareRecords(table, record, r);
                if(compareKey == 0){
                    throw new StorageManagerException("Record already exists.");
                }
                else if(compareKey < 0){
                    insertIntoPage(table, s, record, i);
                    return;
                }
            }
        }

        Object[][] newPage = new Object[this.tableEntriesPerPage.get(table)][];
        for(int i = 0; i < this.tableEntriesPerPage.get(table); i++)
            newPage[i] = null;
        newPage[0] = record;
        writeNewPage(table, newPage, this.pages.get(table).size());
    }

    @Override
    public void updateRecord(int table, Object[] record) throws StorageManagerException {
        if(!this.pages.containsKey(table))
            throw new StorageManagerException("Table does not exist: " + table);

        ArrayList<Integer> p = new ArrayList<>(pages.get(table));
        for(Integer s: p){
            int i = 0;
            Object[][] page = readPage(table, s);
            for(Object[] r: page){
                if(r == null)
                    continue;
                int compareKey = compareRecords(table, r, record);
                if(compareKey == 0){
                    page[i] = record;
                    return;
                }
                else if(compareKey > 0){
                    throw new StorageManagerException("Record does not exist.");
                }
                i++;
            }
        }
        throw new StorageManagerException("Record does not exist.");
    }

    @Override
    public void removeRecord(int table, Object[] keyValue) throws StorageManagerException {
        if(!this.pages.containsKey(table))
            throw new StorageManagerException("Table does not exist: " + table);

        ArrayList<Integer> p = new ArrayList<>(pages.get(table));
        for(Integer s: p){
            int i = 0;
            Object[][] page = readPage(table, s);
            for(Object[] r: page){
                if(r == null)
                    continue;
                int compareKey = compareKey(table, r, keyValue);
                if(compareKey == 0){
                    removeFromPage(table, s, i);
                    return;
                }
                else if(compareKey > 0){
                    throw new StorageManagerException("Record does not exist.");
                }
                i++;
            }
        }
    }

    @Override
    public void dropTable(int table) throws StorageManagerException {
        if(!this.pages.containsKey(table))
            throw new StorageManagerException("No such table: " + table);
        clearTable(table);

        this.pages.remove(table);
        this.tableEntriesPerPage.remove(table);
        this.tableKeyIndices.remove(table);
        this.tableDataTypes.remove(table);
    }

    @Override
    public void clearTable(int table) throws StorageManagerException {
        for(Integer p: this.pages.get(table)){
            File f = new File(this.dbLoc + p);
            if(!f.delete()){
                throw new StorageManagerException("Clearing table failed: " + table);
            }

            if(this.pageBuffer.containsKey(table+" "+p)){
                this.pageBuffer.remove(table+" "+p);
                this.pageBufferOrder.remove(table+" "+p);
            }
        }

        this.pages.get(table).clear();
    }

    @Override
    public void addTable(int table, String[] dataTypes, Integer[] keyIndices) throws StorageManagerException {
        if(this.pages.containsKey(table))
            throw new StorageManagerException("Table already exists: " + table);

        int maxRecordSize = 0;

        for(String s: dataTypes){
            if(s.equals("integer")){
                maxRecordSize += 32;
            }
            else if (s.equals("double")){
                maxRecordSize += 64;
            }
            else if (s.equals("boolean")){
                maxRecordSize += 4;
            }
            else if(s.startsWith("char")){
                int count = Integer.parseInt(s.substring(5, s.length()-1));
                maxRecordSize += count * 16;
            }
            else if(s.startsWith("varchar")){
                int count = Integer.parseInt(s.substring(8, s.length()-1));
                maxRecordSize += count * 16;
            }
        }

        this.pages.put(table, new ArrayList<>());
        this.tableDataTypes.put(table, dataTypes);
        this.tableKeyIndices.put(table, keyIndices);
        this.tableEntriesPerPage.put(table, this.pageSize/(maxRecordSize/8));
    }

    @Override
    public void purgeBuffer() throws StorageManagerException {
        for(String page: this.pageBufferOrder){
            String[] p = page.split(" ");
            writePage(Integer.parseInt(p[0]), Integer.parseInt(p[1]));
        }

        this.pageBufferOrder.clear();
        this.pageBuffer.clear();
    }

    @Override
    public void terminateDatabase() throws StorageManagerException {
        purgeBuffer();
        FileOutputStream f;
        try {
            f = new FileOutputStream(new File(this.dbLoc + "sm.dat"));
            ObjectOutputStream o = new ObjectOutputStream(f);

            o.writeObject(this.pageSize);
            o.writeObject(this.pageNum);

            o.writeObject(this.tableDataTypes);
            o.writeObject(this.tableKeyIndices);
            o.writeObject(this.tableEntriesPerPage);

            o.writeObject(this.pages);
            o.writeObject(this.pageBufferSize);
        } catch (Exception e) {
            throw new StorageManagerException("Failure in terminating database: " + e.getMessage());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void restartDatabase(String dbLoc) throws StorageManagerException {
        this.dbLoc = dbLoc;

        File directory = new File(this.dbLoc + "sm.dat");
        if(directory.exists()){
            try {
                FileInputStream fi = new FileInputStream(directory);
                ObjectInputStream oi = new ObjectInputStream(fi);

                this.pageSize = (Integer)oi.readObject();
                this.pageNum = (Integer)oi.readObject();

                this.tableDataTypes = (Map<Integer, String[]>)oi.readObject();
                this.tableKeyIndices = (Map<Integer, Integer[]>)oi.readObject();
                this.tableEntriesPerPage = (Map<Integer, Integer>)oi.readObject();

                this.pages = (Map<Integer, ArrayList<Integer>>)oi.readObject();

                this.pageBuffer = new HashMap<>();
                this.pageBufferOrder = new ArrayList<>();
                this.pageBufferSize = (Integer)oi.readObject();

                oi.close();

            } catch (Exception e) {
                throw new StorageManagerException("Failure to restart database: " + e.getMessage());
            }
        }
    }

    @Override
    protected void newDatabase(String dbLoc, int pageBufferSize, int pageSize){
        this.dbLoc = dbLoc;
        this.pageSize = pageSize;
        this.pageNum = 0;

        this.tableDataTypes = new HashMap<>();
        this.tableKeyIndices = new HashMap<>();
        this.tableEntriesPerPage = new HashMap<>();

        this.pages = new HashMap<>();

        this.pageBuffer = new HashMap<>();
        this.pageBufferOrder = new ArrayList<>();
        this.pageBufferSize = pageBufferSize;
    }

    private Object[][] readPage(int table, int pageName) throws StorageManagerException{

        if(this.pageBuffer.containsKey(table+" "+pageName)){
            return this.pageBuffer.get(table+" "+pageName);
        }
        Object[][] page;
        try {
            FileInputStream fi = new FileInputStream(new File(this.dbLoc + pageName));
            ObjectInputStream oi = new ObjectInputStream(fi);

            page = (Object[][])oi.readObject();
            oi.close();
        } catch (IOException | ClassNotFoundException e) {
            throw new StorageManagerException("Error reading page");
        }

        if(this.pageBufferOrder.contains(table+" "+pageName)){
            this.pageBufferOrder.remove(table+" "+pageName);
            this.pageBufferOrder.add(table+" "+pageName);
        }
        else if (this.pageBufferOrder.size() < this.pageBufferSize){
            this.pageBuffer.put(table+" "+pageName, page);
            this.pageBufferOrder.add(table+" "+pageName);
        }
        else{
            String[] elems = this.pageBufferOrder.remove(0).split(" ");
            writePage(Integer.parseInt(elems[0]), Integer.parseInt(elems[1]));
            this.pageBuffer.put(table+" "+pageName, page);
            this.pageBufferOrder.add(table+" "+pageName);
        }

        return page;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void writePage(int table, Integer pageName) throws StorageManagerException{
        try {
            Object[][] data = this.pageBuffer.remove(table+" "+pageName);
            if(isEmptyPage(data)){
                File f = new File(this.dbLoc + pageName);
                f.delete();
                this.pages.get(table).remove(pageName);
                return;
            }
            FileOutputStream f = new FileOutputStream(new File(this.dbLoc + pageName));
            ObjectOutputStream o = new ObjectOutputStream(f);

            o.writeObject(data);
            o.close();
        } catch (IOException e) {
            throw new StorageManagerException("Failure to write page.");
        }
    }

    private void writeNewPage(int table, Object[][] page, int index) throws StorageManagerException{
        try {
            Integer pageName = this.pageNum;
            this.pageNum++;

            FileOutputStream f = new FileOutputStream(new File(this.dbLoc + pageName));
            ObjectOutputStream o = new ObjectOutputStream(f);

            o.writeObject(page);
            o.close();

            this.pages.get(table).add(index,pageName);
        } catch (IOException e) {
            throw new StorageManagerException("Failure to write page.");
        }
    }

    private boolean isEmptyPage(Object[][] data) {
        if (data == null)
            return true;
        for (Object[] d : data) {
            if (d != null) {
                return false;
            }
        }
        return true;
    }

    private int compareKey(int table, Object[] record, Object[] keyValues){
        Integer[] keyIndices = this.tableKeyIndices.get(table);
        String[] keyTypes = this.tableDataTypes.get(table);
        int i = 0;

        for(Integer index: keyIndices){
            String type = keyTypes[index];
            if(type.equals("integer")){
                Integer v1 = (Integer)record[index];
                Integer v2 = (Integer)keyValues[i];
                int c = v1.compareTo(v2);
                if(c != 0)
                    return c;
            }
            else if(type.equals("double")){
                Double v1 = (Double)record[index];
                Double v2 = (Double)keyValues[i];
                int c = v1.compareTo(v2);
                if(c != 0)
                    return c;
            }
            else if(type.equals("boolean")){
                Boolean v1 = (Boolean)record[index];
                Boolean v2 = (Boolean)keyValues[i];
                int c = v1.compareTo(v2);
                if(c != 0)
                    return c;
            }
            else if(type.startsWith("char") || type.startsWith("varchar")){
                String v1 = (String)record[index];
                String v2 = (String)keyValues[i];
                int c = v1.compareTo(v2);
                if(c != 0)
                    return c;
            }

            i++;
        }

        return 0;
    }

    private int compareRecords(int table, Object[] r1, Object[] r2){
        Integer[] keyIndices = this.tableKeyIndices.get(table);
        Object[] keyValues = new Object[keyIndices.length];

        for(int i = 0; i < keyIndices.length; i++){
            keyValues[i] = r2[keyIndices[i]];
        }

        return compareKey(table, r1, keyValues);
    }

    @SuppressWarnings("ManualArrayCopy")
    private void insertIntoPage(int table, Integer p, Object[] record, int index) throws StorageManagerException{
        Object[][] page = readPage(table, p);

        if(page[page.length - 1] != null){
            int newIndex = this.pages.get(table).indexOf(p) + 1;
            Object[][] newPage = new Object[page.length][];
            ArrayList<Object[]> data = new ArrayList<>();
            int curr = 0;
            for(Object[] o: page){
                if(curr == index){
                    data.add(record);
                }
                curr++;
                data.add(o);
            }

            for(int j=0; j < page.length; j++) {
                newPage[j] = null;
                page[j] = null;
            }
            int half = data.size()/2;

            int n1 = 0;
            int o1 = 0;
            for(Object[] o: data){
                if(half < 0){
                    newPage[n1] = o;
                    n1++;
                }
                else{
                    page[o1] = o;
                    o1++;
                }
                half--;
            }

            writeNewPage(table, newPage, newIndex);
            return;
        }

        for(int j = page.length - 2; j >= index; j--){
            page[j+1] = page[j];
        }
        page[index] = record;
    }

    @SuppressWarnings("ManualArrayCopy")
    private void removeFromPage(int table, Integer p, int index) throws StorageManagerException{
        Object[][] page = readPage(table, p);

        for(int j = index; j < page.length - 1; j++){
            page[j] = page[j+1];
        }
        page[page.length - 1] = null;

        if(isEmptyPage(page)){
            this.pages.get(table).remove(p);
            this.pageBuffer.remove(table+" "+p);
            this.pageBufferOrder.remove(table+" "+p);
        }
    }

    private void printRecord(Object[] record){
        for(Object o: record){
            System.out.print(o + " ");
        }
        System.out.println();
    }
}
