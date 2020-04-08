package testers;

import database.Database;
import database.IDatabase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class phase4Tester {

    public static void main(String[] args) {


        // You may need to modify some values to test on your system
        String dbLoc = "C:\\Users\\Matthew\\Desktop\\DB\\data";

        int pageBufferSize = 20;
        int pageSize = 4096;

        IDatabase database = Database.getConnection(dbLoc, pageBufferSize, pageSize);

        System.out.println("Adding tables to database...");

        // make a few tables
        // assumes table 1 in storage manager... change if needed
        int table1Id = 1;
        String createTable1 = "create table dataone( " +
                "id integer primarykey, " +
                "name varchar(40), " +
                "amount double, " +
                "married boolean );";

        // assumes table 2 in storage manager
        int table2Id = 2;
        String createTable2 = "CREATE TABLE datatwo( " +
                "id INTEGER PRIMARYKEY, " +
                "name varchar(60));";

        database.executeNonQuery(createTable1);
        database.executeNonQuery(createTable2);

        // Populate tables
        // Table 1
        Map<Integer, Object[]> data1Values = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader("inputs/data1.csv"));

            // Reading in lines and shuffling them
            ArrayList<String> lines = new ArrayList<>();
            String line;
            while((line = br.readLine()) != null) {
                lines.add(line);
            }

            System.out.println("Inserting into Table 1: this may take a moment");

            // Populating the table to table
            for(String line2: lines){
                String[] elems = line2.split(",");
                String insert = String.format("insert into dataone values (%s \"%s\" %s %s);",
                        elems[0], elems[1], elems[2], elems[3]);
                database.executeNonQuery(insert);
                data1Values.put(Integer.parseInt(elems[0]),
                        new Object[] {
                                Integer.parseInt(elems[0]),
                                elems[1],
                                Double.parseDouble(elems[2]),
                                Boolean.valueOf(elems[3])});
            }
        } catch(Exception e) {
            System.err.println("Failed to insert record.");
            e.printStackTrace();
        }

        ArrayList<Object[]> sortedAmount = new ArrayList<>(data1Values.values());

        sortedAmount.sort(new Comparator<Object[]>() {
            public int compare(Object[] o1, Object[] o2) {
                return ((Double)o1[2]).compareTo((Double) o2[2]);
            }
        });

        // Table 2
        Map<Integer, Object[]> data2Values = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader("inputs/data2.csv"));

            // Reading in lines and shuffling them
            ArrayList<String> lines = new ArrayList<>();
            String line;
            while((line = br.readLine()) != null) {
                lines.add(line);
            }

            System.out.println("Inserting into Table 2: this may take a moment");

            // Populating the table to table
            for(String line2: lines){
                String[] elems = line2.split(",");
                String insert = String.format("insert into datatwo values (%s \"%s\");",
                        elems[0], elems[1]);
                database.executeNonQuery(insert);
                data2Values.put(Integer.parseInt(elems[0]),
                        new Object[] {
                                Integer.parseInt(elems[0]),
                                elems[1]});
            }
        } catch(Exception e) {
            System.err.println("Failed to insert record.");
            e.printStackTrace();
        }

        Map<String, Object[]> merged = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader("inputs/data3.csv"));

            // Reading in lines and shuffling them
            ArrayList<String> lines = new ArrayList<>();
            String line;
            while((line = br.readLine()) != null) {
                lines.add(line);
            }

            // Populating the table to table
            for(String line2: lines){
                String[] elems = line2.split(",");
                merged.put(elems[0] + "," + elems[4],
                        new Object[] {
                                Integer.parseInt(elems[0]),
                                elems[1],
                                Double.parseDouble(elems[2]),
                                Boolean.valueOf(elems[3]),
                                Integer.parseInt(elems[4]),
                                elems[5]});
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        String testSelectStarOneTable = "select * from dataone;";

        System.out.println("Testing: " + testSelectStarOneTable);

        Object[][] result = database.executeQuery(testSelectStarOneTable);

        if(result.length != data1Values.size()){
            System.err.println("Failed: " + testSelectStarOneTable);
            System.err.println("Expected size 1000, got " + result.length);
            System.exit(1);
        }

        Map<Integer, Object[]> copyData1Values = new HashMap<>(data1Values);

        for(Object[] o: result){
            Integer i = (Integer)o[0];

            if(o.length != 4){
                System.err.println("Invalid length of entry with key: " + i);
                System.exit(1);
            }
            if(!copyData1Values.containsKey(i)){
                System.err.println("Invalid entry with key: " + i);
                System.exit(1);
            }

            if(!Arrays.equals(o, data1Values.get(i))){
                System.err.println("Invalid values for key: " + i);
                System.exit(1);
            }

            copyData1Values.remove(i);
        }

        String testSelectStarOneTableOrder = "select * from dataone order by amount;";

        System.out.println("Testing: " + testSelectStarOneTableOrder);

        result = database.executeQuery(testSelectStarOneTableOrder);

        if(result.length != data1Values.size()){
            System.err.println("Failed: " + testSelectStarOneTableOrder);
            System.err.println("Expected size 1000, got " + result.length);
            System.exit(1);
        }

        int counter = 0;

        for(Object[] o: result){
            Integer i = (Integer)o[0];

            if(o.length != 4){
                System.err.println("Invalid length of entry with key: " + i);
                System.exit(1);
            }

            if(!Arrays.equals(o, sortedAmount.get(counter++))){
                System.out.println(sortedAmount.get(counter-1)[2]);
                System.out.println(o[2]);
                System.err.println("Invalid values for key: " + i);
                System.exit(1);
            }

            copyData1Values.remove(i);
        }

//        String testSelectSingleAttrOneTable = "select id from dataone;";
//
//        System.out.println("Testing: " + testSelectSingleAttrOneTable);
//
//        result = database.executeQuery(testSelectSingleAttrOneTable);
//
//        if(result.length != data1Values.size()){
//            System.err.println("Failed: " + testSelectSingleAttrOneTable);
//            System.err.println("Expected size " + data1Values.size() + ", got " + result.length);
//            System.exit(1);
//        }
//
//        copyData1Values = new HashMap<>(data1Values);
//
//        for(Object[] o: result){
//            Integer i = (Integer)o[0];
//
//            if(o.length != 1){
//                System.err.println("Invalid length of entry with key: " + i);
//                System.exit(1);
//            }
//            if(!copyData1Values.containsKey(i)){
//                System.err.println("Invalid entry with key: " + i);
//                System.exit(1);
//            }
//
//            Object[] expected = new Object[] {data1Values.get(i)[0]};
//
//            if(!Arrays.equals(o, expected)){
//                System.err.println("Invalid values for key: " + i);
//                System.exit(1);
//            }
//
//            copyData1Values.remove(i);
//        }
//
//        String testSelectMultipleAttrOneTable = "select id, amount from dataone;";
//
//        System.out.println("Testing: " + testSelectMultipleAttrOneTable);
//
//        result = database.executeQuery(testSelectMultipleAttrOneTable);
//
//        if(result.length != data1Values.size()){
//            System.err.println("Failed: " + testSelectMultipleAttrOneTable);
//            System.err.println("Expected size "+ data1Values.size() + ", got " + result.length);
//            System.exit(1);
//        }
//
//        copyData1Values = new HashMap<>(data1Values);
//
//        for(Object[] o: result){
//            Integer i = (Integer)o[0];
//
//            if(o.length != 2){
//                System.err.println("Invalid length of entry with key: " + i);
//                System.exit(1);
//            }
//            if(!copyData1Values.containsKey(i)){
//                System.err.println("Invalid entry with key: " + i);
//                System.exit(1);
//            }
//
//            Object[] expected = new Object[] {data1Values.get(i)[0], data1Values.get(i)[2]};
//
//            if(!Arrays.equals(o, expected)){
//                System.err.println("Invalid values for key: " + i);
//                System.exit(1);
//            }
//
//            copyData1Values.remove(i);
//        }
//
//        String testingJoin = "select * from dataone, datatwo;";
//
//        System.out.println("Testing: " + testingJoin);
//
//        result = database.executeQuery(testingJoin);
//
//        if(result.length != merged.size()){
//            System.err.println("Failed: " + testingJoin);
//            System.err.println("Expected size " + merged.size() + ", got " + result.length);
//            System.exit(1);
//        }
//
//        Map<String, Object[]> mergedCopy = new HashMap<>(merged);
//
//        for(Object[] o: result){
//            String i = o[0] + "," + o[4];
//
//            if(o.length != 6){
//                System.err.println("Invalid length of entry with key: " + i);
//                System.exit(1);
//            }
//            if(!merged.containsKey(i)){
//                System.err.println("Invalid entry with key: " + i);
//                System.exit(1);
//            }
//
//            if(!Arrays.equals(o, mergedCopy.get(i))){
//                System.err.println("Invalid values for key: " + i);
//                System.exit(1);
//            }
//
//            mergedCopy.remove(i);
//        }
//
//        String testingJoinAttr = "select dataone.id, datatwo.id from dataone, datatwo;";
//
//        System.out.println("Testing: " + testingJoinAttr);
//
//        result = database.executeQuery(testingJoinAttr);
//
//        if(result.length != merged.size()){
//            System.err.println("Failed: " + testingJoinAttr);
//            System.err.println("Expected size " + merged.size() + ", got " + result.length);
//            System.exit(1);
//        }
//
//        mergedCopy = new HashMap<>(merged);
//
//        for(Object[] o: result){
//            String i = o[0] + "," + o[1];
//
//            if(o.length != 2){
//                System.err.println("Invalid length of entry with key: " + i);
//                System.exit(1);
//            }
//            if(!merged.containsKey(i)){
//                System.err.println("Invalid entry with key: " + i);
//                System.exit(1);
//            }
//
//            Object[] expected = new Object[]{
//                    mergedCopy.get(i)[0],
//                    mergedCopy.get(i)[4]
//            };
//            if(!Arrays.equals(o, expected)){
//                System.err.println("Invalid values for key: " + i);
//                System.exit(1);
//            }
//
//            mergedCopy.remove(i);
//        }
//
//        String testingJoinWhere = "select * from dataone, datatwo where dataone.id = datatwo.id;";
//
//        System.out.println("Testing: " + testingJoinWhere);
//
//        result = database.executeQuery(testingJoinWhere);
//
//        if(result.length != 49){
//            System.err.println("Failed: " + testingJoinWhere);
//            System.err.println("Expected size " + 49 + ", got " + result.length);
//            System.exit(1);
//        }
//
//        mergedCopy = new HashMap<>(merged);
//
//        for(Object[] o: result){
//            String i = o[0] + "," + o[4];
//
//            if(o.length != 6){
//                System.err.println("Invalid length of entry with key: " + i);
//                System.exit(1);
//            }
//            if(!merged.containsKey(i)){
//                System.err.println("Invalid entry with key: " + i);
//                System.exit(1);
//            }
//
//            if(!o[0].equals(mergedCopy.get(i)[0]) || !o[4].equals(mergedCopy.get(i)[4])){
//                System.err.println("Invalid values joined for key: " + i);
//                System.exit(1);
//            }
//
//            if(!Arrays.equals(o, mergedCopy.get(i))){
//                System.err.println("Invalid values for key: " + i);
//                System.exit(1);
//            }
//
//            mergedCopy.remove(i);
//        }
//
//        System.out.println("Testing complete...");
    }
}
