//package dml;
//
//import database.Catalog;
//import database.Database;
//import database.IDatabase;
//import storagemanager.AStorageManager;
//import storagemanager.StorageManager;
//
//public class DMLTester {
//    public static void main(String[] args) {
//        String dbLoc = "C:\\Users\\Matthew\\Desktop\\db\\dba";
//        int pageBufferSize = 20;
//        int pageSize = 4096;
//        AStorageManager sm;
//
//        IDatabase database = Database.getConnection(dbLoc, pageBufferSize, pageSize);
//
//        DMLParser dmlParser = new DMLParser();
//
//        String insert1 = "insert into foo values (1 \"foo\" true 2.1);";
//        String insert2 = "insert into foo values (1 \"foo\" true 2.1)," +
//                "(2 \"baz\" true 4.14)," +
//                "(3 \"bar\" true 5.2);";
//
//        try {
//            dmlParser.parseDMLStatement(insert1);
//            dmlParser.parseDMLStatement(insert2);
//        }
//        catch (DMLParserException e)    {
//            System.err.println(e.getMessage());
//        }
//
//    }
//}

package dml;

import database.Database;
import database.IDatabase;
import dml.DMLParserException;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;

import java.io.File;
import java.util.Arrays;

public class DMLTester {

    private static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
    }

    public static void main(String[] args) {


        // You may need to modify some values to test on your system
        String dbLoc = "C:\\Users\\Matthew\\Desktop\\DB\\data";
        int pageBufferSize = 20;
        int pageSize = 4096;
        AStorageManager sm;

        IDatabase database = Database.getConnection(dbLoc, pageBufferSize, pageSize);

        System.out.println("Adding tables to database...");

        // make a few tables
        // assumes table 1 in storage manager... change if needed
        int table1Id = 1;
        String createTable1 = "create table foo( " +
                "id integer primarykey, " +
                "name varchar(40), " +
                "amount double, " +
                "married boolean );";

        // assumes table 2 in storage manager
        int table2Id = 2;
        String createTable2 = "CREATE TABLE baz( " +
                "id INTEGER PRIMARYKEY, " +
                "department varchar(60), " +
                "unique( department ) );";

        // assumes table 3 in storage manager
        int table3Id = 3;
        String createTable3 = "CREATE TABLE bar( " +
                "id integer primarykey, " +
                "department varchar(60) notnull, " +
                "age integer);";

        // assumes table 3 in storage manager
        int table4Id = 4;
        String createTable4 = "CREATE TABLE bazzle( " +
                "id integer primarykey, " +
                "department varchar(60), " +
                "foreignkey( id ) references foo( id ) );";

        database.executeNonQuery(createTable1);
        database.executeNonQuery(createTable2);
        database.executeNonQuery(createTable3);
        database.executeNonQuery(createTable4);

        //testing insert
        System.out.println("Testing insert....");

        Object[] expectedData1_0 = new Object[] {1, "baz", 5.8, false};
        Object[] expectedData1_1 = new Object[] {2, "foo", 3.12, true};
        Object[] expectedData1_2 = new Object[] {3, "bar", 35.8, false};

        Object[] expectedData2_0 = new Object[] {1, "baz"};
        Object[] expectedData2_1 = new Object[] {2, "foo"};
        Object[] expectedData2_2 = new Object[] {3, "bar"};

        Object[] expectedData3_0 = new Object[] {1, "baz", 24};
        Object[] expectedData3_1 = new Object[] {2, "foo", null};
        Object[] expectedData3_2 = new Object[] {3, "bar", 12};

        Object[] expectedData4_0 = new Object[] {1, "baz"};
        Object[] expectedData4_1 = new Object[] {2, "foo"};
        Object[] expectedData4_2 = new Object[] {3, "bar"};

        String insert1 = "insert into foo values (2 \"foo\" 3.12 true);";
        String insert1Multiple = "insert into foo values (3 \"bar\" 35.8 false), (1 \"baz\" 5.8 false);";
        String insert1DupKey = "insert into foo values (1 \"bazzle\" 4.12 true);";

        String insert2 = "insert into baz values (2 \"foo\");";
        String insert2Multiple = "insert into baz values (3 \"bar\"), (1 \"baz\");";
        String insert2DupKey = "insert into baz values (1 \"bazzle\");";
        String insert2DupUnique = "insert into baz values (4 \"baz\");";

        String insert3 = "insert into bar values (2 \"foo\" null);";
        String insert3Multiple = "insert into bar values (3 \"bar\" 12), (1 \"baz\" 24);";
        String insert3DupKey = "insert into bar values (1 \"bazzle\" 15);";
        String insert3Null = "insert into bar values (4 null 45);";

        String insert4 = "insert into bazzle values (2 \"foo\");";
        String insert4Multiple = "insert into bazzle values (3 \"bar\"), (1 \"baz\");";
        String insert4DupKey = "insert into bazzle values (1 \"bazzle\");";
        String insert4NonFK = "insert into bazzle values (4 \"baz\");";

        database.executeNonQuery(insert1);
        database.executeNonQuery(insert1Multiple);

        database.executeNonQuery(insert2);
        database.executeNonQuery(insert2Multiple);

        database.executeNonQuery(insert3);
        database.executeNonQuery(insert3Multiple);

        database.executeNonQuery(insert4);
        database.executeNonQuery(insert4Multiple);

        database.executeQuery("select foo.id, name, baz.id " +
                "from foo, baz where aaa order by bbb");
//
//        System.out.println("These inserts should report errors...");
//
//        database.executeNonQuery(insert1DupKey);
//
//        database.executeNonQuery(insert2DupKey);
//        database.executeNonQuery(insert2DupUnique);
//
//        database.executeNonQuery(insert3DupKey);
//        database.executeNonQuery(insert3Null);
//
//        database.executeNonQuery(insert4DupKey);
//        database.executeNonQuery(insert4NonFK);
//
//        database.terminateDatabase();
//
//        System.out.println("Restarting just storage manager...");
//        try {
//            sm = new StorageManager(dbLoc, pageBufferSize, pageSize, true);
//
//            System.out.println("Getting table 1 data...");
//            Object[][] data1 = sm.getRecords(table1Id);
//
//            if(data1.length != 3 || !Arrays.equals(data1[0], expectedData1_0) ||
//                    !Arrays.equals(data1[1], expectedData1_1) ||
//                    !Arrays.equals(data1[2], expectedData1_2)){
//                System.err.println("Inserting into Table 1 failed...");
//                System.exit(1);
//            }
//
//            System.out.println("Getting table 2 data...");
//            Object[][] data2 = sm.getRecords(table2Id);
//
//            if(data2.length != 3 || !Arrays.equals(data2[0], expectedData2_0) ||
//                    !Arrays.equals(data2[1], expectedData2_1) ||
//                    !Arrays.equals(data2[2], expectedData2_2)){
//                System.err.println("Inserting into Table 2 failed...");
//                System.exit(1);
//            }
//
//            System.out.println("Getting table 3 data...");
//            Object[][] data3 = sm.getRecords(table3Id);
//
//            if(data3.length != 3 || !Arrays.equals(data3[0], expectedData3_0) ||
//                    !Arrays.equals(data3[1], expectedData3_1) ||
//                    !Arrays.equals(data3[2], expectedData3_2)){
//                System.err.println("Inserting into Table 3 failed...");
//                System.exit(1);
//            }
//
//            System.out.println("Getting table 4 data...");
//            Object[][] data4 = sm.getRecords(table4Id);
//
//            if(data4.length != 3 || !Arrays.equals(data4[0], expectedData4_0) ||
//                    !Arrays.equals(data4[1], expectedData4_1) ||
//                    !Arrays.equals(data4[2], expectedData4_2)){
//                System.err.println("Inserting into Table 4 failed...");
//                System.exit(1);
//            }
//            sm.terminateDatabase();
//        } catch (StorageManagerException e){
//            e.printStackTrace();
//            System.exit(1);
//        }
//
//        System.out.println("Restarting the database...");
//
//        database = Database.getConnection(dbLoc, pageBufferSize, pageSize);
//
//        //testing delete
//        System.out.println("Testing delete.... none should fail...");
//
//        String deleteByPK = "delete from baz where id = 2";
//        String deleteByPKNonExist = "delete from baz where id = 12";
//
//        String deleteRange = "delete from bazzle where id < 5;";
//
//        String deleteTwoCondsAnd = "delete from bar where id = 1 and age = 24";
//        String deleteTwoCondsOr = "delete from bar where id = 2 or age = 12";
//
//        String deleteMixed = "delete from baz where id = 1 and department = \"baz\" or id  = 3;";
//
//        database.executeNonQuery(deleteByPK);
//        database.executeNonQuery(deleteByPKNonExist);
//        database.executeNonQuery(deleteRange);
//        database.executeNonQuery(deleteTwoCondsAnd);
//        database.executeNonQuery(deleteTwoCondsOr);
//        database.executeNonQuery(deleteMixed);
//
//        database.terminateDatabase();
//
//        System.out.println("Restarting just storage manager...");
//        try {
//            sm = new StorageManager(dbLoc, pageBufferSize, pageSize, true);
//
//            System.out.println("Getting table 1 data...");
//            Object[][] data1 = sm.getRecords(table1Id);
//
//            if(data1.length != 3 || !Arrays.equals(data1[0], expectedData1_0) ||
//                    !Arrays.equals(data1[1], expectedData1_1) ||
//                    !Arrays.equals(data1[2], expectedData1_2)){
//                System.err.println("Deleting from Table 1 failed...");
//                System.exit(1);
//            }
//
//            System.out.println("Getting table 2 data...");
//            Object[][] data2 = sm.getRecords(table2Id);
//
//            if(data2.length != 0 ){
//                System.err.println("Deleting Table 2 failed...");
//                System.exit(1);
//            }
//
//            System.out.println("Getting table 3 data...");
//            Object[][] data3 = sm.getRecords(table3Id);
//
//            if(data3.length != 0){
//                System.err.println("Deleting from Table 3 failed...");
//                System.exit(1);
//            }
//
//            System.out.println("Getting table 4 data...");
//            Object[][] data4 = sm.getRecords(table4Id);
//
//            if(data4.length != 0){
//                System.err.println("Deleting Table 4 failed...");
//                System.exit(1);
//            }
//            sm.terminateDatabase();
//        } catch (StorageManagerException e){
//            e.printStackTrace();
//            System.exit(1);
//        }
//
//        System.out.println("Restarting the database...");
//
//        database = Database.getConnection(dbLoc, pageBufferSize, pageSize);
//
//        //testing delete
//        System.out.println("Testing update.... none should fail...");
//
//        String update1 = "update foo set name = \"hello\" where id = 1;";
//        String update2 = "update foo set amount = amount + 1.0 where id >= 2 and amount < 30.0;";
//        String update3 = "update foo set name = \"hi\", amount = 4.0 where amount = 4.12 or id = 3;";
//
//        database.executeNonQuery(update1);
//        database.executeNonQuery(update2);
//        database.executeNonQuery(update3);
//
//        database.terminateDatabase();
//
//        System.out.println("Restarting just storage manager...");
//        try {
//            sm = new StorageManager(dbLoc, pageBufferSize, pageSize, true);
//
//            System.out.println("Getting table 1 data...");
//            Object[][] data1 = sm.getRecords(table1Id);
//
//            expectedData1_0[1] = "hello";
//            expectedData1_1[2] = 4.0;
//            expectedData1_1[1] = "hi";
//            expectedData1_2[1] = "hi";
//            expectedData1_2[2] = 4.0;
//
//
//            if(data1.length != 3 || !Arrays.equals(data1[0], expectedData1_0) ||
//                    !Arrays.equals(data1[1], expectedData1_1) ||
//                    !Arrays.equals(data1[2], expectedData1_2)){
//                System.err.println("Updating Table 1 failed...");
//                System.exit(1);
//            }
//
//            sm.terminateDatabase();
//        } catch (StorageManagerException e){
//            e.printStackTrace();
//            System.exit(1);
//        }

        System.out.println("Testing complete...");
    }
}
