package ddl;

import database.Database;
import database.IDatabase;
import ddl.DDLParserException;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;

public class Phase2Tester {

    public static void main(String[] args) {

        // You may need to modify some values to test on your system
        String dbLoc = "C:\\Users\\Matthew\\Desktop\\db\\dba";
        int pageBufferSize = 20;
        int pageSize = 4096;
        AStorageManager sm;

        IDatabase database = Database.getConnection(dbLoc, pageBufferSize, pageSize);

        // assumes table 1 in storage manager... change if needed
        int table1Id = 1;
        String createTable1 = "create table foo(" +
                                "id integer unique," +
                                "name varchar(40) not null," +
                                "amount double unique," +
                                "married boolean," +
                                "primarykey( id amount )" +
                                ");";

        // assumes table 2 in storage manager
        int table2Id = 2;
        String createTable2 = "CREATE TABLE baz(" +
                                "id INTEGER PRIMARYKEY," +
                                "department varchar(60)," +
                                "unique( department )," +
                                "foreignkey( id ) references foo( id )" +
                                ");";

        String createErrorType = "create table fail(" +
                                "id primarykey);";

        String createErrorExists= "CREATE TABLE baz(" +
                "id INTEGER PRIMARYKEY," +
                "department varchar(60)," +
                "unique( department )," +
                "foreignkey( id ) references foo( id )" +
                ");";

        String createErrorFK1= "CREATE TABLE bar(" +
                "id INTEGER PRIMARYKEY," +
                "department varchar(60)," +
                "unique( department )," +
                "foreignkey( id ) references bazzle( id )" +
                ");";

        String createErrorFK2= "CREATE TABLE bar(" +
                "id INTEGER PRIMARYKEY," +
                "department varchar(60)," +
                "unique( department )," +
                "foreignkey( id ) references foo( notthere )" +
                ");";

        String createErrorFK3= "CREATE TABLE bar(" +
                "id INTEGER PRIMARYKEY," +
                "department varchar(60)," +
                "unique( department )," +
                "foreignkey( nothere ) references foo( id )" +
                ");";

        String createErrorPK1= "CREATE TABLE bar(" +
                "id INTEGER," +
                "department varchar(60)," +
                "unique( department )," +
                "primarykey( nothere )," +
                "foreignkey( id ) references foo( id )" +
                ");";

        String createErrorU1= "CREATE TABLE bar(" +
                "id INTEGER," +
                "department varchar(60)," +
                "unique( nothere )," +
                "primarykey( id )," +
                "foreignkey( id ) references foo( id )" +
                ");";

        System.out.println("Testing create table:");
        System.out.println("\tCreates 2 tables successfully, 7 failures");
        database.executeNonQuery(createTable1);
        database.executeNonQuery(createTable2);

        System.out.println("\tThese table creates should report failure but not stop the database...");
        database.executeNonQuery(createErrorExists);
        database.executeNonQuery(createErrorType);
        database.executeNonQuery(createErrorPK1);
        database.executeNonQuery(createErrorFK1);
        database.executeNonQuery(createErrorFK2);
        database.executeNonQuery(createErrorFK3);
        database.executeNonQuery(createErrorU1);

        System.out.println("Shutting down the database...");
        database.terminateDatabase();

        System.out.println("Restarting just storage manager...");
        try {
            sm = new StorageManager(dbLoc, pageBufferSize, pageSize, true);

            System.out.println("Trying to populate created tables...");

            try {
                BufferedReader br = new BufferedReader(new FileReader("inputs/data1.csv"));

                // Reading in lines and shuffling them
                ArrayList<String> lines = new ArrayList<>();
                String line;
                while((line = br.readLine()) != null) {
                    lines.add(line);
                }
                Collections.shuffle(lines);

                System.out.println("Inserting into Table 1: this may take a moment");

                // Populating the table to table
                for(String line2: lines){
                    String[] elems = line2.split(",");
                    Object[] record = {Integer.parseInt(elems[0]),
                            elems[1],
                            Double.parseDouble(elems[2]),
                            Boolean.valueOf(elems[3])};
                    sm.insertRecord(table1Id, record);
                }
            } catch(Exception e) {
                System.err.println("Failed to insert record.");
                e.printStackTrace();
            }

            // Table 2
            try {
                BufferedReader br = new BufferedReader(new FileReader("inputs/data2.csv"));

                // Reading in lines and shuffling them
                ArrayList<String> lines = new ArrayList<>();
                String line;
                while((line = br.readLine()) != null) {
                    lines.add(line);
                }
                Collections.shuffle(lines);

                System.out.println("Inserting into Table 2: this may take a moment");

                // Populating the table to table
                for(String line2: lines){
                    String[] elems = line2.split(",");
                    Object[] record = {Integer.parseInt(elems[0]),
                            elems[1]};
                    sm.insertRecord(table2Id, record);
                }
            } catch(Exception e) {
                System.err.println("Failed to insert record.");
                e.printStackTrace();
            }

            try {
                sm.insertRecord(table1Id, new Object[]{500, "Duplicate key", 37.73, false});
                System.err.println("Inserted a record with duplicate key");
                System.exit(1);
            } catch (StorageManagerException e) {}
            System.out.println("Testing getRecords for all tables");
            // Getting all table entries
            // Table 1
            try {
                Object[][] table1Data = sm.getRecords(table1Id);
                Object[] row0 = table1Data[0];
                if(!(row0[0]).equals(1) || !(row0[1]).equals("Morna Probbing")
                        || !(row0[2]).equals(36.76) || !(row0[3]).equals(false)){
                    System.err.println("Invalid record at table 1 position 0");
                }
                Object[] row299 = table1Data[299];
                if(!(row299[0]).equals(300) || !(row299[1]).equals("Urbano Petrussi")
                        || !(row299[2]).equals(77.51) || !(row299[3]).equals(true)){
                    System.err.println("Invalid record at table 1 position 299");
                }
            } catch (StorageManagerException e) {
                System.err.println("Error in get all table 1 records");
                e.printStackTrace();
                System.exit(1);
            }

            // Table 2
            try {
                Object[][] table1Data = sm.getRecords(table2Id);
                Object[] row0 = table1Data[0];
                if(!(row0[0]).equals(2) || !(row0[1]).equals("Sheree Eixenberger")){
                    System.err.println(row0[0]);
                    System.err.println("Invalid record at table 2 position 0");
                }
                Object[] row299 = table1Data[299];
                if(!(row299[0]).equals(301) || !(row299[1]).equals("Robers Hayhow")){
                    System.err.println(row299[0]);
                    System.err.println("Invalid record at table 2 position 299");
                }
            } catch (StorageManagerException e) {
                System.err.println("Error in get all table 2 records");
                e.printStackTrace();
                System.exit(1);
            }

            System.out.println("Shutting down the storage manager...");
            sm.terminateDatabase();
        } catch (StorageManagerException e) {
            e.printStackTrace();
            System.exit(1);
        }
//
//        System.out.println("Restarting the database...");
//
//        database = Database.getConnection(dbLoc, pageBufferSize, pageSize);
//
//        System.out.println("Altering table 1. this may take a moment...");
//        System.out.println("Three failures should be reported.");
//
//
//        String alterAddNull = "alter table foo add gar double;";//       String alterAddDefault = "alter table foo add another integer default 10;";
//        String alterExists = "alter table foo add id double;";
//        String alterDropAttr = "alter table baz drop department;";
//        String alterNonexistentTable = "alter table bazzle drop attr;";
//        String alterNonexistentAttr = "alter table baz drop foo;";
//
//        database.executeNonQuery(alterAddNull);
//        database.executeNonQuery(alterAddDefault);
//        database.executeNonQuery(alterDropAttr);
//
//        System.out.println("These alters should report failure...");
//        database.executeNonQuery(alterExists);
//        database.executeNonQuery(alterNonexistentTable);
//        database.executeNonQuery(alterNonexistentAttr);
//
//        System.out.println("Shutting down the database...");
//        database.terminateDatabase();
//
//        System.out.println("Restarting just storage manager...");
//        try {
//            sm = new StorageManager(dbLoc, pageBufferSize, pageSize, true);
//            System.out.println("Testing getRecords for all tables. Looking for alters...");
//            // Getting all table entries
//            // Table 1
//            try {
//                Object[][] table1Data = sm.getRecords(table1Id);
//                Object[] row0 = table1Data[0];
//                if(row0.length != 6 || !(row0[0]).equals(1) || !(row0[1]).equals("Morna Probbing")
//                        || !(row0[2]).equals(36.76) || !(row0[3]).equals(false) ||
//                        row0[4] != null || !row0[5].equals(10)){
//                    for(Object o: row0)
//                        System.out.print(o + ",");
//                    System.out.println();
//                    System.err.println("Invalid record at table 1 position 0. Alter add failed");
//                }
//                Object[] row299 = table1Data[299];
//                if(row0.length != 6 || !(row299[0]).equals(300) || !(row299[1]).equals("Urbano Petrussi")
//                        || !(row299[2]).equals(77.51) || !(row299[3]).equals(true) ||
//                        row0[4] != null || !row0[5].equals(10)){
//                    System.err.println("Invalid record at table 1 position 299. Alter add failed");
//                }
//            } catch (StorageManagerException e) {
//                System.err.println("Error in get all table 1 records");
//                e.printStackTrace();
//                System.exit(1);
//            }
//
//            // Table 2
//            try {
//                Object[][] table1Data = sm.getRecords(table2Id);
//                Object[] row0 = table1Data[0];
//                if(row0.length != 1 || !(row0[0]).equals(2)){
//                    System.err.println(row0[0]);
//                    System.err.println("Invalid record at table 2 position 0. Alter drop failed");
//                }
//                Object[] row299 = table1Data[299];
//                if(row0.length != 1 || !(row299[0]).equals(301)){
//                    System.err.println(row299[0]);
//                    System.err.println("Invalid record at table 2 position 299. Alter drop failed");
//                }
//            } catch (StorageManagerException e) {
//                System.err.println("Error in get all table 2 records");
//                e.printStackTrace();
//                System.exit(1);
//            }
//
//            System.out.println("Shutting down the storage manager...");
//            sm.terminateDatabase();
//        } catch (StorageManagerException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("Restarting the database...");
//        database = Database.getConnection(dbLoc, pageBufferSize, pageSize);
//
//        System.out.println("Dropping tables. One should report an error...");
//
//        String drop1 = "drop table foo;";
//        String drop2 = "drop table baz;";
//        String dropNonexistent = "drop table bazzle;";
//
//        database.executeNonQuery(drop1);
//        database.executeNonQuery(drop2);
//        database.executeNonQuery(dropNonexistent);
//
//        System.out.println("Shutting down the database...");
//        database.terminateDatabase();
//
//        System.out.println("Restarting the storage manager...");
//
//        try {
//            sm = new StorageManager(dbLoc, pageBufferSize, pageSize, true);
//            sm.dropTable(table1Id); //this should fail...
//            System.err.println("Drop failed...");
//        } catch (StorageManagerException e) {
//            System.out.println("dropped successfully");
//        }
//
//        System.out.println("Testing complete....");
    }
}
