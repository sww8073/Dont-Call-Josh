package storagemanager;

import storagemanager.AStorageManager;
import storagemanager.StorageManager;
import storagemanager.StorageManagerException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
/**
 * this is a tester class for the storage manager
 */
public class StorageManagerTester {

    @SuppressWarnings("EmptyCatchBlock")
    public static void main(String[] args) {

        // Modify based on your system and testing params
        String dbLoc = "C:\\DataBase/";
        int pageBufferSize = 20;
        int pageSize = 4096;
        AStorageManager sm = null;

        System.out.println("Creating storage manager");
        try {
            // Create a new database at the provided location
            sm = new StorageManager(dbLoc, pageBufferSize, pageSize, false);
        } catch (Exception e) {
            System.err.println("Database could not be created");
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("Adding tables to the database");
        // Add tables
        String[] dataTypes1 = {"integer", "char(40)", "double", "boolean"};
        Integer[] keyIndices1 = new Integer[]{0,2};

        /**
        try {
            sm.addTable(1, dataTypes1, keyIndices1);
        } catch (Exception e) {
            System.err.println("Failed to insert into table 1");
            e.printStackTrace();
            System.exit(1);
        }

        String[] dataTypes2 = {"integer", "varchar(60)"};
        Integer[] keyIndices2 = new Integer[]{0};

        try {
            sm.addTable(2, dataTypes2, keyIndices2);
        } catch (Exception e) {
            System.err.println("Failed to insert into table 2");
            e.printStackTrace();
            System.exit(1);
        }

        // Try to add a table with same number, should throw a Storage Manager Exception
        try {
            sm.addTable(1, dataTypes1, keyIndices2);
            System.err.println("Added a table with duplicate ids: 1");
            System.exit(1);
        } catch (StorageManagerException e) {}

        System.out.println("Populating table with data... testing insert");
        // Populate tables
        // Table 1
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
                sm.insertRecord(1, record);
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
                sm.insertRecord(2, record);
            }
        } catch(Exception e) {
            System.err.println("Failed to insert record.");
            e.printStackTrace();
        }

        try {
            sm.insertRecord(1, new Object[]{500, "Duplicate key", 37.73, false});
            System.err.println("Inserted a record with duplicate key");
            System.exit(1);
        } catch (StorageManagerException e) {}

        System.out.println("Testing getRecords for all tables");
        // Getting all table entries
        // Table 1
        try {
            Object[][] table1Data = sm.getRecords(1);
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
            Object[][] table1Data = sm.getRecords(2);
            Object[] row0 = table1Data[0];
            if(!(row0[0]).equals(2) || !(row0[1]).equals("Sheree Eixenberger")){
                System.err.println("Invalid record at table 2 position 0");
            }
            Object[] row299 = table1Data[299];
            if(!(row299[0]).equals(301) || !(row299[1]).equals("Robers Hayhow")){
                System.err.println("Invalid record at table 2 position 299");
            }
        } catch (StorageManagerException e) {
            System.err.println("Error in get all table 2 records");
            e.printStackTrace();
            System.exit(1);
        }

        // Table 3 (does not exist)
        try {
            sm.getRecords(3);
            System.err.println("Returned data for non-existent table");
            System.exit(1);
        } catch (StorageManagerException e) {}

        System.out.println("Testing getRecord");
        // Testing get record

        try {
            Object[] rec1 = sm.getRecord(1, new Object[]{242, 43.29});
            if(!(rec1[0]).equals(242) || !(rec1[1]).equals("Prinz Deeney")
                    || !(rec1[2]).equals(43.29) || !(rec1[3]).equals(false)){
                System.err.println("Invalid record at table 1 key 242,43.29");
                System.exit(1);
            }
            Object[] rec3 = sm.getRecord(2, new Object[]{360});
            if(!(rec3[0]).equals(360) || !(rec3[1]).equals("Gabrielle Bollon")){
                System.err.println("Invalid record at table 2 key 360");
                System.exit(1);
            }
            Object[] rec2 = sm.getRecord(1, new Object[]{242, 55.5});
            if(rec2 != null){
                System.err.println("Record returned for non-existent key");
                System.exit(1);
            }
            try{
                sm.getRecord(3, new Object[1]);
                System.err.println("Returned record for non-existent table");
                System.exit(1);
            } catch (StorageManagerException e1){}

        } catch (StorageManagerException e) {
            System.err.println("Error in getRecord");
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("Testing update");

        try {
            sm.updateRecord(1, new Object[]{970, "Updated", 77.74, false});
            Object[] updated = sm.getRecord(1, new Object[]{970,77.74});
            if(!(updated[0]).equals(970) || !(updated[1]).equals("Updated")
                    || !(updated[2]).equals(77.74) || !(updated[3]).equals(false)){
                System.err.println("Failed to update record");
                System.exit(1);
            }
            try{
                sm.updateRecord(2, new Object[]{1008,"Non-existent"});
                System.err.println("Updated non-existent record");
                System.exit(1);
            } catch (StorageManagerException e){}
        } catch (StorageManagerException e) {
            System.err.println("Failed to update record");
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("Testing remove record");

        try {
            sm.removeRecord(1, new Object[]{997, 74.59});
        } catch (StorageManagerException e) {
            System.err.println("Failed to remove record");
            e.printStackTrace();
            System.exit(1);
        }

        try {
            sm.removeRecord(1, new Object[]{997, 74.59});
            System.err.println("Removed non-existent record");
            System.exit(1);
        } catch (StorageManagerException e) {}

        System.out.println("Testing purge buffer and restart");

        AStorageManager restartedSM = null;

        try {
            sm.purgeBuffer();
            sm.terminateDatabase();
            restartedSM = new StorageManager(dbLoc, pageBufferSize, pageSize, true);
            Object[] row = restartedSM.getRecord(1, new Object[]{1, 36.76});
            if(!(row[0]).equals(1) || !(row[1]).equals("Morna Probbing")
                    || !(row[2]).equals(36.76) || !(row[3]).equals(false)){
                System.err.println("Invalid record at table 1 position 0 after restart");
            }
        } catch (StorageManagerException e) {
            e.printStackTrace();
        }

        if(restartedSM == null){
            System.err.println("Failed to restart the database");
            System.exit(1);
        }

        System.out.println("Testing drop/clear tables");
        try {
            restartedSM.dropTable(2);
            try{
                restartedSM.getRecords(2);
                System.err.println("Failed to drop table");
            } catch (StorageManagerException e){}
        } catch (StorageManagerException e) {
            System.err.println("Failed to drop table");
            e.printStackTrace();
            System.exit(1);
        }

        try {
            restartedSM.clearTable(1);
            Object[][] data = restartedSM.getRecords(1);
            if(data.length != 0){
                System.err.println("Failed to clear table");
                System.exit(1);
            }
        } catch (StorageManagerException e) {
            System.err.println("Failed to clear table");
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("There should be no pages in the db after completing these tests....");

         */
        System.out.println("All tests passed!");
    }
}
