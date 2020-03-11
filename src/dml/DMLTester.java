package dml;

import database.Catalog;
import database.Database;
import database.IDatabase;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;

public class DMLTester {
    public static void main(String[] args) {
        String dbLoc = "C:\\Users\\Matthew\\Desktop\\db\\dba";
        int pageBufferSize = 20;
        int pageSize = 4096;
        AStorageManager sm;

        IDatabase database = Database.getConnection(dbLoc, pageBufferSize, pageSize);

        DMLParser dmlParser = new DMLParser();

        String insert1 = "insert into foo values (1 \"foo\" true 2.1);";
        String insert2 = "insert into foo values (1 \"foo\" true 2.1)," +
                "(2 \"baz\" true 4.14)," +
                "(3 \"bar\" true 5.2);";

        try {
            dmlParser.parseDMLStatement(insert1);
            dmlParser.parseDMLStatement(insert2);
        }
        catch (DMLParserException e)    {
            System.err.println(e.getMessage());
        }

    }
}
