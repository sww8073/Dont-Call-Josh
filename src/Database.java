/**
 * Class to create and access a database.
 */

public class Database implements IDatabase{

    /**
     * Static function that will create/restart and return a database
     * @param dbLoc the location to start/restart the database in
     * @param pageBufferSize the size of the page buffer; max number of pages allowed in the buffer at any given time
     * @param pageSize the size of a page in bytes
     * @return an instance of an IDatabase.
     */
    public static IDatabase getConnection(String dbLoc, int pageBufferSize, int pageSize ){
        return null;
    }

    /**
     * This method will be used when executing database statements that do not return anything.
     *
     * @param statement the statement to execute
     */
    public void executeNonQuery(String statement) {

    }

    /**
     * This method will be used when executing database queries that return tables of data.
     *
     * @param query the query to execute
     * @return the table of data returned. Size zero if empty
     */
    public Object[][] executeQuery(String query) {
        return new Object[0][];
    }

    /**
     * This method will be used to safely shutdown the database.
     * It will store any needed data needed to restart the database to physical hardware.
     */
    public void terminateDatabase() {

    }
}
