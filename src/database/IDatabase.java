package database;

/**
 * Interface representing a database
 */
public interface IDatabase {

    /**
     * This method will be used when executing database statements that do not return anything.
     * @param statement the statement to execute
     */
    public void executeNonQuery(String statement);

    /**
     * This method will be used when executing database queries that return tables of data.
     * @param query the query to execute
     * @return the table of data returned. Size zero if empty
     */
    public Object[][] executeQuery(String query);

    /**
     * This method will be used to safely shutdown the database.
     * It will store any needed data needed to restart the database to physical hardware.
     */
    public void terminateDatabase();
}
