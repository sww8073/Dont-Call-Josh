package storagemanager;

/**
 * this is a tester class for the storage manager
 */
public class StorageManagerTester {

    public static void main(String[] args) {
        try {
            StorageManager storageManager = new StorageManager("C:\\Users\\Matthew\\Desktop\\DBSIDataBase",
                    10, 10, false);



        } catch (StorageManagerException e)  {
            System.out.println(e);
        }
    }
}
