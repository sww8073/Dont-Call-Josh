/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

public class BufferManager {

    // private instance variables
    private int pageSize;
    private int bufferSize;
    private String bufLoc;
    private ArrayList<Page> buffer; // most recently used in front

    /**
     * constructor
     * @param pageSize the size of each page in mb
     * @param bufferSize the amount of pages that can bea added to the buffer
     */
    public BufferManager(int pageSize, int bufferSize, String bufLoc) {
        this.pageSize = pageSize;
        this.bufferSize = bufferSize;
        this.buffer = new ArrayList<>(bufferSize);
        this.bufLoc = bufLoc;
    }

    /**
     * this function adds a page to the buffer. If the buffer is full it
     * write the LRU page to memory and the adds the page to the buffer.
     * @param page
     */
    public void addPage(Page page)  {
        if(buffer.size() < bufferSize)  { // there is room in the buffer
            buffer.add(page);
        }
        else    {
            // remove lRU page and write to mem
            Page removePage = buffer.get(bufferSize - 1);
            writePageToMem(removePage);
            buffer.remove(bufferSize - 1);

            //add new page to buffer to first index
            buffer.add(0, page);
        }
    }

    private void writePageToMem(Page page)  {
        try {
            FileOutputStream fileOut = new FileOutputStream(bufLoc);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(page);
            objectOut.close();

        } catch (FileNotFoundException e)   {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}
