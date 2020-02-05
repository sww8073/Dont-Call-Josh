/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;

import java.util.ArrayList;

public class BufferManager {

    // private instance variables
    private int pageSize;
    private int bufferSize;
    private ArrayList<Page> buffer;

    /**
     * constructor
     * @param pageSize the size of each page in mb
     * @param bufferSize the amount of pages that can bea added to the buffer
     */
    public BufferManager(int pageSize, int bufferSize) {
        this.pageSize = pageSize;
        this.bufferSize = bufferSize;
        this.buffer = new ArrayList<>(bufferSize);
    }

    public void addPage(Page page)  {
        if(buffer.size() < bufferSize)  {

        }
    }

}
