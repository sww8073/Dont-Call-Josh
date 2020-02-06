/**
 * Storage Manager: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;

import java.io.*;
import java.util.ArrayList;

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
        //this.bufferSize = bufferSize;
        this.bufferSize = 0; // TODO remove this, its for testing only
        this.buffer = new ArrayList<>(bufferSize);
        this.bufLoc = bufLoc;
    }

    /**
     * this function adds a page to the buffer. If the buffer is full it
     * write the LRU page to memory and the adds the page to the buffer.
     * @param page Page object being added to buffer
     */
    public void addPage(Page page)  {
        if(buffer.size() < bufferSize)  { // there is room in the buffer
            buffer.add(0, page);
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

    /**
     * This function retrieves a page, first checking the buffer, then checking memory
     * @param pageId the unique id of a page
     * @return Page Obejct or null if page does not exist
     */
    public Page getPage(Integer pageId) {
        for(int i = 0;i < buffer.size();i++)    {
            Page page = buffer.get(i);
            if(page.getPageId() == pageId) { // page found in buffer
                // move page to front of buffer
                buffer.remove(i);
                buffer.add(0, page);
                return page;
            }
        }

        // look for page in memory
        File file = new File(bufLoc);
        File[] files = file.listFiles();
        // TODO loop through all object in file and check to see if it what we want
        Page page = readPageFromMem(pageId);

        return null;
    }

    /**
     * Reads in a page from memory using a given page number
     * @param pageInt page number to read in from memory
     * @return the found page, null if the page was not found
     */
    private Page readPageFromMem(Integer pageInt) {
        boolean cont = true;
        try {
            FileInputStream fileIn = new FileInputStream(bufLoc + "/" + pageInt + ".txt");
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);

            // loop through entire directory
            while(cont) {
                Page page = (Page) objectIn.readObject();
                if(page != null && page.getPageId() == pageInt)    {
                    return page; // we found the page we are looking for
                }
                else    {
                    cont = false; // all objects in directory have been read
                }
            }

            objectIn.close();
            return null;
        } catch (FileNotFoundException e)   {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        } catch (ClassNotFoundException e) {
            System.err.println(e);
        }
        return null;
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
