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
        this.bufferSize = bufferSize;
        this.buffer = new ArrayList<>(bufferSize);
        this.bufLoc = bufLoc;
    }

    public int getBufferSize(){
        return this.bufferSize;
    }
    public int getPageSize(){
        return this.pageSize;
    }
    public String getBufLoc(){
        return this.bufLoc;
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
            if((int)page.getPageId() == (int)pageId) { // page found in buffer
                // move page to front of buffer
                buffer.remove(i);
                buffer.add(0, page);
                return page;
            }
        }

        // look for page in memory
        Page page = readPageFromMem(pageId);
        addPage(page); // add page to buffer
        return page;
    }

    /**
     * This function deletes a page rom either the buffer of form
     * its corresponding directory.
     * @param pageId the unique page id of a Page
     * @return true if page was deleted
     */
    public boolean deletePage(Integer pageId)  {
        for(int i = 0;i < buffer.size();i++)    {
            Page page = buffer.get(i);
            if(page.getPageId() == pageId) { // page found in buffer
                // move page to front of buffer
                buffer.remove(i);
                return true;
            }
        }

        String path = bufLoc + "\\" + pageId;
        File pageFile = new File(path);
        return pageFile.delete();
    }

    /**
     * Reads in a page from memory using a given page number
     * @param pageInt page number to read in from memory
     * @return the found page, null if the page was not found
     */
    private Page readPageFromMem(Integer pageInt) {
        String path = bufLoc + "\\" + pageInt;
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);

            Page page = (Page) objectIn.readObject();

            File pageFile = new File(path);
            objectIn.close();
            pageFile.delete();
            return page;
        } catch (FileNotFoundException e)   {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        } catch (ClassNotFoundException e) {
            System.err.println(e);
        }
        return null;
    }

    /**
     * This function will write a page to the specified
     * @param page the page to be removed
     */
    private void writePageToMem(Page page)  {
        try {
            String path = bufLoc + "\\" + page.getPageId();
            FileOutputStream fileOut = new FileOutputStream(path);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(page);
            objectOut.close();

        } catch (FileNotFoundException e)   {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    /**
     * This function writes all the pages in the buffer to memory
     */
    public void purge() {
        while(buffer.size() > 0)    {
            writePageToMem(buffer.get(0));
            buffer.remove(0);
        }
    }
}
