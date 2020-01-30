/**
 * Buffer: Phase 1
 * Team: Don't Tell Josh
 * Members: Matthew Clements, Josh Tellier, Stone Warren, Josh Schenk
 */
package storagemanager;

/**
 * this class is an im-memory buffer used to stor recently used pages
 */
public class Buffer {

    private Object[] bufferArr; // object array that will hold pages in memory
    private int pageBufferSize; // max number of pages allowed in the buffer at any given time
    private int pageSize; // the size of a page in kilobytes

    /**
     * Constructor for a new buffer
     * @param pageBufferSize max number of pages allowed in the buffer at any given time
     * @param pageSize the size of a page in kilobytes
     */
    public Buffer(int pageBufferSize, int pageSize) {
        this.bufferArr = new Object[pageBufferSize];
        this.pageBufferSize = pageBufferSize;
        this.pageSize = pageSize;
    }
}
