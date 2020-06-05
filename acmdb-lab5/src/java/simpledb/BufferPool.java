package simpledb;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */

class PageLock{
    public PageId pageId;
    private Set<TransactionId> sharedLocks;
    private TransactionId exclusiveLock;

    PageLock(PageId pid){
        pageId = pid;
        sharedLocks =  Collections.synchronizedSet(new HashSet<>());
        exclusiveLock = null;
    }

    PageId getPageId(){
        return pageId;
    }

    Set<TransactionId> getSharedLocks(){
        return sharedLocks;
    }

    TransactionId getExclusiveLock(){
        return exclusiveLock;
    }

    boolean applyLock(Permissions perm, TransactionId tid){
        if (perm.equals(Permissions.READ_ONLY))
        //read_only for shared locks
        {
            //check exclusive
            if (exclusiveLock != null) return exclusiveLock.equals(tid);
            sharedLocks.add(tid);
            return true;
        }
        //need exclusiveLock
        else {
            if (exclusiveLock != null) return exclusiveLock.equals(tid);
            if (sharedLocks.size() > 1) return false;
            if (sharedLocks.isEmpty() || sharedLocks.contains(tid)){
                exclusiveLock = tid;
                sharedLocks.clear();
                return true;
            }
            return false;
        }
    }

    void releaseLock(TransactionId tid){
        assert exclusiveLock == null || tid.equals(exclusiveLock);
        if (tid.equals(exclusiveLock)) exclusiveLock = null;
        else sharedLocks.remove(tid);
    }

    boolean holdsLock(TransactionId tid){
        return exclusiveLock.equals(tid) || sharedLocks.contains(tid);
    }

    boolean exclusive() {
        return exclusiveLock != null;
    }

    Set<TransactionId> relatedTransactions(){
        Set<TransactionId> transactionIds = new HashSet<>(sharedLocks);
        if (exclusiveLock != null)
            transactionIds.add(exclusiveLock);
        return  transactionIds;
    }
}


public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int capacity;
    private ConcurrentHashMap<PageId, Page> pgBufferpool;
    private ConcurrentHashMap<PageId, Integer> LRUmap;

    private ConcurrentHashMap<PageId, PageLock> PageIdToLock;
    private ConcurrentHashMap<TransactionId, Set<PageId>> TidToPageId;
    private DependencyGraph dependencyGraph;

    //Detect the Deadlock
    private class DependencyGraph{

        private ConcurrentHashMap<TransactionId, Set<TransactionId>> TidToEdge = new ConcurrentHashMap<>();
        private Set<TransactionId> visit = Collections.synchronizedSet(new HashSet<>());

        synchronized void modifyEdges(TransactionId tid, PageId pid){
            TidToEdge.putIfAbsent(tid, new HashSet<>());
            Set<TransactionId> edges = TidToEdge.get(tid);
            edges.clear();
            if (pid == null) return;

            Set<TransactionId> pidToTidlist;
            synchronized (PageIdToLock.get(pid)){
                pidToTidlist = PageIdToLock.get(pid).relatedTransactions();
            }
            edges.addAll(pidToTidlist);
        }

        //If deadlock(cycle) return true else return false
        synchronized boolean checkDeadlock(TransactionId tid){
            visit.clear();
            boolean flag = dfs(tid);
            return flag;
        }

        synchronized boolean dfs(TransactionId tid){
            visit.add(tid);
            Set<TransactionId> edges = TidToEdge.get(tid);
            boolean flag = false;
            for (TransactionId nextTid : edges){
                if (visit.contains(nextTid)){
                    return true;
                }
                flag = flag || dfs(nextTid);
            }
            return flag;
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        capacity = numPages;
        pgBufferpool = new ConcurrentHashMap<>();
        LRUmap = new ConcurrentHashMap<>();

        PageIdToLock = new ConcurrentHashMap<>();
        TidToPageId = new ConcurrentHashMap<>();
        dependencyGraph = new DependencyGraph();

    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    private void LRUupdate(PageId pid){
        for (PageId key : LRUmap.keySet())
        {
            int tmp = LRUmap.get(key);
            if (tmp > 0) LRUmap.put(key, tmp - 1);
        }
        LRUmap.put(pid, capacity);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        PageIdToLock.putIfAbsent(pid, new PageLock(pid));
        boolean applyLock;
        synchronized (PageIdToLock.get(pid)){
            applyLock = PageIdToLock.get(pid).applyLock(perm, tid);
        }
        while (!applyLock) {
            dependencyGraph.modifyEdges(tid, pid);
            if (dependencyGraph.checkDeadlock(tid)) {
                throw new TransactionAbortedException();
            }
            synchronized (PageIdToLock.get(pid)){
                applyLock = PageIdToLock.get(pid).applyLock(perm, tid);
            }
        }
        dependencyGraph.modifyEdges(tid, null);

        TidToPageId.putIfAbsent(tid, new HashSet<>());
        TidToPageId.get(tid).add(pid);
        Page page;
        if (pgBufferpool.containsKey(pid)){
            page = pgBufferpool.get(pid);
        } else{
            if (pgBufferpool.size() >= capacity) evictPage();
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pgBufferpool.put(pid, page);
            //New page into the bufferpool should record Before statue
            page.setBeforeImage();
        }
//      LRUupdate(pid);
        return page;
    }



    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2

        synchronized (PageIdToLock.get(pid)){
            PageIdToLock.get(pid).releaseLock(tid);
        }
        TidToPageId.get(tid).remove(pid);
    }


    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        synchronized (PageIdToLock.get(p)){
            return PageIdToLock.get(p).holdsLock(tid);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> LockPages = TidToPageId.get(tid);
        TidToPageId.remove(tid);
        if (LockPages == null) return;
        for (PageId pid: LockPages){

            Page page = pgBufferpool.get(pid);
            if (page != null && PageIdToLock.get(pid).exclusive()){
                // FORCE buffer management policy
                // If commit = true we force the dirty pages to disk
                // If commit = false, It means abort and we recover from before image
                if (commit) {
                    if (page.isDirty() != null){
                        flushPage(pid);
                        page.setBeforeImage();
                    }
                } else {
                    assert page.getBeforeImage() != null;
                    pgBufferpool.put(pid, page.getBeforeImage());
                }
            }
            synchronized (PageIdToLock.get(pid)){
                PageIdToLock.get(pid).releaseLock(tid);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile tableFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirty_pages = tableFile.insertTuple(tid,t);
        for (Page page : dirty_pages) {
            PageId pid = page.getId();
            page.markDirty(true, tid);
            pgBufferpool.put(pid, page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile tableFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirty_pages = tableFile.deleteTuple(tid, t);
        for (Page page : dirty_pages) {
            PageId pid = page.getId();
            page.markDirty(true, tid);
            pgBufferpool.put(pid, page);
        }
    }


    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageId: pgBufferpool.keySet()){
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
//        if (!pgBufferpool.containsKey(pid)) return;
//        try {
//            flushPage(pid);
//        } catch (IOException e){
//            e.printStackTrace();
//        }
//        pgBufferpool.remove(pid);
//        LRUmap.remove(pid);
        pgBufferpool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        Page page = pgBufferpool.get(pid);
        if (page == null) throw new IOException();
        if (page.isDirty() == null) return;
        page.markDirty(false, null);
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
//        Set<PageId> pageIdSet = TidToPageId.get(tid);
//        for (PageId pageId : pageIdSet){
//            flushPage(pageId);
//        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // In particular, it must never evict a dirty page.
        for (Map.Entry<PageId, Page> entry : pgBufferpool.entrySet()) {
            PageId pid = entry.getKey();
            Page   page   = entry.getValue();
            if (page.isDirty() == null) {
                // dont need to flushpage since all page evicted are not dirty
                // flushPage(pid);
                discardPage(pid);
                return;
            }
        }
        throw new DbException("NO STEAL POLICY failed because all pages are dirty");
    }

}

