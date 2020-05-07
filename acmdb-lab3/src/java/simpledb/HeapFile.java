package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            int pos = pid.pageNumber() * BufferPool.getPageSize();
            randomAccessFile.seek(pos);
            randomAccessFile.read(data, 0, data.length);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e){
            throw new IllegalArgumentException();
        }
        return page;
    }

    // see DbFile.java for javadocs
//    public void writePage(Page page) throws IOException {
//        // some code goes here
//        // not necessary for lab1
//        PageId pid = page.getId();
//        try {
//            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
//            randomAccessFile.seek(pid.pageNumber() * BufferPool.getPageSize());
//            randomAccessFile.write(page.getPageData());
//        } catch (IOException e) {
//            throw new IllegalArgumentException();
//        }
//    }
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int length = BufferPool.getPageSize();
        int offset = pid.pageNumber() * length;

        RandomAccessFile randomAccessFile;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.seek(offset);
            randomAccessFile.write(page.getPageData());
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList <Page> affectPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++){
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0){
                page.insertTuple(t);
                affectPages.add(page);
                return affectPages;
            }
        }

        HeapPageId pid = new HeapPageId(getId(), numPages());
        HeapPage page = new HeapPage(pid, HeapPage.createEmptyPageData());
        page.insertTuple(t);
        writePage(page);
        affectPages.add(page);
        return affectPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList <Page> affectPages = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        affectPages.add(page);
        return affectPages;
        // not necessary for lab1
    }


    public class HeapFileIterator implements DbFileIterator{
        private int currentPid;
        private Iterator<Tuple> tupleIterator;
        private TransactionId tid;
        public HeapFileIterator(TransactionId tid){ this.tid = tid; }

        @Override
        public void open() throws DbException, TransactionAbortedException{
            currentPid = 0;
            PageId pageId = new HeapPageId(getId(), currentPid);
            tupleIterator = ((HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException{
            // if closed
            if (tupleIterator == null) return false;
//            // has next tuple
//            if (tupleIterator.hasNext()) return true;
//            // has next page
//            if (currentPid < numPages() - 1) return true;
            //Modify from lab1 because of the delete may change the struct so we need to transverse all pages
            while (currentPid < numPages() - 1){
                if (tupleIterator.hasNext()) return true;
                currentPid ++;
                PageId pageId = new HeapPageId(getId(), currentPid);
                tupleIterator = ((HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY)).iterator();
            }

            return tupleIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException{
            if (!hasNext()) throw new NoSuchElementException();
            if (tupleIterator.hasNext()) return tupleIterator.next();
            currentPid = currentPid + 1;
            PageId pageId = new HeapPageId(getId(), currentPid);
            tupleIterator = ((HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY)).iterator();
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close(){
            currentPid = 0;
            tupleIterator = null;
        }
    }

        // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

//    public DbFileIterator iterator(TransactionId tid) {
//        // some code goes here
//        DbFileIterator heapFileIterator = new DbFileIterator() {
//            private int currentPid;
//            private Iterator<Tuple> tupleIterator;
//
//            @Override
//            public void open() throws DbException, TransactionAbortedException {
//                reset();
//            }
//
//            @Override
//            public boolean hasNext() throws DbException, TransactionAbortedException {
//                if (tupleIterator == null) return false;
//                while (!tupleIterator.hasNext() && currentPid < numPages() - 1) {
//                    ++currentPid;
//                    PageId pageId = new HeapPageId(getId(), currentPid);
//                    HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
//                    tupleIterator = currentPage.iterator();
//                }
//                return tupleIterator.hasNext();
//            }
//
//            @Override
//            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//                if (tupleIterator == null) throw new NoSuchElementException();
//
//                Tuple tup = tupleIterator.next();
//
//                return tup;
//            }
//
//            @Override
//            public void rewind() throws DbException, TransactionAbortedException {
//                reset();
//            }
//            private void reset() throws DbException, TransactionAbortedException {
//                currentPid = 0;
//                PageId pageId = new HeapPageId(getId(), currentPid);
//                HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
//                tupleIterator = currentPage.iterator();
//            }
//
//            @Override
//            public void close() {
//                currentPid = 0;
//                tupleIterator = null;
//            }
//        };
//
//        return heapFileIterator;
//    }



}

