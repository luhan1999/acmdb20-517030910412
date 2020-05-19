//package simpledb;
//
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
///**
// * TableStats represents statistics (e.g., histograms) about base tables in a
// * query.
// *
// * This class is not needed in implementing lab1, lab2 and lab3.
// */
//public class TableStats {
//
//    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();
//
//    static final int IOCOSTPERPAGE = 1000;
//
//    public static TableStats getTableStats(String tablename) {
//        return statsMap.get(tablename);
//    }
//
//    public static void setTableStats(String tablename, TableStats stats) {
//        statsMap.put(tablename, stats);
//    }
//
//    public static void setStatsMap(HashMap<String,TableStats> s)
//    {
//        try {
//            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
//            statsMapF.setAccessible(true);
//            statsMapF.set(null, s);
//        } catch (NoSuchFieldException e) {
//            e.printStackTrace();
//        } catch (SecurityException e) {
//            e.printStackTrace();
//        } catch (IllegalArgumentException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    public static Map<String, TableStats> getStatsMap() {
//        return statsMap;
//    }
//
//    public static void computeStatistics() {
//        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();
//
//        System.out.println("Computing table stats.");
//        while (tableIt.hasNext()) {
//            int tableid = tableIt.next();
//            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
//            setTableStats(Database.getCatalog().getTableName(tableid), s);
//        }
//        System.out.println("Done.");
//    }
//
//    /**
//     * Number of bins for the histogram. Feel free to increase this value over
//     * 100, though our tests assume that you have at least 100 bins in your
//     * histograms.
//     */
//    static final int NUM_HIST_BINS = 100;
//
//    private int numTuples;
//    private int numFields;
//    private int numPages;
//    private int iocostperpage;
//    private IntHistogram [] fieldToIntHistogram;
//    private StringHistogram [] fieldToStringHistogram;
//    /**
//     * Create a new TableStats object, that keeps track of statistics on each
//     * column of a table
//     *
//     * @param tableid
//     *            The table over which to compute statistics
//     * @param ioCostPerPage
//     *            The cost per page of IO. This doesn't differentiate between
//     *            sequential-scan IO and disk seeks.
//     */
//    public TableStats(int tableid, int ioCostPerPage) {
//        // For this function, you'll have to get the
//        // DbFile for the table in question,
//        // then scan through its tuples and calculate
//        // the values that you need.
//        // You should try to do this reasonably efficiently, but you don't
//        // necessarily have to (for example) do everything
//        // in a single scan of the table.
//        // some code goes here
//        this.iocostperpage = ioCostPerPage;
//
//        TransactionId tid = new TransactionId();
//        SeqScan scan = new SeqScan(tid, tableid, "");
//        this.numFields = scan.getTupleDesc().numFields();
//        this.numPages = ((HeapFile) Database.getCatalog().getDatabaseFile(tableid)).numPages();
//        int maxs[] = new int[this.numFields];
//        int mins[] = new int[this.numFields];
//        fieldToIntHistogram = new IntHistogram[this.numFields];
//        fieldToStringHistogram = new StringHistogram[this.numFields];
//
//        for (int i = 0; i < numFields; i++) {
//            maxs[i] = Integer.MIN_VALUE;
//            mins[i] = Integer.MAX_VALUE;
//        }
//
//        try {
//            scan.open();
//            while (scan.hasNext()){
//                this.numTuples += 1;
//                Tuple tuple = scan.next();
//                for (int i = 0; i < numFields; i++){
//                    if (tuple.getField(i).getType() == Type.INT_TYPE) {
//                        int value = ((IntField) tuple.getField(i)).getValue();
//                        mins[i] = Integer.min(mins[i], value);
//                        maxs[i] = Integer.max(maxs[i], value);
//                    }
//                }
//            }
//
//            for (int i = 0; i < this.numFields; i++){
//                if (scan.getTupleDesc().getFieldType(i) == Type.INT_TYPE){
//                    fieldToIntHistogram[i] = new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
//                } else {
//                    fieldToStringHistogram[i] = new StringHistogram(NUM_HIST_BINS);
//                }
//            }
//
//            scan.rewind();
//            while (scan.hasNext()){
//                Tuple tuple = scan.next();
//                for (int i = 0; i < numFields; i++) {
//                    if (tuple.getField(i).getType() == Type.INT_TYPE){
//                        int value = ((IntField) tuple.getField(i)).getValue();
//                        fieldToIntHistogram[i].addValue(value);
//                    } else {
//                        String value = ((StringField) tuple.getField(i)).getValue();
//                        fieldToStringHistogram[i].addValue(value);
//                    }
//                }
//            }
//        } catch (TransactionAbortedException e){
//            e.printStackTrace();
//        } catch (DbException e){
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * Estimates the cost of sequentially scanning the file, given that the cost
//     * to read a page is costPerPageIO. You can assume that there are no seeks
//     * and that no pages are in the buffer pool.
//     *
//     * Also, assume that your hard drive can only read entire pages at once, so
//     * if the last page of the table only has one tuple on it, it's just as
//     * expensive to read as a full page. (Most real hard drives can't
//     * efficiently address regions smaller than a page at a time.)
//     *
//     * @return The estimated cost of scanning the table.
//     */
//    public double estimateScanCost() {
//        // some code goes here
//        return  numPages * iocostperpage;
//    }
//
//    /**
//     * This method returns the number of tuples in the relation, given that a
//     * predicate with selectivity selectivityFactor is applied.
//     *
//     * @param selectivityFactor
//     *            The selectivity of any predicates over the table
//     * @return The estimated cardinality of the scan with the specified
//     *         selectivityFactor
//     */
//    public int estimateTableCardinality(double selectivityFactor) {
//        // some code goes here
//        return (int) Math.ceil(selectivityFactor * numTuples);
//    }
//
//    /**
//     * The average selectivity of the field under op.
//     * @param field
//     *        the index of the field
//     * @param op
//     *        the operator in the predicate
//     * The semantic of the method is that, given the table, and then given a
//     * tuple, of which we do not know the value of the field, return the
//     * expected selectivity. You may estimate this value from the histograms.
//     * */
//    public double avgSelectivity(int field, Predicate.Op op) {
//        // some code goes here
//        return 1.0;
//    }
//
//    /**
//     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
//     * table.
//     *
//     * @param field
//     *            The field over which the predicate ranges
//     * @param op
//     *            The logical operation in the predicate
//     * @param constant
//     *            The value against which the field is compared
//     * @return The estimated selectivity (fraction of tuples that satisfy) the
//     *         predicate
//     */
//    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
//        // some code goes here
//        if (constant.getType() == Type.INT_TYPE){
//            int value = ((IntField) constant).getValue();
//            return fieldToIntHistogram[field].estimateSelectivity(op, value);
//        } else {
//            String value = ((StringField) constant).getValue();
//            return fieldToStringHistogram[field].estimateSelectivity(op, value);
//        }
//
//    }
//
//    /**
//     * return the total number of tuples in this table
//     * */
//    public int totalTuples() {
//        // some code goes here
//        return numTuples;
//    }
//
//}
//TODO
package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1, lab2 and lab3.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    private ConcurrentHashMap<Integer, IntHistogram> fieldToIntHistogram;
    private ConcurrentHashMap<Integer, StringHistogram> fieldToStringHistogram;
    private int tableId, ioCostPerPage;
    private TupleDesc schema;
    private int totalTuples = 0;
    private HeapFile table;


    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.fieldToIntHistogram = new ConcurrentHashMap<>();
        this.fieldToStringHistogram = new ConcurrentHashMap<>();
        this.table = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        this.schema = table.getTupleDesc();
        Transaction transaction = new Transaction();
        createHistograms(table.iterator(transaction.getId()));

    }

    private void createHistograms(DbFileIterator tupleIter) {
        try {
            tupleIter.open();
            int mins[] = new int[schema.numFields()], maxs[] = new int[schema.numFields()];
            for (int field = 0; field < schema.numFields(); ++field) {
                mins[field] = Integer.MAX_VALUE;
                maxs[field] = Integer.MIN_VALUE;
            }
            while (tupleIter.hasNext()) {
                ++totalTuples;
                Tuple tuple = tupleIter.next();
                for (int field = 0; field < schema.numFields(); ++field) {
                    if (schema.getFieldType(field).equals(Type.INT_TYPE)) {
                        int value = ((IntField) tuple.getField(field)).getValue();
                        mins[field] = Integer.min(mins[field], value);
                        maxs[field] = Integer.max(maxs[field], value);
                    }
                }
            }
            for (int field = 0; field < schema.numFields(); ++field) {
                if (schema.getFieldType(field).equals(Type.INT_TYPE)) {
                    fieldToIntHistogram.put(field, new IntHistogram(NUM_HIST_BINS, mins[field], maxs[field]));
                } else if (schema.getFieldType(field).equals(Type.STRING_TYPE)) {
                    fieldToStringHistogram.put(field, new StringHistogram(NUM_HIST_BINS));
                }
            }
            tupleIter.rewind();
            while (tupleIter.hasNext()) {
                Tuple tuple = tupleIter.next();
                for (int field = 0; field < schema.numFields(); ++field) {
                    if (schema.getFieldType(field).equals(Type.INT_TYPE)) {
                        int value = ((IntField) tuple.getField(field)).getValue();
                        fieldToIntHistogram.get(field).addValue(value);
                    } else if (schema.getFieldType(field).equals(Type.STRING_TYPE)) {
                        String value = ((StringField) tuple.getField(field)).getValue();
                        fieldToStringHistogram.get(field).addValue(value);
                    }
                }
            }
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return table.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) Math.ceil(totalTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (fieldToIntHistogram.containsKey(field)) {
            int value = ((IntField) constant).getValue();
            IntHistogram histogram = fieldToIntHistogram.get(field);
            return histogram.estimateSelectivity(op, value);
        } else if (fieldToStringHistogram.containsKey(field)) {
            String value = ((StringField) constant).getValue();
            StringHistogram histogram = fieldToStringHistogram.get(field);
            return histogram.estimateSelectivity(op, value);
        }
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return this.totalTuples;
    }

}