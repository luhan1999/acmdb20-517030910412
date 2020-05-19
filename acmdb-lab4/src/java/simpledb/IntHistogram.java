package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    private int numBuckets;
    private int min;
    private int max;
    private int width;
    private int lastBucketWidth;
    private int[] buckets;
    private int numTuples;
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        //If the bucket larger than the size, we should avoid width = 0
        if (max - min + 1 < buckets) buckets = max - min + 1;
        this.numBuckets = buckets;
        this.min = min;
        this.max = max;
        this.width = (max - min + 1) / buckets;
        this.lastBucketWidth = (max - min + 1) - this.width * (buckets - 1);
        this.buckets = new int[numBuckets];
        this.numTuples = 0;
    }

    private int getBucketId(int value){
        if (value - min + 1 <= this.width * (this.numBuckets - 1))
            return (value - min) / this.width;
        else return this.numBuckets - 1;
    }

    private int getBucketWidth(int BuckedId){
        if (BuckedId == this.numBuckets - 1) return this.lastBucketWidth;
        else return this.width;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int BuckedId = getBucketId(v);
        buckets[BuckedId] ++;
        numTuples ++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int buckedId = getBucketId(v);
        double numSatisfyTuples = 0.0;
        switch (op){
            case EQUALS:
                if (v < min || v > max) return 0.0;
                    numSatisfyTuples = buckets[buckedId] * 1.0 / getBucketWidth(buckedId);
                    break;
            case NOT_EQUALS:
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case LESS_THAN:
                if (v <= min) return 0.0;
                if (v > max) return 1.0;
                for (int i = 0; i < buckedId; i++){
                    numSatisfyTuples = numSatisfyTuples + buckets[i];
                }
                int left = buckedId * this.width + 1;
                numSatisfyTuples += buckets[buckedId] * (v - left) * 1.0 / getBucketWidth(buckedId);
                break;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            case GREATER_THAN:
                if (v < min) return 1.0;
                if (v >= max) return 0.0;
                for (int i = buckedId + 1; i < numBuckets; i++){
                    numSatisfyTuples += buckets[i];
                }
                int right = buckedId * this.width + getBucketWidth(buckedId);
                numSatisfyTuples += buckets[buckedId] * (right - v) * 1.0 / getBucketWidth(buckedId);
                break;
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
        }


        return numSatisfyTuples / numTuples;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
