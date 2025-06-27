package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    int gbfield; // index of the group-by field
    Type gbfieldtype; // type of the group-by field
    int afield; // index of the aggregate field
    Op what; // aggregation operator
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    HashMap<Field, int[]> groupMap = new HashMap<>(); // Map to store aggregate values by group
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        // Get the group key based on the gbfield index
        Field grpKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);

        // get val to aggregate
        // Downcasting the field to IntField
        int val = ((IntField) tup.getField(afield)).getValue();

        if(!groupMap.containsKey(grpKey)) {
            // If the group key is not present, initialize it based on the operation
            switch(what){
                case MIN:
                    groupMap.put(grpKey, new int[]{val});
                    break;
                case MAX:
                    groupMap.put(grpKey, new int[]{val});
                    break;
                case SUM:
                    groupMap.put(grpKey, new int[]{val});
                    break;
                case AVG:
                    groupMap.put(grpKey, new int[]{val, 1});
                    break;
                case COUNT:
                    groupMap.put(grpKey, new int[]{1});
                    break;
                default:
                    break;
            }
        }else{
            // If the group key is already present, update the aggregate value
            int[] curr = groupMap.get(grpKey);
            switch(what){
                case MIN:
                    curr[0] = Math.min(curr[0], val);
                    break;
                case MAX:
                    curr[0] = Math.max(curr[0], val);
                    break;
                case SUM:
                    curr[0] += val;
                    break;
                case AVG:
                    curr[0] += val;
                    curr[1] += 1; // Increment number of values to calculate average
                    break;
                case COUNT:
                    curr[0] += 1;
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        
        // Create tuple description based on grouping
        TupleDesc td;
        if(gbfield == NO_GROUPING){
            // No grpings, just return aggregate value
            td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        }else{
            // with grp, return pair (groupVal, aggregateVal)
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{"groupVal", "aggregateVal"});
        }

        // Table that will hold all the result tuples (rows)
        ArrayList<Tuple> resTuples = new ArrayList<>();

        // Process each grp in the groupMap
        for(Map.Entry<Field, int[]> entry: groupMap.entrySet()){
            Field grpKey = entry.getKey();
            int[] val = entry.getValue();
            Tuple newTuple = new Tuple(td); // new row

            int aggregateVal;
            if (what == Op.AVG){
                // For AVG, calculate the average (current sum / count)
                aggregateVal = val[0] / val[1];
            }else{
                // for other operations
                aggregateVal = val[0];
            }

            // set fields (column) in the new tuple (row) before appending it to the result (table)
            if(gbfield == NO_GROUPING){
                // No grping, just set aggregate value
                newTuple.setField(0, new IntField(aggregateVal));
            }else{
                // with grping, set both group value and aggregate value
                newTuple.setField(0, grpKey);
                newTuple.setField(1, new IntField(aggregateVal));
            }

            resTuples.add(newTuple);
        }

        return new TupleIterator(td, resTuples);
        // throw new
        // UnsupportedOperationException("please implement me for lab2");
    }

}
