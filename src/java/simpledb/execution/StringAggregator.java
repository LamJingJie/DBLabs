package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbfield; // index of the group-by field
    private Type gbfieldtype; // type of the group-by field
    private int afield; // index of the aggregate field
    private Op what; // aggregation operator
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    HashMap<Field, Integer> groupMap = new HashMap<>();
    public void mergeTupleIntoGroup(Tuple tup) throws IllegalArgumentException{
        // some code goes here
        if(tup.getField(afield) instanceof StringField == false){
            throw new IllegalArgumentException("Field is not a StringField.");
        }
        
        Field grpKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);

        if (what != Op.COUNT){
            // unsupported operation
            throw new IllegalArgumentException("StringAggregator only supports COUNT operation");
        }

        if(!groupMap.containsKey(grpKey)) {
            // first occurance
            groupMap.put(grpKey, 1); // initialize count to 1
        }else{
            // grp already exists, increment count
            int currCnt = groupMap.get(grpKey);
            groupMap.put(grpKey, currCnt + 1); // increment count
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
        for(Map.Entry<Field, Integer> entry: groupMap.entrySet()){
            Field grpKey = entry.getKey();
            Integer val = entry.getValue();
            Tuple newTuple = new Tuple(td); // new row


            // set fields (column) in the new tuple (row) before appending it to the result (table)
            if(gbfield == NO_GROUPING){
                // No grping, just set aggregate value
                newTuple.setField(0, new IntField(val));
            }else{
                // with grping, set both group value and aggregate value
                newTuple.setField(0, grpKey);
                newTuple.setField(1, new IntField(val));
            }

            resTuples.add(newTuple);
        }

        // Up-casting to Iterable
        return new TupleIterator(td, resTuples);
        // throw new UnsupportedOperationException("please implement me for lab2");
    }

}
