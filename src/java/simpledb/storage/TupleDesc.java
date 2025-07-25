package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private final List<TDItem> items = new ArrayList<>();

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("Type and Field arrays must have the same length");
        }

        for(int i =0; i < typeAr.length; i++){
            TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
            this.items.add(tdItem);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        for(int i = 0; i < typeAr.length; i ++){
            TDItem tdItem = new TDItem(typeAr[i], null);
            this.items.add(tdItem);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.items.size()){
            new NoSuchElementException("Invalid index");
        }

        return this.items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.items.size()){
            new NoSuchElementException("Invalid index");
        }

        return this.items.get(i).fieldType;
    }

    // Additional method for easier access to field types for SeqScan
    public Type[] getFieldTypes() {
        Type[] fieldTypes = new Type[this.items.size()];
        for (int i = 0; i< this.items.size(); i++){
            fieldTypes[i] = this.getFieldType(i);
        }
        return fieldTypes;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < this.items.size(); i++){
            if(this.items.get(i).fieldName != null && this.items.get(i).fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException("No field with name: " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalByteSize = 0; // For the total byte size of the tuple
        for (TDItem item : items) {
            totalByteSize += item.fieldType.getLen();
        }
        return totalByteSize;

    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here

        // Create an array of fixed size to hold its respective type and name
        int totalFields = td1.numFields() + td2.numFields();
        Type[] tdFieldType = new Type[totalFields];
        String[] tdFieldName = new String[totalFields];

        int index = 0;
        Iterator<TDItem> td1Iterator = td1.iterator();
        Iterator<TDItem> td2Iterator = td2.iterator();
        while (td1Iterator.hasNext()){
            TDItem item = td1Iterator.next();
            tdFieldType[index] = item.fieldType;
            tdFieldName[index] = item.fieldName;
            index ++;
        }

        while (td2Iterator.hasNext()){
            TDItem item = td2Iterator.next();
            tdFieldType[index] = item.fieldType;
            tdFieldName[index] = item.fieldName;
            index ++;
        }

        TupleDesc merged = new TupleDesc(tdFieldType, tdFieldName);
        return merged;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here

        if (o instanceof TupleDesc == false){
            return false;
        }

        // Downcasting
        TupleDesc other = (TupleDesc) o;
     
        // Check same number of items
        if (this.items.size() == other.items.size()){
            for (int i = 0; i < this.items.size(); i++){
                TDItem thisItem = this.items.get(i);
                TDItem otherItem = other.items.get(i);

                // Check if ith type is not equal
                if(!thisItem.fieldType.equals(otherItem.fieldType)){
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int hash = 7;
        Iterator<TDItem> it = iterator();
        while(it.hasNext()) {
            TDItem item = it.next();
            hash = 31 * hash + (item.fieldType == null ? 0 : item.fieldType.hashCode());
        }
        
        return hash;
        // throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < items.size(); i++) {
            TDItem item = items.get(i);
            sb.append(item.fieldType.toString());

            // Append field name
            if (item.fieldName != null){
                sb.append("(");
                sb.append(item.fieldName);
                sb.append(")");
            }
            
            // If not the last item, append a comma
            if (i != items.size() - 1){
                sb.append(",");
            }
        }
        return sb.toString();
    }
}
