package entities;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom writable implementation.
 * Holds Two ints.
 */
public class TwoIntWritable implements WritableComparable<TwoIntWritable> {

    private int intValueOne;
    private int intValueTwo;

    public TwoIntWritable() {}

    public TwoIntWritable(int intValueOne, int intValueTwo) {
        setIntValueOne(intValueOne);
        setIntValueTwo(intValueTwo);
    }

    /** Common getter for 'intValueOne' value */
    public int getIntValueOne() {
        return intValueOne;
    }

    /** Common setter for 'intValueOne' value */
    public void setIntValueOne(int intValueOne) {
        this.intValueOne = intValueOne;
    }

    /** Common getter for 'intValueTwo' value */
    public int getIntValueTwo() {
        return intValueTwo;
    }

    /** Common setter for 'intValueTwo' value */
    public void setIntValueTwo(int intValueTwo) {
        this.intValueTwo = intValueTwo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(intValueOne);
        out.writeInt(intValueTwo);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        intValueOne = in.readInt();
        intValueTwo = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TwoIntWritable))
            return false;
        TwoIntWritable other = (TwoIntWritable)o;
        return ((this.intValueOne == other.intValueOne) && (this.intValueTwo == other.intValueTwo));
    }

    @Override
    public int hashCode() {
        return intValueOne + intValueTwo;
    }

    @Override
    public String toString() {
        return (intValueOne + "," + intValueTwo);
    }

    /**
     * Compares two TwoIntWritable.
     */
    @Override
    public int compareTo(TwoIntWritable o) {

        int thisValue = this.intValueOne;
        int thatValue = o.intValueTwo;

        int res = Integer.compare(thisValue, thatValue);
        if (res != 0)
            return res;

        thisValue = this.intValueTwo;
        thatValue = o.intValueTwo;

        return Integer.compare(thisValue, thatValue);
    }

    /**
     * A RawComparator implementation.
     */
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(TwoIntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);

            int res = Integer.compare(thisValue, thatValue);
            if (res != 0)
                return res;

            thisValue = readInt(b1, s1+4);
            thatValue = readInt(b2, s2+4);

            return (Integer.compare(thisValue, thatValue));
        }
    }

    // register this comparator
    static {
        WritableComparator.define(TwoIntWritable.class, new Comparator());
    }
}
