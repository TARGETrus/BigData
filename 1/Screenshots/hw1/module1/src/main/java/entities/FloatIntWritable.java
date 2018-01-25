package entities;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom writable implementation.
 * This class's instances are not intended to be used as keys, so no Comparator or CompareTo was implemented.
 */
public class FloatIntWritable implements Writable {

    private float floatValue;
    private int   intValue;

    public FloatIntWritable() {}

    public FloatIntWritable(float floatValue, int intValue) {
        setFloatValue(floatValue);
        setIntValue(intValue);
    }

    /** Common getter for 'floatValue' value */
    public float getFloatValue() {
        return floatValue;
    }

    /** Common setter for 'floatValue' value */
    public void setFloatValue(float floatValue) {
        this.floatValue = floatValue;
    }

    /** Common getter for 'intValue' value */
    public int getIntValue() {
        return intValue;
    }

    /** Common setter for 'intValue' value */
    public void setIntValue(int intValue) {
        this.intValue = intValue;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(floatValue);
        out.writeInt(intValue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        floatValue = in.readFloat();
        intValue   = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FloatIntWritable))
            return false;
        FloatIntWritable other = (FloatIntWritable)o;
        return ((Float.compare(this.floatValue, other.floatValue) == 0) && this.intValue == other.intValue);
    }

    @Override
    public int hashCode() {
        return Float.floatToIntBits(floatValue) + intValue;
    }

    @Override
    public String toString() {
        return floatValue + "," + intValue;
    }
}
