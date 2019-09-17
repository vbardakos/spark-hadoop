import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The TwoIntWritable class replaces apache's standard Writable and it aims to fill the
 * needs of our task. It reads two integers and it writes a String with both integers.
 * Input:
 *      - distance  See Position class; getDistance method.
 *      - count     A simple counter; essential to compute the average distance.
 * Output: "distance / count"
 * 
 * @author Vasileios Bardakos
 *
 */
public class TwoIntWritable implements Writable
{
    private IntWritable distance;
    private IntWritable count;

    public TwoIntWritable()
    {
        distance = new IntWritable();
        count = new IntWritable(1);
    }

    public IntWritable getDistance()
    {
        return distance;
    }

    public IntWritable getCount()
    {
        return count;
    }

    public void setDistance(int distance)
    {
        IntWritable writableDistance = new IntWritable(distance);
        this.distance = writableDistance;
    }

    public void setCount(int count)
    {
        IntWritable writableCount = new IntWritable(count);
        this.count = writableCount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        distance.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        distance.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public String toString()
    {
        return distance.toString() + "/" + count.toString();
    }
}
