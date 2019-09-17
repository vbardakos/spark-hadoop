import org.apache.hadoop.io.Text;
import java.util.ArrayList;

public class Position
{
    private ArrayList<Pattern> sequence;
    protected Text value;

    public Position(ArrayList<Pattern> mySequence)
    {
        this.sequence = mySequence;
    }

    public int start(int codonPosition, int indexStart)
    {
        int start = value.toString().indexOf(sequence.get(codonPosition).getStart(),indexStart);
        return start;
    }

    public int end(int codonPosition, int indexEnd)
    {
        int end = value.toString().indexOf(sequence.get(codonPosition).getEnd(), indexEnd);
        return end;
    }
}
