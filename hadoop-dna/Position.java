/**
 * The Position class extends Pattern and it stores the position (index) of a start and end tag (triplet).
 * method getDistance returns the distance between the start protein and the end in our DNA sequence.
 * 
 * @author Vasileios Bardakos
 *
 */
public class Position extends Pattern
{
    private String value;
    private int start;
    private int end;

    public Position(String start, String end) {
        super(start, end);
    }

    public void setValue(String myValue)
    {
        this.value = myValue;
    }

    public int start(int index)
    {
        this.start = value.indexOf(super.getStart(),index);
        return start;
    }

    public int end(int index)
    {
        this.end = value.indexOf(super.getEnd(), index);
        return end;
    }

    public int getDistance()
    {
        return end-start-3;
    }
}
