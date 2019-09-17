/**
 * The Pattern class stores a start and end tag that you will need to search for in the sample genome data.
 * The tags are provided when you create a new Pattern object and can be accessed later by the getStart() and
 * getEnd() methods. There is also a toString method that provides a String representation of the start and end
 * tags combined. See the SeqMapper.map method for an example of its use.
 * 
 * @author David Cairns
 *
 */
public class Pattern 
{
	private String start;	// The start tag to look for
	private String end;		// The end tag to search for after you have found the start tag

	/**
	 * Create a new Pattern object with the given 'start' and 'end' tags.
	 * 
	 * @param start	The start tag
	 * @param end	The end tag
	 */
	public Pattern(String start, String end)
	{
		this.start = start;
		this.end = end;
	}
	
	/**
	 * Returns a String representation of the two attributes in the form "start...end"
	 */
	public String toString()
	{
		return start + "..." + end;
	}

	/**
	 * 	Returns the start tag
	 */
	public String getStart() { 	return start; }

	/**
	 * Returns the end tag
	 */
	public String getEnd() { return end; }
}
