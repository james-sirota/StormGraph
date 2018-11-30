package metron.graph;

public class TrippleStoreConf {
	
	private String FROM;
	private String VERB;
	private String TO;

	public TrippleStoreConf(String from, String to, String verb)
	{
		FROM=from;
		VERB=verb;
		TO=to;
	}
	
	public String getFrom()
	{
		return FROM;
	}
	
	public String getVerb()
	{
		return VERB;
	}
	
	public String getTo()
	{
		return TO;
	}
}
