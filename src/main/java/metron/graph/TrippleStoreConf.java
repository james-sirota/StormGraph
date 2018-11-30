package metron.graph;

public class TrippleStoreConf {
	
	private String FROM;
	private String FROM_NODE_TYPE;
	private String VERB;
	private String TO;
	private String TO_NODE_TYPE;

	public TrippleStoreConf(String from, String to, String verb, String fromNodeType, String toNodeType)
	{
		FROM=from;
		FROM_NODE_TYPE = fromNodeType;
		VERB=verb;
		TO=to;
		TO_NODE_TYPE = toNodeType;
	}
	
	public String getFrom()
	{
		return FROM;
	}
	public String getFromNodeType()
	{
		return FROM_NODE_TYPE;
	}
	
	public String getVerb()
	{
		return VERB;
	}
	
	public String getTo()
	{
		return TO;
	}
	
	public String getToNodeType()
	{
		return TO_NODE_TYPE;
	}
	
}
