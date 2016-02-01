package core.data;

/**
 * This class defines the partitioning of the graph.
 * It is used when calculating the query cost.
 * 
 * @author alekh
 *
 */
public abstract class Partitioning {
	
	// partition by source vertex
	public static class SourceVertexPartitioning extends Partitioning {
		public long getKey(Edge e){
			return e.getHead().hashCode();
		}
	}
	
	// partition by destination vertex
	public static class DestinationVertexPartitioning extends Partitioning {
		public long getKey(Edge e){
			return e.getTail().hashCode();
		}
	}
	
	public static class TwoDPartitioning extends Partitioning {
		// partition by both source and destination vertices
		public long getKey(Edge e){
			return 0;	//TODO
		}
	}
	
	
	public abstract long getKey(Edge e);
	
}
