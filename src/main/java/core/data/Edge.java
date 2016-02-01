package core.data;
public class Edge {
	private String head;
	private String edgeLabel;
	private String tail;
	private String filter = null;
	private boolean valid;
	
	public Edge(String head, String edgeLabel, String tail, String filter) {
		this.head = head;
		this.edgeLabel = edgeLabel;
		this.tail = tail;
		this.filter = filter;
		valid = true;
	}
	
	public Edge(String head, String edgeLabel, String tail) {
		this.head = head;
		this.edgeLabel = edgeLabel;
		this.tail = tail;
		valid = true;
	}	
	
	public void print() {
		System.out.print(toString());
	}
	
	public void invalidateEdge() {
		valid = false;
	}
	
	public void validateEdge() {
		valid = true;
	}
	
	public String getHead() {
		return head;
	}
	
	public String getEdgeLabel() {
		return edgeLabel;
	}
	
	public String getTail() {
		return tail;
	}	

	public boolean hasFilter() {
		return !(filter == null);
	}
	
	public String getFilter() {
		return new String(filter);
	}	
	
	public boolean getValid() {
		return valid;
	}
	
	public String toString() {
		if (filter == null)
			return (head + " " + edgeLabel + " " + tail + "\n");
		else 
			return (head + " " + edgeLabel + " " + tail + " " + filter + "\n");
	}
	
	public boolean equals(Object obj){
		Edge e = (Edge)obj;
		boolean filterEquality = (filter==null && e.filter==null) || 
								 (filter!=null && filter.equals(e.filter));
		return (
				e.head.equals(head) &&
				e.tail.equals(tail) &&
				e.edgeLabel.equals(edgeLabel) &&
				filterEquality
			);
	}
}