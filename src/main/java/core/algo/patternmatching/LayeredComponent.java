package core.algo.patternmatching;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import core.data.Edge;

public class LayeredComponent {
	private Vector<HashSet<String>> layers;
	private Vector<Vector<Edge>> edges;
	
	public LayeredComponent() {
		layers = new Vector<HashSet<String>>();
		edges = new Vector<Vector<Edge>>();
	}
	
	private class Vertex {
		private int layerID;
		private String label;
		
		public Vertex(int layerID, String label) {
			this.layerID = layerID;
			this.label = label;
		}
		
		public int getLayerID() {
			return layerID;
		}
		
		public String getLabel() {
			return label;
		}
		
		public boolean theSameAs(Vertex v) {
			if (label.equals(v.getLabel()) && layerID == v.getLayerID() )
				return true;
			else 
				return false;
		}
	}
	
	public Set<String> getLayer(int layerID) {
		if (layerID < layers.size())
			return layers.get(layerID);
		else
			return null;
	}

	public Vector<Edge> getEdgesOfLayer(int layerID) {
		if (layerID < edges.size())
			return edges.get(layerID);
		else
			return null;
	}
	
	public int numberOfLayers() {
		return layers.size();
	}
	
	public boolean hasEdges() {
		return (edges.size() > 0);
	}
	
	public void addVertex(String v, int layerID) {
		HashSet<String> layer;
		
		if (layerID == layers.size()) {
			layer = new HashSet<String>();
			layers.add(layer);	
		} else {
			layer = layers.get(layerID);
		}
		
		layer.add(v);
	}
	
	public void addEdge(Edge e, int layerID) {
		Vector<Edge> layerEdges;
		
		if (layerID == edges.size()) {
			layerEdges = new Vector<Edge>();
			edges.add(layerEdges);	
		} else {
			layerEdges = edges.get(layerID);
		}
		
		layerEdges.add(e);
	}
	
	private int numberOfEdges() {
		int sum = 0;
		for (Vector<Edge> ve : edges)
			sum += ve.size();
		return sum;
	}
	
    // Find the connection points between this LayeredComponent and lc
    public Vector<ConnectionPoint> findConnectionPoints(LayeredComponent lc) {
    	Vector<ConnectionPoint> result = new Vector<ConnectionPoint>();
    	
    	for (int i = 0; i < layers.size(); i++) {
    		HashSet<String> layer = layers.get(i); 
    		for (String v : layer) {
    			for (int j = 0; j < lc.numberOfLayers(); j++) {
    				Set<String> anotherLayer = lc.getLayer(j);
                    
    				for (String v2 : anotherLayer) {
    					if (v.equals(v2))
    						result.add(new ConnectionPoint(v, i, j)); 
    				}
    			}
    		}
    	}
    	
    	return result;
    }
    
    public BushyDecomposition BushyDecomposition() {
    	Vector<String> result; 
    	
    	for (int i = layers.size() - 1; i >= 0; i--) {
    		HashSet<String> layer = layers.get(i); 
    		for (String v : layer) {
    			Vector<String> heads = headOfIncomingEdges(i, v);
    			
    			if (heads.size() > 1) {
    				//System.out.println("Partition vertex: " + v);
    				result = doPartition(i, v, heads);
    				
    				if (result != null)
    					return new BushyDecomposition(result, v);
    			}
    		}
    	}
    	
    	//System.out.println("---------------------AAAAAAAAAA-------------------------");
    	
    	return null;
    }
    
    private Vector<String> doPartition(int layerID, String v, Vector<String> heads) {
    	Vector<String> result = new Vector<String>();
    	Vector<Set<Edge>> partitioning = new Vector<Set<Edge>>(); 
     
    	Set<Edge> tmp = reachableDAG(layerID, v);
    	if (tmp.size() != 0)
    		partitioning.add(reachableDAG(layerID, v));
    	//System.out.println("Size:" + partitioning.get(0).size());
    	//printASetOfEdges(partitioning.get(0));
    	
    	Vertex obstacle = new Vertex(layerID, v);
    	
    	for (String head : heads) {
    		partitioning.add(reachableDAG(new Vertex(layerID - 1, head), obstacle));
    		//System.out.println("Size:" + partitioning.get(partitioning.size() - 1).size());
    		//printASetOfEdges(partitioning.get(partitioning.size() - 1));
    	}
    	
    	// Check whether variable partitioning is a partition of edges of the LayeredComponenet
    	// 1. Sum of sizes of all sets should be the same as the size of edges
    	int sum = 0;
    	for (Set<Edge> s : partitioning) {
    		sum += s.size();
    	}
    	if (sum != numberOfEdges()) {
    		//System.out.println("Sum:" + sum);
    		return null;
    	}
    	
    	// 2. All sets are non-overlapping.
    	if (!intersectionIsEmpty(partitioning))
    		return null;
    	
    	//System.out.println("dddddddddddd " + partitioning.size());
    	
    	for (Set<Edge> s : partitioning) {
    		String[] edges = new String[s.size()];
    		//System.out.println("---------------------Start of a partition----------------");
    		int i = 0;
    		for (Edge e : s) {
    			//e.print();
    			edges[i] = e.toString();
    			//System.out.println("dddddddddddd " + edges[i]);
    			i++;
    		}
    		Arrays.sort(edges);
    		
    		String subquery = "";
    		for (String e : edges) {
    			subquery += e;
    		}
    		
    		//System.out.println("dddddddddddd " + subquery);
    		
    		// remove the last new line symbol
    		//subquery = subquery.substring(0, subquery.length() - 1);
    		result.add(subquery);
    	}
    		
    	return result;
    }
    
    @SuppressWarnings("unused")
	private void printASetOfEdges (Set<Edge> s) {
    	for (Edge e : s)
    		e.print();
    }
    
    private boolean intersectionIsEmpty(Vector<Set<Edge>> partitioning) {
    	for (int i = 0; i < partitioning.size() - 1; i++) {
    		for (int j = i + 1; j < partitioning.size(); j++) {
    			if (!intersectionIsEmpty(partitioning.get(i), partitioning.get(j)))
    				return false;
    		}
    	}
    	return true;
    }
    
    private boolean intersectionIsEmpty(Set<Edge> s1, Set<Edge> s2) {
    	for (Edge e1 : s1) {
    		for (Edge e2 : s2) {
    			if (e1 == e2)
    				return false;
    		}
    	}
    	
    	return true;
    }
    
    // Return the forward and backward reachable DAG of start without going through obstacle 
    private Set<Edge> reachableDAG(Vertex start, Vertex obstacle) {
    	HashSet<Edge> result = new HashSet<Edge>();
    	Vector<Vertex> currentVertexes = new Vector<Vertex>();
    	int lastResultNumber = 0;
    	currentVertexes.add(start);
    	
    	do {
    		//System.out.println("size of currentVertexes:" + currentVertexes.size());
    		
    		Vector<Vertex> newVertexes = new Vector<Vertex>();
    		lastResultNumber = result.size();
    		
    		for (Vertex v : currentVertexes) {
    			//System.out.println("layerID:" + v.layerID + "\tlabel:" + v.label);
    			addIncomingEdges(v, obstacle, result, newVertexes);
    			addOutgoingEdges(v, obstacle, result, newVertexes);
    		}
    		
    		//System.out.println("size of newVertexes:" + newVertexes.size());
    		
    		currentVertexes = newVertexes;	
    		//System.out.println("lastResultNumber:" + lastResultNumber);
    		//System.out.println("result.size():" + result.size());
    	} while (lastResultNumber != result.size());
    	
		return result;
    }

	// Add the incoming edges of v to result
	// Add the heads of incoming edges of v to newVertexes
    private void addIncomingEdges(Vertex v, Vertex obstacle, Set<Edge> result, Vector<Vertex> newVertexes) {
    	// Vertexes in the top layer have no incoming edges
    	if (v.getLayerID() == 0)
    		return;
    	
    	// Fetch the edges from the layer above
    	int edgeLayerID = v.getLayerID() - 1;
    	Vector<Edge> es = edges.get(edgeLayerID);
    	
    	for (Edge e : es) {
    		// Finds an incoming edge of v
    		if (e.getTail().equals(v.getLabel())) {
    			Vertex tmp = new Vertex(edgeLayerID, e.getHead());
    			// If the incoming edge does not come from obstacle, add the edge and its head
    			if (!tmp.theSameAs(obstacle)) {
    				result.add(e);
    				newVertexes.add(tmp);
    				//System.out.println("TEMP is added: layerID:" + tmp.layerID + "\tlabel:" + tmp.label);
    			}
    		}
    	}	
    }

    private void addOutgoingEdges(Vertex v, Vertex obstacle, Set<Edge> result, Vector<Vertex> newVertexes) {
    	// Vertexes in the bottom layer have no outgoing edges
    	if (v.getLayerID() == layers.size() - 1)
    		return;
    	
    	// Fetch the edges from the layer of v
    	int edgeLayerID = v.getLayerID();
    	Vector<Edge> es = edges.get(edgeLayerID);
    	
    	for (Edge e : es) {
    		if (e.getHead().equals(v.getLabel())) {
    			// The tail vertex is at the layer below
    			Vertex tmp = new Vertex(edgeLayerID + 1, e.getTail());
    			// Add the edge to result
    			result.add(e);
    			// If the outgoing edge does not go to obstacle, add its tail
    			if (!tmp.theSameAs(obstacle)) {
    				newVertexes.add(tmp);
    				//System.out.println("TEMP is added: layerID:" + tmp.layerID + "\tlabel:" + tmp.label);
    			}
    		}
    	}
    }
    
    //
    private Set<Edge> reachableDAG (int layerID, String v) {
    	HashSet<Edge> result = new HashSet<Edge>();
    	
    	Vector<String> currentNeighbours = new Vector<String>();
    	currentNeighbours.add(v);
    	
    	Vector<String> newNeighbours = new Vector<String>();
    	
    	int currentLayer = layerID;
    	
    	// The reason to use (layers.size() - 1) is that the last layer does not have any edges
    	while (!currentNeighbours.isEmpty() && currentLayer < layers.size() - 1) {
    		for (String neighbour : currentNeighbours) {
    			Vector<Edge> es = edges.get(currentLayer);
    			for (Edge e : es) {
    				if (neighbour.equals(e.getHead())) {
    					result.add(e);
    					newNeighbours.add(e.getTail());
    				}
    			}
    		}
    		
    		currentNeighbours = newNeighbours;
    		currentLayer++;
    		newNeighbours = new Vector<String>();
    	}
    	 
    	return result;
    }
    
    // Return heads of incoming edges of a vertex v in layer layerID
    private Vector<String> headOfIncomingEdges(int layerID, String v) {
    	Vector<String> result = new Vector<String>();
    	
    	if (layerID == 0)
    		return result;
    		
    	Vector<Edge> es = edges.get(layerID - 1);
    	
    	for (Edge e : es) {
    		if (e.getTail().equals(v))
    			result.add(e.getHead());
    	}
    	
    	return result;
    }
    
    // Used by mergeLayeredComponents
    private static boolean mergerTwoComponents(Vector<LayeredComponent> lcs) {
    	boolean hasMerged = false;
    	
    	for (int i = 0; i < lcs.size();i++) {
    		for (int j = i + 1; j < lcs.size();j++) {
    			LayeredComponent lc1 = lcs.get(i), lc2 = lcs.get(j);
    			
    			Vector<ConnectionPoint> cps = lc1.findConnectionPoints(lc2);
    			
    			if (cps.size() > 0) {
    	    		LayeredComponent newLC = mergeTwoLayeredComponents(cps.get(0), lc1, lc2);
    	    		lcs.remove(lc1);
    	    		lcs.remove(lc2);
    	    		lcs.add(newLC);
    	    		hasMerged = true;
    			}
    		}
    	}    	
    	
    	return hasMerged;
    }
 
    // Given a vector of LayeredComponent, merge them into one LayeredComponent
    public static LayeredComponent mergeLayeredComponents(Vector<LayeredComponent> lcs) {  
    	while (lcs.size() > 1) {
    		boolean hasMerged = mergerTwoComponents(lcs);
    		// If the layer components are not connected, the intermediate result tends to be huge.
    		// We simply ignore them
    		if (!hasMerged) {
    			return null;
    		}
    	}
    	
    	return lcs.get(0);
    }
/*    
    // Given a vector of LayeredComponent, merge them into one LayeredComponent.
    public static LayeredComponent mergeLayeredComponents(Vector<LayeredComponent> lcs) {    	
    	while (lcs.size() > 1) {
			int lc1ID = 0, lc2ID = 0;
			Vector<ConnectionPoint> cps = null;
			LayeredComponent lc1 = null, lc2 = null;
	    	Random generator = new Random();
	    	boolean findConnectionPoint = false;
	    	
    		while (findConnectionPoint == false) {
    			lc1ID = 0;
    			lc2ID = 1 + generator.nextInt(lcs.size() - 1);
    			lc1 = lcs.get(lc1ID);
    			lc2 = lcs.get(lc2ID);
    			
    			cps = lc1.findConnectionPoints(lc2);
    			findConnectionPoint = (cps.size() > 0);
    			System.out.println("dddddddddddd");
    		}
    		
    		// TODO: Is it a good idea to pick the first connection point?
    		LayeredComponent newLC = mergeTwoLayeredComponents(cps.get(0), lc1, lc2);
    		
    		lcs.remove(lc1);
    		lcs.remove(lc2);
    		lcs.add(newLC);
    	} 
    	
    	return lcs.get(0);
    }
*/
    
    
    // Connect two layered components with a connection point.
    public static LayeredComponent mergeTwoLayeredComponents(ConnectionPoint cp, LayeredComponent lc1, LayeredComponent lc2) {
    	LayeredComponent result = new LayeredComponent();
    	LayeredComponent first, second;
    	
    	int diff = cp.getLayerID1() - cp.getLayerID2();
    	
    	if (diff >= 0) {
    		first = lc1;
    		second = lc2;
    	}
    	else {
    		first = lc2;
    		second = lc1;
    	}
    	
    	addLayerComponent(result, first, 0);
    	addLayerComponent(result, second, Math.abs(diff));
    	
    	return result;
    }
    
    // Adds a LayeredComponent lc1 to another lc2
    // such that lc2.layer0 is added to lc1.firstLayer, lc2.layer1 is added to lc1.(firstLayer+1) and so on.
    public static void addLayerComponent(LayeredComponent lc1, LayeredComponent lc2, int firstLayer) {
    	int layerInlc1 = firstLayer;
    	for (int i = 0; i < lc2.numberOfLayers(); i++, layerInlc1++) {
    		Set<String> tmp = lc2.getLayer(i);
    		for (String v : tmp)
    			lc1.addVertex(v, layerInlc1);
    	}
	
    	layerInlc1 = firstLayer;
    	for (int i = 0; i < lc2.numberOfLayers() - 1; i++, layerInlc1++) {
    		Vector<Edge> tmp = lc2.getEdgesOfLayer(i);
    		for (Edge e : tmp)
    			lc1.addEdge(e, layerInlc1);
    	}    	
    }  
    
    // A shallow comparison between two LayeredComponents
    public static boolean theSame(LayeredComponent lc1, LayeredComponent lc2) {
    	if (lc1.numberOfLayers() != lc2.numberOfLayers()) {
    		return false;
    	}
    
    	for (int i = 0; i < lc1.numberOfLayers(); i++) {
    		if (lc1.getLayer(i).size() != lc2.getLayer(i).size())
    			return false;
    	}
    	
    	return true;
    }
    
	public void print() {
		System.out.println("-----------------------------start of a component");
		for (int i = 0; i < layers.size() - 1; i++) {
			HashSet<String> layer = layers.get(i);
			Vector<Edge> layerEdges = edges.get(i);
			
			System.out.println("-------------layer " + i);
			for (String v: layer)
				System.out.print(v + " ");
			System.out.println();
			
			System.out.println("-------------edges for layer " + i);
			for (Edge e: layerEdges)
				e.print();			
		}
		
		HashSet<String> layer = layers.get(layers.size() - 1);
		System.out.println("-------------layer " + (layers.size() - 1));
		for (String v: layer)
			System.out.print(v + " ");
		System.out.println();
		
		System.out.println("-----------------------------end of a component");
	}
}
