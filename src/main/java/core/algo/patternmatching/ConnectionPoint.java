package core.algo.patternmatching;

public class ConnectionPoint {
	private String vertex;
	private int layerID1;
	private int layerID2;
	
	public ConnectionPoint (String vertex, int layerID1, int layerID2) {
		this.vertex = vertex;
		this.layerID1 = layerID1;
		this.layerID2 = layerID2;
	}
	
	public String getVertex() {
		return vertex;
	}
	
	public int getLayerID1() {
		return layerID1;
	}
	
	public int getLayerID2() {
		return layerID2;
	}
	
	public void print() {
		System.out.println("vertex:" + vertex + " layerID1:" + layerID1 + " layerID2:" + layerID2);
	}
}
