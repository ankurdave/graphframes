package core.query.plan.execute;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Vector;

import core.query.plan.Action;
import core.query.plan.ActionType;
import core.query.plan.QueryPlanNode;
import core.query.plan.QueryPlanNodeType;

public class PlanExecutor {
  public static final String JOIN_PROGRAM = System.getProperty("user.dir") + "/" + "join.py";
  public static final String LOCAL_JOIN = "LOCAL_JOIN";
  public static final String LOCAL_JOIN_WITH_A_FILE = "LOCAL_JOIN_WITH_A_FILE";
  public static final String BROADCAST = "BROADCAST";
  public static final String HASH = "HASH";
  public static final String MOVE = "MOVE";
  public static final String IMPORT = "IMPORT";
  public static final String DELETE_TABLES = "DELETE_TABLES";

  private final int numberOfMachines;
  private final String hostFile;

  private final String DB_NAME;
  private String DB_PORT;
  private String CODE_DIR;
  private String TMP_DIR;
  private String OUTPUT_DIR;
  private String PSQL_DIR;

  public PlanExecutor(int numberOfMachines, String hostFile, String databaseName, String configuration) {
    this.numberOfMachines = numberOfMachines;
    this.hostFile = hostFile;
    this.DB_NAME = databaseName;

    readConfiguration(configuration);
  }

  private void readConfiguration(String configuration) {
    try {
      FileInputStream fstream = new FileInputStream(configuration);
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));

      String strLine;
      while ((strLine = br.readLine()) != null)   {
        String[] splits = strLine.split(" ");
        if (splits[0].equals("DB_PORT"))
        	DB_PORT = splits[1];
        else if (splits[0].equals("CODE_DIR"))
        	CODE_DIR = splits[1];
        else if (splits[0].equals("TMP_DIR"))
        	TMP_DIR = splits[1];
        else if (splits[0].equals("OUTPUT_DIR"))
        	OUTPUT_DIR = splits[1];
        else if (splits[0].equals("PSQL_DIR"))
        	PSQL_DIR = splits[1];
        else 
        	System.err.println("Unknown configuration parameter!");
      }
      in.close();
    }
    catch (Exception e){
      System.err.println("Error: " + e.getMessage());
    }
  }
  
  public void execute(QueryPlanNode currentNode) {
    if (currentNode == null)
      return ;

    Vector<QueryPlanNode> children = currentNode.getAllChildren();

    if (children.size() == 0) {
      if (currentNode.getType() == QueryPlanNodeType.LOCAL_EXECUTION)
      	doLocalJoin(currentNode);
      
      return ;
    }

    Vector<Action> actions = currentNode.getActions();
    for (int i = 0; i < children.size(); i++){
      if (actions.get(i).getType() != ActionType.NO)	
        execute(children.get(i));
    }
    
    doDistributedJoin(currentNode);
  }
  
  /*
   * 1. Hash/Broadcast the intermediate results 
   * 2. Import the intermediate results to postgres
   * 3. Perform the join
   * 4. Delete the intermediate tables 
   */
  private void doDistributedJoin(QueryPlanNode node) {
      Vector<QueryPlanNode> children = node.getAllChildren();
	  Vector<Action> actions = node.getActions();
	  Process p = null;
	  long totalTime = System.currentTimeMillis();
	  long time = System.currentTimeMillis();
	  System.out.println("--------------------" + node.getType() + "--------------------");
	  try{
		  // 1. Hash/Broadcast the intermediate results
		  for (int i = 0; i < children.size(); i++) {
			  Action action = actions.get(i);
			  if (action.getType() == ActionType.NO)
				  continue;
			  QueryPlanNode child = children.get(i);
			  String nodeID = child.getNodeID();
			  if (action.getType() == ActionType.HASH) {
				  String hashColumn = child.getIndexOfVertex(action.getHashVertex()) + "";
				  String prefix = OUTPUT_DIR + nodeID;
				  p = Runtime.getRuntime().exec(new String[] {"python", JOIN_PROGRAM, HASH, hostFile, CODE_DIR, prefix, numberOfMachines + "", hashColumn});
				  p.waitFor();
				  
				  System.out.println("\tHash:\t\t\t\t" +  ((double)(System.currentTimeMillis() - time) / 1000) + "");
				  /*
                  BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                  p.waitFor();
                  while (br.ready())
                	  System.out.println(br.readLine());
				   */
				  time = System.currentTimeMillis();
				  p = Runtime.getRuntime().exec(new String[] {"python", JOIN_PROGRAM, MOVE, hostFile, CODE_DIR, prefix, TMP_DIR});
                  p.waitFor();
                  
                  /*
                  BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream())); //p.getErrorStream()
                  //BufferedReader br = new BufferedReader(new InputStreamReader(p.g)); 
                  p.waitFor();
                  while (br.ready())
                	  System.out.println(br.readLine());
                  */
                  System.out.println("\tMove:\t\t\t\t" +  ((double)(System.currentTimeMillis() - time) / 1000) + "");
			  }
			  else if (action.getType() == ActionType.BROADCAST) {
				  p = Runtime.getRuntime().exec(new String[] {"python", JOIN_PROGRAM, BROADCAST, hostFile, OUTPUT_DIR + nodeID, TMP_DIR});
				  p.waitFor();
				  
				  System.out.println("\tBroadcast:\t\t\t" +  ((double)(System.currentTimeMillis() - time) / 1000) + "");
			  }
		  }
		  // 2. Import the intermediate results to postgres
		  time = System.currentTimeMillis();
		  for (int i = 0; i < children.size(); i++) {
			  if (actions.get(i).getType() == ActionType.NO)
				  continue;			  
			  QueryPlanNode child = children.get(i);
			  p = Runtime.getRuntime().exec(new String[] {"python", JOIN_PROGRAM, IMPORT, hostFile, CODE_DIR, TMP_DIR, PSQL_DIR, DB_PORT, DB_NAME,
	                    child.getNodeID(), "\"" + child.getSQLSchema().replaceAll("tmp", child.getNodeID()) + "\""});
			  p.waitFor(); 
		  }
		  System.out.println("\tImport:\t\t\t\t" +  ((double)(System.currentTimeMillis() - time) / 1000) + "");
		  // 3. Perform the join
		  time = System.currentTimeMillis();
		  Vector<String> inputTables = new Vector<String>();
		  for (QueryPlanNode child : children)
			  inputTables.add(child.getNodeID());
		  p = Runtime.getRuntime().exec(new String[] {"python", JOIN_PROGRAM, LOCAL_JOIN, hostFile, PSQL_DIR, DB_PORT, DB_NAME, OUTPUT_DIR + node.getNodeID(), node.getJoinQuery(inputTables)});
		  p.waitFor(); 
		  System.out.println("\tJoin and export:\t\t" +  ((double)(System.currentTimeMillis() - time) / 1000) + "");
		  // 4. Delete the intermediate tables
		  time = System.currentTimeMillis();
		  String tables = "";
		  ///////////////////////////////////////////////
		  // This is a hack for linear shared plan
		  if (node.getNodeID().startsWith("D")){
			  System.out.println(((double)(System.currentTimeMillis() - totalTime) / 1000) + "");
			  return;
		  }
		  //////////////////////////////////////////////
		  for (int i = 0; i < children.size(); i++) {
			  if (actions.get(i).getType() == ActionType.NO)
				  continue;
			  tables += children.get(i).getNodeID() + " ";
		  }
	      p = Runtime.getRuntime().exec(new String[] {"python", JOIN_PROGRAM, DELETE_TABLES, hostFile, PSQL_DIR, DB_PORT, DB_NAME, tables});
		  p.waitFor();
		  
		  System.out.println("\tDelete intermediate tables:\t" +  ((double)(System.currentTimeMillis() - time) / 1000) + "");
		  
	  } catch(Exception e) {
	      e.printStackTrace();
	  }
	  System.out.println(((double)(System.currentTimeMillis() - totalTime) / 1000) + "");
  }
  
  private void doLocalJoin(QueryPlanNode node) {
    long time = System.currentTimeMillis();
    try {
      //System.out.println(node.getJoinQuery(null));
      wrietQueryToDisk(node.getJoinQuery(null));	
      // TODO: Fix this hard coded location
      String queryFile = "~/Distributed-Graph-Query-Optimization/DGPM/src/query.sql";
      
      Process p = Runtime.getRuntime().exec(new String[]{"python", JOIN_PROGRAM, LOCAL_JOIN_WITH_A_FILE, hostFile, PSQL_DIR, DB_PORT, DB_NAME, OUTPUT_DIR + node.getNodeID(), queryFile});      
      p.waitFor();
      /*
      BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
      p.waitFor();
      while (br.ready())
    	  System.out.println(br.readLine());
	  */ 
    } catch(Exception e) {
      e.printStackTrace();
    }
    System.out.println("--------------------" + node.getType() + "--------------------");
    System.out.println(((double)(System.currentTimeMillis() - time) / 1000) + ""); 
  }
  
  private void wrietQueryToDisk(String query) {
	  String output = "query.sql";
	  
	  try {
  		PrintWriter writer = new PrintWriter(new FileWriter(output));
  		writer.print(query);
  		writer.close();
	  } catch (Exception e) {
		  System.err.println("Error: " + e.getMessage());
	  }
  }
}