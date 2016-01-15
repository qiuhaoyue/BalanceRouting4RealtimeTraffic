import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.sql.*;

public class TRkShortestPath {
	
	ArrayList<GraphEdgeInfo> m_vecEdgeVector;
	HashMap<Long, Long> m_mapEdgeId2Index;
	HashMap<Long, ArrayList<Long>> m_mapNodeId2Edge; //map from node to edge_index
	long max_node_id;
	long max_edge_id;
	long m_lStartEdgeId;
	long m_lEndEdgeId;
	double m_dStartpart;
	double m_dEndPart;
	boolean isStartVirtual;
	boolean isEndVirtual;
	
	long m_lStartVertex;
	long m_lEndVertex;
	
	ArrayList<Path_element> m_vecPath;
	HashMap<Long, ArrayList<Rule>> m_ruleTable;
	boolean m_bIsturnRestrictOn;
	boolean m_bIsGraphConstructed;
	boolean m_bIsCostParentCreated;
	
	Parent_path[] parent;
	CostHolder[] m_dCost;
		
	ArrayList<Edge> edges;
	ArrayList<Edge> virtual_edges;
	ArrayList<Rule> rulelist;
	int max_virtual_size;
	
	//for ksp
	int m_itopK;
	int m_nGeneratedPathNum;
	ArrayList<Path> m_vResultList;
	TreeMap<Double, ArrayList<Integer>> m_mpPathCandidates;
	
	//for updating cost of previous shortest path
	HashMap<Long, Pair> m_mpRevisedEdge;
	//Turning2DoubleMap m_mpRevisedTurning;
	double m_dPenaltyFactor;
	
	public static final int ITERATION_THRESHOLD=20;
	public static final double DEFAULT_PENALTY=0.2;
	
	TRkShortestPath(){
		m_vecEdgeVector=new ArrayList<GraphEdgeInfo>();
		m_mapEdgeId2Index=new HashMap<Long, Long> ();
		m_mapNodeId2Edge=new HashMap<Long, ArrayList<Long>>();
		m_vecPath=new ArrayList<Path_element>();
		m_ruleTable=new HashMap<Long, ArrayList<Rule>>();
		
		max_node_id=-1;
		max_edge_id=-1;
		m_lStartEdgeId=-1;
		m_lEndEdgeId=-1;
		m_dStartpart=-1;
		m_dEndPart=-1;
		isStartVirtual=false;
		isEndVirtual=false;
		
		m_bIsturnRestrictOn=false;
		m_bIsGraphConstructed=false;
		m_bIsCostParentCreated=false;
		
		edges=new ArrayList<Edge>();
		virtual_edges=new ArrayList<Edge>();
		rulelist= new ArrayList<Rule>();
		
		m_vResultList = new ArrayList<Path>();
		m_mpPathCandidates = new TreeMap<Double, ArrayList<Integer>>();
		m_mpRevisedEdge=new HashMap<Long, Pair>();
		m_dPenaltyFactor = DEFAULT_PENALTY;
		max_virtual_size=4;
		
	}
	
	public void init()
	{
		if(isEndVirtual){
			//remove the added edges and rules caused by virtual end;
		}
		if(isStartVirtual){
			//remove the added edges and rules caused by virtual start; 
		}
		max_edge_id = 0;
		max_node_id = 0;
		isStartVirtual = false;
		isEndVirtual = false;
	}

	public boolean get_edges(String database, String roadmap_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs=null;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return false;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    //String sql="select id as gid, source, target, cost as to_cost, reverse_cost from " + roadmap_table +";";
		    String sql="select gid, source, target, to_cost, reverse_cost from " + roadmap_table +";";
		    //System.out.println(sql);
		    rs=stmt.executeQuery(sql);
		    edges.clear();
		    while(rs.next()){
		    	Edge new_edge=new Edge(rs.getInt("gid"),rs.getInt("source"), rs.getInt("target"), rs.getDouble("to_cost"), rs.getDouble("reverse_cost"));
		    	edges.add(new_edge);
		    }
		}
		catch (SQLException e) {
		    e.printStackTrace();
		    return false;
		}
		catch (Exception e) {
		    e.printStackTrace();
		    return false;
		}
		finally {
		    DBconnector.dropConnection(con);
		}
		//System.out.println("clear finished!");
		return true;
	}
	
	public boolean get_restriction(String database, String restriction_table){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs=null;
		
		con = DBconnector.getConnection("jdbc:postgresql://localhost:5432/"+database, "postgres", "");
		if (con == null) {
			System.out.println("Failed to make connection!");
			return false;
		}
		
		try {
			//import the data from database;
		    stmt = con.createStatement();
		    //String sql="select to_cost, to_edge as to_way, from_edge as from_way from " + restriction_table +" where to_edge IS NOT NULL and from_edge IS NOT NULL;";
		    String sql="select to_cost, to_way, from_way from " + restriction_table +" where to_way IS NOT NULL and from_way IS NOT NULL;";
		    //System.out.println(sql);
		    rs=stmt.executeQuery(sql);
		    rulelist.clear();
		    while(rs.next()){
		    	Rule new_rule=new Rule();
		    	new_rule.cost=rs.getDouble("to_cost");
		    	new_rule.precedencelist.add(rs.getLong("to_way"));
		    	new_rule.precedencelist.add(rs.getLong("from_way"));
		    	rulelist.add(new_rule);
		    }
		}
		catch (SQLException e) {
		    e.printStackTrace();
		    return false;
		}
		catch (Exception e) {
		    e.printStackTrace();
		    return false;
		}
		finally {
		    DBconnector.dropConnection(con);
		}
		//System.out.println("clear finished!");
		return true;
	}
	
	boolean construct_graph(ArrayList<Edge> edges)
	{
		int i;
		for(i = 0; i < edges.size(); i++)
		{
			addEdge(edges.get(i));
		}
		return true;
	}
	
	boolean connectEdge(GraphEdgeInfo firstEdge, GraphEdgeInfo secondEdge, boolean bIsStartNodeSame)
	{
		if(bIsStartNodeSame)
		{
			if(firstEdge.m_dReverseCost >= 0.0)
				firstEdge.m_vecStartConnectedEdge.add(secondEdge.m_lEdgeIndex);
			if(firstEdge.m_lStartNode == secondEdge.m_lStartNode)
			{
				if(secondEdge.m_dReverseCost >= 0.0)
					secondEdge.m_vecStartConnectedEdge.add(firstEdge.m_lEdgeIndex);
			}
			else
			{
				if(secondEdge.m_dCost >= 0.0)
					secondEdge.m_vecEndConnedtedEdge.add(firstEdge.m_lEdgeIndex);
			}
		}
		else
		{
			if(firstEdge.m_dCost >= 0.0)
				firstEdge.m_vecEndConnedtedEdge.add(secondEdge.m_lEdgeIndex);
			if(firstEdge.m_lEndNode == secondEdge.m_lStartNode)
			{
				if(secondEdge.m_dReverseCost >= 0.0)
					secondEdge.m_vecStartConnectedEdge.add(firstEdge.m_lEdgeIndex);
			}
			else
			{
				if(secondEdge.m_dCost >= 0.0)
					secondEdge.m_vecEndConnedtedEdge.add(firstEdge.m_lEdgeIndex);
			}
		}

		return true;
	}
	
	boolean disconnectEdge(GraphEdgeInfo firstEdge, GraphEdgeInfo secondEdge, boolean bIsStartNodeSame)
	{
		if(bIsStartNodeSame)
		{
			if(firstEdge.m_dReverseCost >= 0.0)
				firstEdge.m_vecStartConnectedEdge.remove(secondEdge.m_lEdgeIndex);
			if(firstEdge.m_lStartNode == secondEdge.m_lStartNode)
			{
				if(secondEdge.m_dReverseCost >= 0.0)
					secondEdge.m_vecStartConnectedEdge.remove(firstEdge.m_lEdgeIndex);
			}
			else
			{
				if(secondEdge.m_dCost >= 0.0)
					secondEdge.m_vecEndConnedtedEdge.remove(firstEdge.m_lEdgeIndex);
			}
		}
		else
		{
			if(firstEdge.m_dCost >= 0.0)
				firstEdge.m_vecEndConnedtedEdge.remove(secondEdge.m_lEdgeIndex);
			if(firstEdge.m_lEndNode == secondEdge.m_lStartNode)
			{
				if(secondEdge.m_dReverseCost >= 0.0)
					secondEdge.m_vecStartConnectedEdge.remove(firstEdge.m_lEdgeIndex);
			}
			else
			{
				if(secondEdge.m_dCost >= 0.0)
					secondEdge.m_vecEndConnedtedEdge.remove(firstEdge.m_lEdgeIndex);
			}
		}

		return true;
	}
	
	public boolean addEdge(Edge edgeIn)
	{
		/*
		if(edgeIn.id==34719){
			System.out.println("here");
		}*/
		if(m_mapEdgeId2Index.containsKey(edgeIn.id)){
			return false;
		}
		
		GraphEdgeInfo newEdge=new GraphEdgeInfo();
		newEdge.m_lEdgeID = edgeIn.id;
		newEdge.m_lEdgeIndex = m_vecEdgeVector.size();	
		newEdge.m_lStartNode = edgeIn.source;
		newEdge.m_lEndNode = edgeIn.target;
		newEdge.m_dCost = edgeIn.cost;
		newEdge.m_dReverseCost = edgeIn.reverse_cost;

		if(edgeIn.id > max_edge_id)
		{
			max_edge_id = edgeIn.id;
		}

		if(newEdge.m_lStartNode > max_node_id)
		{
			max_node_id = newEdge.m_lStartNode;
		}
		if(newEdge.m_lEndNode > max_node_id)
		{
			max_node_id = newEdge.m_lEdgeIndex;
		}

		//Searching the start node for connectivity
		if(m_mapNodeId2Edge.containsKey(edgeIn.source)){
			//Connect current edge with existing edge with start node
			//connectEdge(
			int lEdgeCount = m_mapNodeId2Edge.get(edgeIn.source).size();
			int lEdgeIndex;
			for(lEdgeIndex = 0; lEdgeIndex < lEdgeCount; lEdgeIndex++)
			{
				long lEdge = (m_mapNodeId2Edge.get(edgeIn.source).get(lEdgeIndex));	
				connectEdge(newEdge, m_vecEdgeVector.get((int)lEdge), true);
			}
		}


		//Searching the end node for connectivity
		if(m_mapNodeId2Edge.containsKey(edgeIn.target)){
			//Connect current edge with existing edge with start node
			//connectEdge(
			int lEdgeCount = m_mapNodeId2Edge.get(edgeIn.target).size();
			int lEdgeIndex;
			for(lEdgeIndex = 0; lEdgeIndex < lEdgeCount; lEdgeIndex++)
			{
				long lEdge = (m_mapNodeId2Edge.get(edgeIn.target).get(lEdgeIndex));	
				connectEdge(newEdge, m_vecEdgeVector.get((int)lEdge), false);
			}
		}

		//Add this node and edge into the data structure
		if(m_mapNodeId2Edge.containsKey(edgeIn.source)){
			m_mapNodeId2Edge.get(edgeIn.source).add(newEdge.m_lEdgeIndex);
		}
		else{
			ArrayList<Long> edgelist=new ArrayList<Long>();
			edgelist.add(newEdge.m_lEdgeIndex);
			m_mapNodeId2Edge.put(edgeIn.source, edgelist);
		}
		if(m_mapNodeId2Edge.containsKey(edgeIn.target)){
			m_mapNodeId2Edge.get(edgeIn.target).add(newEdge.m_lEdgeIndex);
		}
		else{
			ArrayList<Long> edgelist=new ArrayList<Long>();
			edgelist.add(newEdge.m_lEdgeIndex);
			m_mapNodeId2Edge.put(edgeIn.target, edgelist);
		}


		//Adding edge to the list
		m_mapEdgeId2Index.put(newEdge.m_lEdgeID, (long)m_vecEdgeVector.size());
		m_vecEdgeVector.add(newEdge);
		
		/*
		if(edgeIn.source==56155 || edgeIn.target==56155){
			if(m_mapEdgeId2Index.containsKey(edgeIn.id)){
				System.out.println("correct!");
			}
			long index=m_mapEdgeId2Index.get(edgeIn.id);
			System.out.println(index);
		}*/
		

		return true;
	}
	
	public boolean removeEdge(Edge edgeIn)
	{
		
		if(!m_mapEdgeId2Index.containsKey(edgeIn.id)){
			return false;
		}

		GraphEdgeInfo newEdge=new GraphEdgeInfo();
		newEdge.m_lEdgeID = edgeIn.id;
		newEdge.m_lEdgeIndex = m_mapEdgeId2Index.get(edgeIn.id);
		newEdge.m_lStartNode = edgeIn.source;
		newEdge.m_lEndNode = edgeIn.target;
		newEdge.m_dCost = edgeIn.cost;
		newEdge.m_dReverseCost = edgeIn.reverse_cost;

		//Searching the start node for connectivity
		if(m_mapNodeId2Edge.containsKey(edgeIn.source)){
			//Connect current edge with existing edge with start node
			//connectEdge(
			int lEdgeCount = m_mapNodeId2Edge.get(edgeIn.source).size();;
			int lEdgeIndex;
			for(lEdgeIndex = lEdgeCount-1; lEdgeIndex >=0; lEdgeIndex--)
			{
				long lEdge = (m_mapNodeId2Edge.get(edgeIn.source).get(lEdgeIndex));	
				disconnectEdge(newEdge, m_vecEdgeVector.get((int)lEdge), true);
			}
		}

		//Searching the end node for connectivity
		if(m_mapNodeId2Edge.containsKey(edgeIn.target)){
			//Connect current edge with existing edge with start node
			//connectEdge(
			int lEdgeCount = m_mapNodeId2Edge.get(edgeIn.target).size();;
			int lEdgeIndex;
			for(lEdgeIndex = lEdgeCount-1; lEdgeIndex >=0; lEdgeIndex--)
			{
				long lEdge = (m_mapNodeId2Edge.get(edgeIn.target).get(lEdgeIndex));	
				disconnectEdge(newEdge, m_vecEdgeVector.get((int)lEdge), false);
			}
		}

		//Add this node and edge into the data structure
		int size=0;
		if(m_mapNodeId2Edge.containsKey(edgeIn.source)){
			m_mapNodeId2Edge.get(edgeIn.source).remove(newEdge.m_lEdgeIndex);
			size=m_mapNodeId2Edge.get(edgeIn.source).size();
			//No need to remove the entry in hashTable: every time the new node has the same id, so it will not keep expanding;
			if(size==0){
				m_mapNodeId2Edge.remove(edgeIn.source);
			}
		}
		
		if(m_mapNodeId2Edge.containsKey(edgeIn.target)){
			m_mapNodeId2Edge.get(edgeIn.target).remove(newEdge.m_lEdgeIndex);
			size=m_mapNodeId2Edge.get(edgeIn.target).size();
			if(size==0){
				m_mapNodeId2Edge.remove(edgeIn.target);
			}
		}

		//Remove edge from the list
		m_mapEdgeId2Index.remove(newEdge.m_lEdgeID);
		m_vecEdgeVector.remove((int)newEdge.m_lEdgeIndex);
		
		max_edge_id--;
		
		return true;
	}
	
	public void KSPclear(){
		m_nGeneratedPathNum = 0;
		m_vResultList.clear();
		m_mpRevisedEdge.clear();
		this.m_mpPathCandidates.clear();
	}
	
	public int get_shortest_paths(String database, String roadmap_table, String restriction_table, boolean update_graph,
			int start_edge_id, double start_part, int end_edge_id, double end_part, int top_k, double max_delay, ArrayList<Path> paths){
		
		if(paths==null){
			return -1;
		}
		paths.clear();
		this.KSPclear();
		this.m_itopK=top_k;
		double max_cost=0.0;
		
		//create or update the graph and get the shortest path
		ArrayList<Path_element> shorest_path= new ArrayList<Path_element>();
		if(this.shortest_path(database, roadmap_table, restriction_table, update_graph, 
				start_edge_id, start_part, end_edge_id, end_part, shorest_path, false)<0){
			return -1;
		}
		//paths.add(shorest_path);
		Path first_path=new Path(shorest_path);
		max_cost=first_path.get_cost()*max_delay;
		m_vResultList.add(first_path);
		
		int iteration=0;
		int path_index=0;
		while (m_vResultList.size() < top_k && iteration < TRkShortestPath.ITERATION_THRESHOLD)
		{
			path_index = next(path_index);
			iteration++;
			//System.out.println("--------"+iteration+"--------");
		}
		
		//all paths are in m_vResultList, all except for the last one are in m_mpPathCandidates
		int path_count=m_vResultList.size();
		path_index=path_count-1;
		Path cur_path = m_vResultList.get(path_count-1);
		
		if(m_mpPathCandidates.containsKey(cur_path.get_cost())){
			if(!m_mpPathCandidates.get(cur_path.get_cost()).contains(path_index)){
				m_mpPathCandidates.get(cur_path.get_cost()).add(path_index);
			}
		}
		else{
			ArrayList<Integer> index_list=new ArrayList<Integer>();
			index_list.add(path_index);
			m_mpPathCandidates.put(cur_path.get_cost(), index_list);
		}
		
		Set<Entry<Double, ArrayList<Integer>>> set=m_mpPathCandidates.entrySet();
		Iterator<Entry<Double, ArrayList<Integer>>> iterator= set.iterator();
		
		while(iterator.hasNext()){
			ArrayList<Integer> index_list=iterator.next().getValue();
			for(int i=0; i<index_list.size(); i++){
				Path path_to_add=m_vResultList.get(index_list.get(i));
				
				if(isStartVirtual)
				{
					path_to_add.path.get(0).vertex_id = -1;
					path_to_add.path.get(0).edge_id = m_lStartEdgeId;
				}
				
				if(isEndVirtual)
				{
					path_to_add.path.get(path_to_add.length()-1).edge_id = m_lEndEdgeId;
				}
				
				if(path_to_add.get_cost()<=max_cost){
					paths.add(path_to_add);
				}
			}
		}
		//paths.addAll(m_vResultList);
		return 0;
	}
	
	//remove edges in loopless path pk
	public int revise_path(Path cur_path){
		
		int path_length = cur_path.length();
		for(int i=0; i<path_length; ++i)
		{
			
			Path_element node=cur_path.path.get(i);
			long edge_id = node.edge_id;
			if((i==path_length-1) && edge_id<0){
				continue; //do this because the last road segment is -1 when end at position 0.0;
			}
			
			try{
				long edge_index=this.m_mapEdgeId2Index.get(edge_id);
				GraphEdgeInfo cur_edge=this.m_vecEdgeVector.get((int)edge_index);
				
				boolean direction=true;
				if(cur_edge.m_lEndNode==node.vertex_id){
					direction=false;
				}
				
				if(this.m_mpRevisedEdge.containsKey(edge_index)){
					if(direction==true){
						m_mpRevisedEdge.get(edge_index).first*=(1+this.m_dPenaltyFactor);
					}
					else{
						m_mpRevisedEdge.get(edge_index).second*=(1+this.m_dPenaltyFactor);
					}
				}
				else{
					if(direction==true){
						m_mpRevisedEdge.put(edge_index, new Pair((1+this.m_dPenaltyFactor)*cur_edge.m_dCost,cur_edge.m_dReverseCost));
					}
					else{
						m_mpRevisedEdge.put(edge_index, new Pair(cur_edge.m_dCost, (1+this.m_dPenaltyFactor)*cur_edge.m_dReverseCost));
					}
				}
			}
			catch(Exception e){
				e.printStackTrace();	
			}
		}
		
		return 0;
	}
	
	public int next(int path_index){

		Path cur_path;
		//1. Prepare for revise vertices and arcs
		if(path_index<0){
			return -1;
		}
		else{
			cur_path = m_vResultList.get(path_index);
			if(m_mpPathCandidates.containsKey(cur_path.get_cost())){
				if(!m_mpPathCandidates.get(cur_path.get_cost()).contains(path_index)){
					m_mpPathCandidates.get(cur_path.get_cost()).add(path_index);
				}
			}
			else{
				ArrayList<Integer> index_list=new ArrayList<Integer>();
				index_list.add(path_index);
				m_mpPathCandidates.put(cur_path.get_cost(), index_list);
			}
		}

		//2. revise edges along the cur_path
		this.revise_path(cur_path);
		
		//3. Calculate the shortest path again in the graph
		Path new_path = new Path();
		if(this.my_dijkstra(edges, m_lStartVertex, m_lEndVertex, new_path.path, false)<0){
			return -1;
		}
		//new_path.print();
		
		//3.1 restore the correct cost of each road segment
		for(int i=0; i<new_path.length();i++){
			Path_element node=new_path.path.get(i);
			
			if(i==new_path.length()-1 && node.edge_id<0){
				continue;
			}
			
			GraphEdgeInfo cur_edge = this.m_vecEdgeVector.get(m_mapEdgeId2Index.get(node.edge_id).intValue());
			if(node.vertex_id==cur_edge.m_lStartNode){
				node.cost=this.get_original_cost(cur_edge, true);
			}
			else{
				node.cost=this.get_original_cost(cur_edge, false);
			}
			new_path.path.set(i, node);
		}
		
		//4. Test whether the newly calculated path has been calculated previously
		double search_weight = new_path.get_cost();
		boolean is_unique=true;
		int equal_path_index=-1;
		
		if(m_mpPathCandidates.containsKey(search_weight)){
			ArrayList<Integer> equalweight_paths=m_mpPathCandidates.get(search_weight);
			for(int i=0; i<equalweight_paths.size(); i++){
				equal_path_index=equalweight_paths.get(i);
				if(this.m_vResultList.get(equal_path_index).equals(new_path)){
					is_unique=false;
					break;
				}
			}
		}
		
		if(is_unique){
			m_vResultList.add(new_path);
			return m_vResultList.size()-1;
		}
		else{
			//new_path.print();
			return equal_path_index;
		}

	}
	
	public int shortest_path(String database, String roadmap_table, String restriction_table, boolean update_graph,
			int start_edge_id, double start_part, int end_edge_id, double end_part, ArrayList<Path_element> path, boolean forout){
		
		/*
		Date time1=new Date();
    	for(int i=0;i<path.size();i++){
    		//System.out.println(path.get(i).vertex_id + ", " + path.get(i).edge_id + ", " + path.get(i).cost);
    	}*/
		//Build Basic Graph
		if(!m_bIsGraphConstructed || update_graph)
		{
			init();
			if(!get_edges(database, roadmap_table)){
				System.out.println("get_edges failed!");
				return -1;
			}
			construct_graph(edges);
			m_bIsGraphConstructed = true;
			m_bIsCostParentCreated=false;
		}
		/*
		Date time2=new Date();
		int step_count=0;
    	System.out.println("	Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	
    	time1=new Date();*/
		//Build basic m_ruleTable
		if(!m_bIsturnRestrictOn || update_graph){
			if(!get_restriction(database, restriction_table)){
				System.out.println("get_restrictions failed!");
				return -1;
			}
			
			m_ruleTable.clear();
			int total_rule = rulelist.size();
			int i;
			
			for(i = 0; i < total_rule; i++){
				Rule rule=new Rule();
				rule.cost = rulelist.get(i).cost;
				int count=rulelist.get(i).precedencelist.size();
				for(int j=1; j<count; j++){
					rule.precedencelist.add(rulelist.get(i).precedencelist.get(j));
				}
				
				Long dest_edge_id = rulelist.get(i).precedencelist.get(0);
				if(m_ruleTable.containsKey(dest_edge_id)){
					m_ruleTable.get(dest_edge_id).add(rule);
				}
				else{
					ArrayList<Rule> temprules=new ArrayList<Rule>();
					temprules.clear();
					temprules.add(rule);
					m_ruleTable.put(dest_edge_id, temprules);
				}
			
			}
			m_bIsturnRestrictOn = true;
		}
		/*
		time2=new Date();
    	System.out.println("	Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	time1=new Date();*/
		//Remove the influence of previous virtual start & end
		if(!virtual_edges.isEmpty()){
			for(int j=virtual_edges.size()-1; j>=0;j--){
				
				Edge virtual_edge=virtual_edges.get(j);
				
				//remove the rules ending at virtual_edge
				if(m_ruleTable.containsKey(virtual_edge.id)){
					m_ruleTable.remove(virtual_edge.id);
				}
				
				//remove the rules starting at virtual_edge
				if(virtual_edge.source== m_lStartVertex || virtual_edge.source== m_lEndVertex){
					ArrayList<Long> vecsource = new ArrayList<Long>();
					vecsource = m_mapNodeId2Edge.get(virtual_edge.target);
					int k;
					for(k = 0; k < vecsource.size(); k++){
						long edge_id=m_vecEdgeVector.get(vecsource.get(k).intValue()).m_lEdgeID;
						if(m_ruleTable.containsKey(edge_id)){
							ArrayList<Rule> buf=m_ruleTable.get(edge_id);
							int size=buf.size();
							for(int i=size-1; i>=0; i--){
								if(buf.get(i).precedencelist.get(0).longValue()==virtual_edge.id){
									m_ruleTable.get(edge_id).remove(i);
								}
							}
							if(m_ruleTable.get(edge_id).size()==0){
								m_ruleTable.remove(edge_id);
							}
						}
					}
				}
				
				if(virtual_edge.target== m_lStartVertex || virtual_edge.target== m_lEndVertex){
					ArrayList<Long> vecsource = new ArrayList<Long>();
					vecsource = m_mapNodeId2Edge.get(virtual_edge.source);
					int k;
					for(k = 0; k < vecsource.size(); k++){
						long edge_id=m_vecEdgeVector.get(vecsource.get(k).intValue()).m_lEdgeID;
						if(m_ruleTable.containsKey(edge_id)){
							ArrayList<Rule> buf=m_ruleTable.get(edge_id);
							int size=buf.size();
							for(int i=size-1; i>=0; i--){
								if(buf.get(i).precedencelist.get(0).longValue()==virtual_edge.id){
									m_ruleTable.get(edge_id).remove(i);
								}
							}
							if(m_ruleTable.get(edge_id).size()==0){
								m_ruleTable.remove(edge_id);
							}
						}
					}
				}
				
				//remove virtual_edge;
				removeEdge(virtual_edge);
			}	
				
			virtual_edges.clear();
			
			if(isEndVirtual){
				isEndVirtual=false;
				max_node_id--;
			}
			
			if(isStartVirtual){
				isStartVirtual=false;
				max_node_id--;
			}
		}
		/*
		time2=new Date();
    	System.out.println("	Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	time1=new Date();*/
		
		//Add virtual edges for virtual start & end
		if(!m_mapEdgeId2Index.containsKey((long)start_edge_id)){
			System.out.println("Start Edge not found: "+ start_edge_id +" !");
			return -1;
		}
		GraphEdgeInfo start_edge_info = m_vecEdgeVector.get(m_mapEdgeId2Index.get((long)start_edge_id).intValue());

		long start_vertex, end_vertex;
		m_dStartpart = start_part;
		m_dEndPart = end_part;
		m_lStartEdgeId = start_edge_id;
		m_lEndEdgeId = end_edge_id;
		
		if(start_part == 0.0)
		{
			start_vertex = start_edge_info.m_lStartNode;
			m_lStartVertex=start_vertex;
		}
		else if(start_part == 1.0)
		{
			start_vertex = start_edge_info.m_lEndNode;
			m_lStartVertex=start_vertex;
		}
		else
		{
			isStartVirtual = true;
			m_lStartEdgeId = start_edge_id;
			start_vertex = max_node_id + 1;
			m_lStartVertex=start_vertex;
			
			max_node_id++;
			
			if(start_edge_info.m_dCost >= 0.0)
			{
				Edge start_edge=new Edge();
				start_edge.id = max_edge_id + 1;
				max_edge_id++;
				start_edge.source = start_vertex;
				start_edge.reverse_cost = -1.0;
				
				start_edge.target = start_edge_info.m_lEndNode;
				start_edge.cost = (1.0 - start_part) * start_edge_info.m_dCost;
				addEdge(start_edge);
				virtual_edges.add(start_edge);
			}
			
			if(start_edge_info.m_dReverseCost >= 0.0)
			{
				Edge start_edge=new Edge();
				start_edge.id = max_edge_id + 1;
				max_edge_id++;
				start_edge.source = start_vertex;
				start_edge.reverse_cost = -1.0;
				
				start_edge.target = start_edge_info.m_lStartNode;
				start_edge.cost = start_part * start_edge_info.m_dReverseCost;
				addEdge(start_edge);
				virtual_edges.add(start_edge);
			}
		}
		
		if(!m_mapEdgeId2Index.containsKey((long)end_edge_id)){
			System.out.println("End Edge not found: "+ end_edge_id +" !");
			return -1;
		}
		GraphEdgeInfo end_edge_info = m_vecEdgeVector.get(m_mapEdgeId2Index.get((long)end_edge_id).intValue());
		
		if(end_part == 0.0)
		{
			end_vertex = end_edge_info.m_lStartNode;
			m_lEndVertex=end_vertex;
		}
		else if(end_part == 1.0)
		{
			end_vertex = end_edge_info.m_lEndNode;
			m_lEndVertex=end_vertex;
		}
		else
		{
			isEndVirtual = true;
			m_lEndEdgeId = end_edge_id;
			end_vertex = max_node_id + 1;
			m_lEndVertex=end_vertex;
			max_node_id++;
			
			if(end_edge_info.m_dCost >= 0.0)
			{
				Edge end_edge = new Edge();
				end_edge.id = max_edge_id + 1;
				max_edge_id++;
				end_edge.target = end_vertex;
				end_edge.reverse_cost = -1.0;
				
				end_edge.source = end_edge_info.m_lStartNode;
				end_edge.cost = end_part * end_edge_info.m_dCost;
				addEdge(end_edge);
				virtual_edges.add(end_edge);
			}
			if(end_edge_info.m_dReverseCost >= 0.0)
			{
				Edge end_edge = new Edge();
				end_edge.id = max_edge_id + 1;
				max_edge_id++;
				end_edge.target = end_vertex;
				end_edge.reverse_cost = -1.0;
				
				end_edge.source = end_edge_info.m_lEndNode;
				end_edge.cost = (1.0 - end_part) * end_edge_info.m_dReverseCost;
				addEdge(end_edge);
				virtual_edges.add(end_edge);
			}
		}
		/*
		time2=new Date();
    	System.out.println("	Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	time1=new Date();*/
    	//Add virtual rules for virtual start & end
		int total_rule = rulelist.size();
		int i;
		ArrayList<Long> vecsource = new ArrayList<Long>();
		int kk;
		for(i = 0; i <total_rule; i++)
		{
			Long dest_edge_id = rulelist.get(i).precedencelist.get(0);
			if(isStartVirtual){
				if(rulelist.get(i).precedencelist.size() == 2 && rulelist.get(i).precedencelist.get(1) == m_lStartEdgeId){
					vecsource = m_mapNodeId2Edge.get(start_vertex);
					int size=0;
					for(kk = 0; kk < vecsource.size(); kk++){
						
						GraphEdgeInfo from_edge=m_vecEdgeVector.get(vecsource.get(kk).intValue());
						GraphEdgeInfo to_edge=m_vecEdgeVector.get(m_mapEdgeId2Index.get(dest_edge_id).intValue());
						
						if((from_edge.m_lStartNode==start_vertex && (from_edge.m_lEndNode == to_edge.m_lEndNode || from_edge.m_lEndNode == to_edge.m_lStartNode))
								||(from_edge.m_lEndNode==start_vertex && (from_edge.m_lStartNode == to_edge.m_lEndNode || from_edge.m_lStartNode == to_edge.m_lStartNode))){
							Rule rule=new Rule();
							rule.cost = rulelist.get(i).cost;
							rule.precedencelist.clear();
							rule.precedencelist.add(from_edge.m_lEdgeID);
							m_ruleTable.get(dest_edge_id).add(rule);
							size=m_ruleTable.get(dest_edge_id).size();//
						}
					}
					size=size+0;
				}
			}
		}
		if(isEndVirtual){
			if(m_ruleTable.containsKey(m_lEndEdgeId)){
				vecsource = m_mapNodeId2Edge.get(end_vertex);
				for(kk = 0; kk < vecsource.size(); kk++){
					ArrayList<Rule> tmpRules = new ArrayList<Rule>(m_ruleTable.get(m_lEndEdgeId));
					m_ruleTable.put(m_vecEdgeVector.get(vecsource.get(kk).intValue()).m_lEdgeID, tmpRules);
				}
			}
		}
		/*
		time2=new Date();
    	System.out.println("	Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	time1=new Date();*/
    	
		int return_value=(my_dijkstra(edges, start_vertex, end_vertex, path, forout));
		/*
		time2=new Date();
    	System.out.println("	Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	time1=new Date();*/
    	return return_value;
	}
	
	public double get_cost(GraphEdgeInfo cur_edge, boolean direction){
		return this.get_cost(cur_edge, direction, false);
	}
	
	public double get_original_cost(GraphEdgeInfo cur_edge, boolean direction){
		return this.get_cost(cur_edge, direction, true);
	}
	
	public double get_cost(GraphEdgeInfo cur_edge, boolean direction, boolean original){
		
		double cost=-1;
		
		if(direction==true && cur_edge.m_dCost>=0){
			if(!original && this.m_mpRevisedEdge.containsKey(cur_edge.m_lEdgeIndex)){
				return m_mpRevisedEdge.get(cur_edge.m_lEdgeIndex).first;
			}
			else{
				return cur_edge.m_dCost;
			}
		}
		else if(direction==false && cur_edge.m_dReverseCost>=0){
			if(!original && this.m_mpRevisedEdge.containsKey(cur_edge.m_lEdgeIndex)){
				return m_mpRevisedEdge.get(cur_edge.m_lEdgeIndex).second;
			}
			else{
				return cur_edge.m_dReverseCost;
			}
		}
		
		return cost;
	}
	
	public int my_dijkstra(ArrayList<Edge> edges, long start_vertex, long end_vertex, ArrayList<Path_element> path, boolean forout){
		
		/*
    	Date time1=new Date();
    	int step_count=0;*/
		
		if(!m_bIsGraphConstructed)
		{
			init();
			construct_graph(edges);
			m_bIsGraphConstructed = true;
			m_bIsCostParentCreated=false;
		}
		Comparator<WeightDirectEdge> comparator = new WeightDirectEdgeComparator();
        PriorityQueue<WeightDirectEdge> queue = new PriorityQueue<WeightDirectEdge>(1000, comparator);
        
        /*Date time2=new Date();
    	System.out.println("		--Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	time1=new Date();*/
        
        int i;
        int edge_count=edges.size() + max_virtual_size;
        if(!m_bIsCostParentCreated || virtual_edges.size()> max_virtual_size){
        	if(virtual_edges.size() > max_virtual_size){
        		max_virtual_size=virtual_edges.size();
        		edge_count=edges.size()+max_virtual_size;
        		System.out.println("virtual_edges.size( > 4 !!!!!");
        	}
	        
			parent = new Parent_path[edge_count + 1];
			m_dCost = new CostHolder[edge_count + 1];
			
			for(i = 0; i <= edge_count; i++)
			{
				m_dCost[i]=new CostHolder();
				parent[i]=new Parent_path();
			}
			
			m_bIsCostParentCreated=true;
        }
        
        m_vecPath.clear();
        for(i = 0; i <= edge_count; i++)
		{
			m_dCost[i].startCost = 1e15;
			m_dCost[i].endCost = 1e15;
			parent[i].restore();
		}
        /*
        time2=new Date();
    	System.out.println("		****Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	time1=new Date();*/
        
		if(!m_mapNodeId2Edge.containsKey(start_vertex)){
			System.out.println("Start not found !");
			return -1;
		}
		
		if(!m_mapNodeId2Edge.containsKey(end_vertex)){
			System.out.println("Destination Not Found");
			return -1;
		}
		
		ArrayList<Long> vecsource = m_mapNodeId2Edge.get(start_vertex);
		GraphEdgeInfo cur_edge=new GraphEdgeInfo();
		
		for(i = 0; i < vecsource.size(); i++)
		{
			cur_edge = m_vecEdgeVector.get(vecsource.get(i).intValue());
			if(cur_edge.m_lStartNode == start_vertex)
			{
				double dCost=this.get_cost(cur_edge, true);
				if( dCost >= 0.0)
				{
					m_dCost[(int)cur_edge.m_lEdgeIndex].endCost= dCost;
					parent[(int)cur_edge.m_lEdgeIndex].v_pos[0] = -1;
					parent[(int)cur_edge.m_lEdgeIndex].ed_ind[0] = -1;
					queue.add(new WeightDirectEdge(dCost, (int)cur_edge.m_lEdgeIndex, true));
				}
			}
			else
			{
				double dReverseCost=this.get_cost(cur_edge, false);
				if( dReverseCost >= 0.0)
				{
					m_dCost[(int)cur_edge.m_lEdgeIndex].startCost = dReverseCost;
					parent[(int)cur_edge.m_lEdgeIndex].v_pos[1] = -1;
					parent[(int)cur_edge.m_lEdgeIndex].ed_ind[1] = -1;
					queue.add(new WeightDirectEdge(dReverseCost, (int)cur_edge.m_lEdgeIndex, false));
				}
			}
		}
		//parent[start_vertex].v_id = -1;
		//parent[start_vertex].ed_id = -1;
		//m_dCost[start_vertex] = 0.0;
		
		//int new_node=-1;
		int cur_node=-1;
		/*
		time2=new Date();
    	System.out.println("		--Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	time1=new Date();*/
		
		while(!queue.isEmpty())
		{
			WeightDirectEdge cur_pos = queue.remove();
			int cured_index = cur_pos.edge;
			cur_edge = m_vecEdgeVector.get(cured_index);
			//GraphEdgeInfo new_edge=new GraphEdgeInfo();
			double dCost=this.get_cost(cur_edge, true);
			double dReverseCost=this.get_cost(cur_edge, false);
			
			if(cur_pos.direction)      // explore edges connected to end node
			{
				cur_node = (int)cur_edge.m_lEndNode;
				if(dCost < 0.0)
					continue;
				if(cur_node == end_vertex)
					break;
				explore(cur_node, cur_edge, true, cur_edge.m_vecEndConnedtedEdge, queue);
			}
			else            // explore edges connected to start node
			{
				cur_node = (int)cur_edge.m_lStartNode;
				if(dReverseCost < 0.0)
					continue;
				if(cur_node == end_vertex)
					break;
				explore(cur_node, cur_edge, false, cur_edge.m_vecStartConnectedEdge, queue);
			}
		}
		/*
		time2=new Date();
    	System.out.println("		Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	time1=new Date();*/
    	
		if(cur_node != end_vertex)
		{
			if(m_lStartEdgeId == m_lEndEdgeId)
			{
				if(get_single_cost(1000.0, path))
				{
					return 0;
				}
			}
			System.out.println("Path Not Found");
			return -1;
		}
		else
		{
			double total_cost;
			if(cur_node == cur_edge.m_lStartNode)
			{
				total_cost = m_dCost[(int)cur_edge.m_lEdgeIndex].startCost;
				construct_path((int)cur_edge.m_lEdgeIndex, 1);
			}
			else
			{
				total_cost = m_dCost[(int)cur_edge.m_lEdgeIndex].endCost;
				construct_path((int)cur_edge.m_lEdgeIndex, 0);
			}
		
			if(m_lStartEdgeId == m_lEndEdgeId)
			{
				if(get_single_cost(total_cost, path))
				{
					return 0;
				}
			}
			
			if(!isEndVirtual){
				Path_element pelement=new Path_element();
				pelement.vertex_id = end_vertex;
				pelement.edge_id = -1;
				pelement.cost = 0.0;
				m_vecPath.add(pelement);
			}
			
			path.clear();
			path.addAll(m_vecPath);
			
			if(isStartVirtual && forout)
			{
				path.get(0).vertex_id = -1;
				path.get(0).edge_id = m_lStartEdgeId;
			}
			
			if(isEndVirtual && forout)
			{
				path.get(path.size()-1).edge_id = m_lEndEdgeId;
			}
			
		}
		/*
		time2=new Date();
    	System.out.println("		Step "+step_count+":"+(time2.getTime()-time1.getTime()));
    	step_count++;
    	time1=new Date();*/
		return 0;
	}
	
	void explore(int cur_node, GraphEdgeInfo cur_edge, boolean isStart, ArrayList<Long> vecIndex, PriorityQueue<WeightDirectEdge> queue)
	{
		int i;
		double extCost = 0.0;
		GraphEdgeInfo new_edge=new GraphEdgeInfo();
		//int new_node;
		double totalCost;
		for(i = 0; i < vecIndex.size(); i++)
		{
			new_edge = m_vecEdgeVector.get(vecIndex.get(i).intValue());
			extCost = 0.0;
			if(m_bIsturnRestrictOn)
			{
				extCost = getRestrictionCost((int)cur_edge.m_lEdgeIndex, new_edge, isStart);
			}
			if(new_edge.m_lStartNode == cur_node)
			{
				double dCost=this.get_cost(new_edge, true);
				if(dCost >= 0.0)
				{
					//new_node = (int)new_edge.m_lEndNode;			
					if(isStart)
						totalCost = m_dCost[(int)cur_edge.m_lEdgeIndex].endCost + dCost + extCost;
					else
						totalCost = m_dCost[(int)cur_edge.m_lEdgeIndex].startCost + dCost + extCost;
					if(totalCost < m_dCost[vecIndex.get(i).intValue()].endCost)
					{
						m_dCost[vecIndex.get(i).intValue()].endCost = totalCost;
						parent[(int)new_edge.m_lEdgeIndex].v_pos[0] = (isStart?0:1);
						parent[(int)new_edge.m_lEdgeIndex].ed_ind[0] = (int)cur_edge.m_lEdgeIndex;
						queue.add(new WeightDirectEdge(totalCost, (int)new_edge.m_lEdgeIndex, true));
					}
				}
			}
			else
			{
				double dReverseCost=this.get_cost(new_edge, false);
				if(dReverseCost >= 0.0)
				{
					//new_node = (int)new_edge.m_lStartNode;
					if(isStart)
						totalCost = m_dCost[(int)cur_edge.m_lEdgeIndex].endCost + dReverseCost + extCost;
					else
						totalCost = m_dCost[(int)cur_edge.m_lEdgeIndex].startCost + dReverseCost + extCost;
					if(totalCost < m_dCost[vecIndex.get(i).intValue()].startCost)
					{
						m_dCost[vecIndex.get(i).intValue()].startCost = totalCost;
						parent[(int)new_edge.m_lEdgeIndex].v_pos[1] = (isStart?0:1);
						parent[(int)new_edge.m_lEdgeIndex].ed_ind[1] = (int)cur_edge.m_lEdgeIndex;
						queue.add(new WeightDirectEdge(totalCost, (int)new_edge.m_lEdgeIndex, false));
					}
				}
			}
		}
	}
	
	double getRestrictionCost(int edge_ind, GraphEdgeInfo new_edge, boolean isStart)
	{
		double cost = 0.0;
		long edge_id = new_edge.m_lEdgeID;
		if(!m_ruleTable.containsKey(edge_id))
		{	
			return(0.0);
		}
		ArrayList<Rule> vecRules = m_ruleTable.get(edge_id);
		int ruleIndex;
		int totalRule = vecRules.size();
		int st_edge_ind = edge_ind;
		for(ruleIndex = 0; ruleIndex < totalRule; ruleIndex++)
		{
			boolean flag = true;
			int total_edge = vecRules.get(ruleIndex).precedencelist.size();
			int i;
			int v_pos = (isStart?0:1);
			edge_ind = st_edge_ind;
			for(i = 0; i < total_edge; i++)
			{
				if(edge_ind == -1)
				{
					flag = false;
					break;
				}
				if(vecRules.get(ruleIndex).precedencelist.get(i).longValue() != m_vecEdgeVector.get(edge_ind).m_lEdgeID)
				{
					flag = false;
					break;
				}
				int parent_ind = parent[edge_ind].ed_ind[v_pos];
				v_pos = parent[edge_ind].v_pos[v_pos];
				edge_ind = parent_ind;
			}
			if(flag)
				cost += vecRules.get(ruleIndex).cost;
		}
		return cost;
	}

	
	double construct_path(int ed_id, int v_pos)
	{
		if(parent[ed_id].ed_ind[v_pos] == -1)
		{
			Path_element pelement=new Path_element();
			
			GraphEdgeInfo cur_edge = m_vecEdgeVector.get(ed_id);
			double dCost=this.get_cost(cur_edge, true);
			double dReverseCost=this.get_cost(cur_edge, false);
			if(v_pos == 0)
			{
				pelement.vertex_id = (int)cur_edge.m_lStartNode;
				pelement.cost = dCost;
			}
			else
			{
				pelement.vertex_id = (int)cur_edge.m_lEndNode;
				pelement.cost = dReverseCost;
			}
			pelement.edge_id = (int)cur_edge.m_lEdgeID;

			m_vecPath.add(pelement);
			return pelement.cost;
		}
		double ret = construct_path(parent[ed_id].ed_ind[v_pos], parent[ed_id].v_pos[v_pos]);
		
		Path_element pelement = new Path_element();
		GraphEdgeInfo cur_edge = m_vecEdgeVector.get(ed_id);

		if(v_pos == 0)
		{
			pelement.vertex_id = (int)cur_edge.m_lStartNode;
			pelement.cost = m_dCost[ed_id].endCost - ret;// cur_edge.m_dCost;
			ret = m_dCost[ed_id].endCost;
		}
		else
		{
			pelement.vertex_id = (int)cur_edge.m_lEndNode;
			pelement.cost = m_dCost[ed_id].startCost - ret;
			ret = m_dCost[ed_id].startCost;
		}
		pelement.edge_id = (int)cur_edge.m_lEdgeID;

		m_vecPath.add(pelement);

		return ret;
	}
	
	boolean get_single_cost(double total_cost, ArrayList<Path_element> path)
	{
		GraphEdgeInfo start_edge_info = m_vecEdgeVector.get(m_mapEdgeId2Index.get(m_lStartEdgeId).intValue());
		double dCost=this.get_cost(start_edge_info, true);
		double dReverseCost=this.get_cost(start_edge_info, false);
		
		if(m_dEndPart >= m_dStartpart)
		{
			if(dCost >= 0.0 && dCost * (m_dEndPart - m_dStartpart) <= total_cost)
			{
				path.clear();
				Path_element node=new Path_element();
				node.vertex_id = -1;
				node.edge_id = (int)m_lStartEdgeId;
				node.cost = dCost * (m_dEndPart - m_dStartpart);
				path.add(node);

				return true;
			}
		}
		else
		{
			if(dReverseCost >= 0.0 && dReverseCost * (m_dStartpart - m_dEndPart) <= total_cost)
			{
				path.clear();
				Path_element node=new Path_element();
				node.vertex_id = -1;
				node.edge_id = (int)m_lStartEdgeId;
				node.cost = dReverseCost * (m_dStartpart - m_dEndPart);
				path.add(node);
				
				return true;
			}
		}
		return false;
		
	}
	
	/*
	public static void main(String[] args){
		TRkShortestPath routing_instance=new TRkShortestPath();

		//Date time1=new Date();
		String database="mydb";
		String roadmap_table="edge_table";
		String restriction_table="restrictions_table";
		int start_edge_id=3;
		double start_part=0.5;
		int end_edge_id=13;
		double end_part=0.5;
		boolean update_graph=false;
		int top_k=5;
		ArrayList<Path> paths=new ArrayList<Path>();
		double max_delay=1.5;
		
		/*
		routing_instance.get_shortest_paths(database, roadmap_table, restriction_table, update_graph, start_edge_id, start_part, end_edge_id, end_part, top_k, max_delay, paths);
		
		//(database, roadmap_table, restriction_table, update_graph, start_edge_id, start_part, end_edge_id, end_part, top_k, paths)shortest_path("mydb", "edge_table", "intersections_table", 1, 0.5, 10, 0.5, path);
		//Date time2=new Date();
    	//System.out.println("Routing Time:"+(time2.getTime()-time1.getTime()));
		System.out.println("-------------------------------------------------------");
		for(int j=0; j<paths.size();j++){
			ArrayList<Path_element> path=paths.get(j).path;
	    	for(int i=0;i<path.size();i++){
	    		System.out.println(path.get(i).vertex_id + ", " + path.get(i).edge_id + ", " + path.get(i).cost);
	    	}
	    	System.out.println();
		}
		System.out.println("------------------------------------------------------");
		
		routing_instance.get_shortest_paths(database, roadmap_table, restriction_table, update_graph, start_edge_id, start_part, end_edge_id, end_part, top_k, max_delay,paths);
		
		//(database, roadmap_table, restriction_table, update_graph, start_edge_id, start_part, end_edge_id, end_part, top_k, paths)shortest_path("mydb", "edge_table", "intersections_table", 1, 0.5, 10, 0.5, path);
		//Date time2=new Date();
    	//System.out.println("Routing Time:"+(time2.getTime()-time1.getTime()));
		System.out.println("-------------------------------------------------------");
		for(int j=0; j<paths.size();j++){
			ArrayList<Path_element> path=paths.get(j).path;
	    	for(int i=0;i<path.size();i++){
	    		System.out.println(path.get(i).vertex_id + ", " + path.get(i).edge_id + ", " + path.get(i).cost);
	    	}
	    	System.out.println();
		}
    	
    	
    	for(int j=0; j<100; j++){
    		path.clear();
	    	//time1=new Date();
			routing_instance.shortest_path("mydb", "oneway_test", "intersection_test", 34650, 0.5, 12810, 0.5, path);
			//time2=new Date();
	    	//System.out.println("Routing Time:"+(time2.getTime()-time1.getTime()));
	    	if(j>=10){
	    		continue;
	    	}
	    	//System.out.println(j+"	:");
	    	for(int i=0;i<path.size();i++){
	    		//System.out.println(path.get(i).vertex_id + ", " + path.get(i).edge_id + ", " + path.get(i).cost);
	    	}
    	}
    	//System.out.println("finished");
	}*/
}

class Pair{
	double first;
	double second;
	
	Pair(double f, double s){
		first=f;
		second=s;
	}
}

class Path{
	ArrayList<Path_element> path;
	boolean edgeset_calcualted=false;
	HashSet<Long> edge_set;
	boolean cost_calcualted=false;
	private double cost=-1;
	
	Path(){
		path=new ArrayList<Path_element>();
		edge_set=new HashSet<Long>();
		cost=-1;
		cost_calcualted=false;
		edgeset_calcualted=false;
	}
	
	Path(ArrayList<Path_element> p){
		path=new ArrayList<Path_element>(p);
		edge_set=new HashSet<Long>();
		cost=-1;
		cost_calcualted=false;
		edgeset_calcualted=false;
	}
	
	public void build_edgeset(){
		for(int i=0; i<path.size();i++){
			edge_set.add(path.get(i).edge_id);
		}
		edgeset_calcualted=true;
	}
	
	public HashSet<Long> get_edgeset(){
		if(edgeset_calcualted){
			return edge_set;
		}
		else{
			this.build_edgeset();
			return edge_set;
		}
	}
	
	public boolean equals(Path new_path){
		
		if(this.get_cost()!=new_path.get_cost() || this.length()!=new_path.length()){
			return false;
		}
		
		for(int i=0; i<path.size();i++){
			if(this.path.get(i).edge_id!=new_path.path.get(i).edge_id || this.path.get(i).vertex_id!=new_path.path.get(i).vertex_id){
				return false;
			}
		}
		
		return true;
	}
	
	public double get_cost(){
		/*if(cost_calcualted){
			return cost;
		}
		else{*/
			cost=0;
			for(int i=0; i<path.size();i++){
				cost+=path.get(i).cost;
			}
			cost_calcualted=true;
			return cost;
		//}
	}
	
	public int length(){
		return path.size();
	}
	
	public void print(){
		for(int i=0; i<path.size();i++){
			System.out.println(path.get(i).vertex_id + ", " + path.get(i).edge_id + ", " + path.get(i).cost);
		}
		System.out.println();
	}
}
