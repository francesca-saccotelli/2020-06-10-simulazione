package it.polito.tdp.imdb.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import it.polito.tdp.imdb.db.ImdbDAO;

public class Model {

	ImdbDAO dao;
	private Graph<Actor,DefaultWeightedEdge>grafo;
	Map<Integer,Actor>idMap;
	
	public Model() {
		dao=new ImdbDAO();
		idMap=new HashMap<>();
		this.dao.listAllActors(idMap);
	}
	
	public List<String>getGeneri(){
		return this.dao.getGeneri();
	}
	
	public void creaGrafo(String genere) {
		grafo=new SimpleWeightedGraph<>(DefaultWeightedEdge.class);
		Graphs.addAllVertices(this.grafo, this.dao.getVertici(idMap, genere));
		for(Adiacenza a:this.dao.getAdiacenze(idMap,genere)) {
			if(this.grafo.containsVertex(a.getA1())&& this.grafo.containsVertex(a.getA2())) {
				Graphs.addEdgeWithVertices(this.grafo, a.getA1(), a.getA2(), a.getPeso());
			}
		}
	}
	public int vertexNumber() {
		return this.grafo.vertexSet().size();
	}
	
	public int edgeNumber() {
		return this.grafo.edgeSet().size();
	}
	
	public List<Actor>getVertici(){
		List<Actor>attori=new ArrayList<>(this.grafo.vertexSet());
		return attori;
	}
	
	public List<Actor> getAttoriSimili(Actor a) {
		ConnectivityInspector<Actor,DefaultWeightedEdge>conn=new ConnectivityInspector<Actor,DefaultWeightedEdge>(grafo); 
		List<Actor> lista = new ArrayList<>(conn.connectedSetOf(a));
		lista.remove(a);
		Collections.sort(lista);
		return lista ;
	}
}
