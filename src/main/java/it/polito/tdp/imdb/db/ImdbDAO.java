package it.polito.tdp.imdb.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import it.polito.tdp.imdb.model.Actor;
import it.polito.tdp.imdb.model.Adiacenza;
import it.polito.tdp.imdb.model.Director;
import it.polito.tdp.imdb.model.Movie;

public class ImdbDAO {
	
	public void listAllActors(Map<Integer,Actor>idMap){
		String sql = "SELECT * FROM actors";
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {
				if(!idMap.containsKey(res.getInt("id"))) {
				Actor a = new Actor(res.getInt("id"), res.getString("first_name"), res.getString("last_name"),
						res.getString("gender"));
				idMap.put(a.getId(), a);
				}
			}
			conn.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}
	
	public List<Movie> listAllMovies(){
		String sql = "SELECT * FROM movies";
		List<Movie> result = new ArrayList<Movie>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Movie movie = new Movie(res.getInt("id"), res.getString("name"), 
						res.getInt("year"), res.getDouble("rank"));
				
				result.add(movie);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	public List<Director> listAllDirectors(){
		String sql = "SELECT * FROM directors";
		List<Director> result = new ArrayList<Director>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Director director = new Director(res.getInt("id"), res.getString("first_name"), res.getString("last_name"));
				
				result.add(director);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<String>getGeneri(){
		String sql="SELECT DISTINCT mg.genre "
				+ "FROM movies_genres AS mg "
				+ "ORDER BY mg.genre";
		List<String>generi=new ArrayList<String>();
		Connection conn = DBConnect.getConnection();
		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {
				generi.add(res.getString("mg.genre"));
			}
			conn.close();
			return generi;
		}catch(SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<Actor>getVertici(Map<Integer,Actor>idMap,String genere){
		String sql="SELECT DISTINCT a.id,a.first_name,a.last_name,a.gender "
				+ "FROM movies_genres AS mg,movies AS m,roles AS r,actors AS a "
				+ "WHERE a.id=r.actor_id AND r.movie_id=m.id AND "
				+ "m.id=mg.movie_id AND mg.genre=? "
				+ "ORDER BY a.last_name";
		List<Actor>attori=new ArrayList<Actor>();
		try {
			Connection conn = DBConnect.getConnection() ;
			PreparedStatement st = conn.prepareStatement(sql) ;
			st.setString(1, genere);
		    ResultSet res = st.executeQuery() ;
		    while(res.next()) {
		    	if(idMap.containsKey(res.getInt("a.id"))) {
		    		attori.add(idMap.get(res.getInt("a.id")));
		    	}
		    }
		    conn.close();
		    return attori;
		}catch(SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<Adiacenza>getAdiacenze(Map<Integer,Actor>idMap,String genere){
		String sql="SELECT r1.actor_id,r2.actor_id,COUNT(m.id) AS peso "
				+ "FROM roles AS r1,roles AS r2,movies AS m,movies_genres AS mg "
				+ "WHERE r1.actor_id>r2.actor_id AND "
				+ "r1.movie_id=m.id AND r1.movie_id=r2.movie_id AND "
				+ "m.id=mg.movie_id AND mg.genre=? "
				+ "GROUP BY r1.actor_id,r2.actor_id";
		List<Adiacenza>adiacenze=new ArrayList<Adiacenza>();
		try {
			Connection conn = DBConnect.getConnection() ;
			PreparedStatement st = conn.prepareStatement(sql) ;
			st.setString(1, genere);
		    ResultSet res = st.executeQuery() ;
		    while(res.next()) {
		    	adiacenze.add(new Adiacenza(idMap.get(res.getInt("r1.actor_id")),idMap.get(res.getInt("r2.actor_id")),res.getDouble("peso")));
		    }
		    conn.close();
		    return adiacenze;
		}catch(SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
}
