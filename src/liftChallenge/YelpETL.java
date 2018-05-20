package liftChallenge;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class YelpETL {
	public static void main(String[] args) throws Exception {
		top20Cities();
	}
	
	public static Connection getSqlConnection() throws Exception {
		try {
			String driver = "com.mysql.jdbc.Driver";
			String url = "jdbc:mysql://192.168.10.128:3306/yelp_db";
			String username = "root";
			String password = "root";
			Class.forName(driver);
			
			Connection conn = DriverManager.getConnection(url, username, password);
			System.out.println("SQL Connected");
			return conn;
		} catch(Exception e) {
			System.out.println(e);
		}
		
		return null;
	}
	
	public static void top20Cities() throws Exception{
		Connection con = getSqlConnection();
		PreparedStatement statement = con.prepareStatement("SELECT city, SUM(review_count) as reviews, AVG(stars) as avgStars FROM `business` WHERE 1 GROUP BY city ORDER BY SUM(review_count) DESC LIMIT 20");
		
		ResultSet result = statement.executeQuery();
		
		ArrayList<String> array = new ArrayList<String>();
		while(result.next()) {
			System.out.println(result.getString("city"));
			
			array.add(result.getString("city"));
		}
		System.out.println("Selected");
		
		
		MongoClientURI uri = new MongoClientURI(
		    "mongodb://root:<PASSWORD>@mymongodb-shard-00-00-ixsrh.mongodb.net:27017,mymongodb-shard-00-01-ixsrh.mongodb.net:27017,mymongodb-shard-00-02-ixsrh.mongodb.net:27017/test?ssl=true&replicaSet=MyMongoDB-shard-0&authSource=admin&retryWrites=true");

		MongoClient mongoClient = new MongoClient(uri);
		MongoDatabase database = mongoClient.getDatabase("test");
		
		database.getCollection("top20Cities").drop();
		
		MongoCollection<Document> collection = database
				.getCollection("top20Cities");
		
		//collection.insertOne(updateObj_tMongoDBOutput_1);
	}
}
