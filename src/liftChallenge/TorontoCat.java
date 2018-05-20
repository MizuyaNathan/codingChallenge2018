package liftChallenge;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import liftChallenge.SQLConnection;

public class TorontoCat {
	public static void main(String[] args) throws Exception {
		writeToMongoDB();
	}
	
	public static void writeToMongoDB() throws Exception{
		Connection con = SQLConnection.getSqlConnection();
		PreparedStatement statement = con.prepareStatement("SELECT c.* FROM `category` c\r\n" + 
				"JOIN `business` b\r\n" + 
				"ON c.business_id = b.id\r\n" + 
				"WHERE b.city = \"Toronto\"");
		
		ResultSet result = statement.executeQuery();
		
		MongoClientURI uri = new MongoClientURI("mongodb://root:root@ds019976.mlab.com:19976/mymongodb");

		MongoClient mongoClient = new MongoClient(uri);
		MongoDatabase database = mongoClient.getDatabase("mymongodb");
		
		database.getCollection("TorontoCat").drop();
		
		MongoCollection<Document> collection = database.getCollection("TorontoCat");
		
		Map<String, Integer> map = new HashMap<>();

		while(result.next()) {
			String category = result.getString("category");
		    Integer n = map.get(category);
		    n = (n == null) ? 1 : ++n;
	        map.put(category, n);
		}
		
		List<Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
		list.sort(Entry.comparingByValue());
		
//		for (int i = list.size() - 1; i > list.size() - 11; i--) {
//			System.out.print(list.get(i).getValue() + " ");
//			System.out.println(list.get(i).getKey());
//		}
		
		for (int i = list.size() - 1; i >= 0; --i) {
			Document doc1 = new Document();
			doc1.put("category", list.get(i).getKey());
			doc1.put("category count", list.get(i).getValue());
			collection.insertOne(doc1);
		}
	}
}
