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

public class CommonWords {
	public static void main(String[] args) throws Exception {
		writeToMongoDB();
	}
	
	public static void writeToMongoDB() throws Exception{
		Connection con = SQLConnection.getSqlConnection();
		PreparedStatement statement = con.prepareStatement("SELECT r.text FROM `review` r\r\n" + 
				"JOIN `business` b\r\n" + 
				"WHERE r.business_id = b.id\r\n" + 
				"AND b.name = \"Chipotle Mexican Grill\"");
		
		ResultSet result = statement.executeQuery();
		
		MongoClientURI uri = new MongoClientURI("mongodb://root:root@ds019976.mlab.com:19976/mymongodb");

		MongoClient mongoClient = new MongoClient(uri);
		MongoDatabase database = mongoClient.getDatabase("mymongodb");
		
		database.getCollection("CommonWords").drop();
		
		MongoCollection<Document> collection = database.getCollection("CommonWords");
		
		Map<String, Integer> map = new HashMap<>();

		while(result.next()) {
			String text = result.getString("text");
			String[] words = text.split("\\s+");
			
			for (int i = 0; i < words.length; i++) {
			    String word = words[i].replaceAll("[^\\w]", "").toLowerCase();
			    Integer n = map.get(word);
			    n = (n == null) ? 1 : ++n;
		        map.put(word, n);
//		        System.out.print(word + " ");
//		        System.out.println(n);
			}
		}
		
		List<Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
		list.sort(Entry.comparingByValue());
		
//		for (int i = list.size() - 1; i > list.size() - 11; i--) {
//			System.out.print(list.get(i).getValue() + " ");
//			System.out.println(list.get(i).getKey());
//		}
		
		int rank = 0;
		int index = list.size() - 1;
		String[] excluded = {"the", "and", "I", "to", "a", "is", "was", "of", "for", "this"};
//		String[] excluded = {};
		
		while (rank < 10 && index >= 0) {
			if (!Arrays.asList(excluded).contains(list.get(index).getKey())) {
//				System.out.print(list.get(index).getValue() + " ");
//				System.out.println(list.get(index).getKey());
				Document doc1 = new Document();
				doc1.put("word", list.get(index).getKey());
				doc1.put("appearances", list.get(index).getValue());
				collection.insertOne(doc1);
				++rank;
			}
			--index;
		}
	}
}
