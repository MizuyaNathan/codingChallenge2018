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

public class USReview_Raw {
	public static void main(String[] args) throws Exception {
		writeToMongoDB();
	}
	
	public static void writeToMongoDB() throws Exception{
		Connection con = SQLConnection.getSqlConnection();
		PreparedStatement statement = con.prepareStatement("SELECT user_id, COUNT(DISTINCT business_id) AS USReviews FROM `review`\r\n" + 
				"WHERE business_id IN\r\n" + 
				"(SELECT id FROM `business`\r\n" + 
				"WHERE state NOT IN\r\n" + 
				"(\"AB\", \"BC\", \"MB\", \"NB\", \"NL\", \"NL\", \"NT\", \"NS\", \"NU\", \"ON\", \"PE\", \"QC\", \"SK\", \"YT\"))\r\n" + 
				"AND user_id IN\r\n" + 
				"(SELECT DISTINCT user_id FROM `review`\r\n" + 
				"WHERE business_id = \"4JNXUYY8wbaaDmk3BPzlWw\")\r\n" + 
				"GROUP BY user_id");
		
		ResultSet result = statement.executeQuery();
		
		MongoClientURI uri = new MongoClientURI("mongodb://root:root@ds019976.mlab.com:19976/mymongodb");

		MongoClient mongoClient = new MongoClient(uri);
		MongoDatabase database = mongoClient.getDatabase("mymongodb");
		
		database.getCollection("USReview_Raw").drop();
		
		MongoCollection<Document> collection = database.getCollection("USReview_Raw");

		while(result.next()) {
			Document doc1 = new Document();
			doc1.put("user_id", result.getString("user_id"));
			doc1.put("USReviews", result.getString("USReviews"));
			collection.insertOne(doc1);
		}
	}
}
