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

public class MAGReview_Raw {
	public static void main(String[] args) throws Exception {
		writeToMongoDB();
	}
	
	public static void writeToMongoDB() throws Exception{
		Connection con = SQLConnection.getSqlConnection();
		PreparedStatement statement = con.prepareStatement("SELECT r.user_id, r.text FROM `review` r\r\n" + 
				"WHERE r.business_id = \"4JNXUYY8wbaaDmk3BPzlWw\"\r\n" + 
				"AND date > now() - INTERVAL 1 YEAR");
		
		ResultSet result = statement.executeQuery();
		
		MongoClientURI uri = new MongoClientURI("mongodb://root:root@ds019976.mlab.com:19976/mymongodb");

		MongoClient mongoClient = new MongoClient(uri);
		MongoDatabase database = mongoClient.getDatabase("mymongodb");
		
		database.getCollection("MAGReview_Raw").drop();
		
		MongoCollection<Document> collection = database.getCollection("MAGReview_Raw");

		while(result.next()) {
			Document doc1 = new Document();
			doc1.put("user_id", result.getString("user_id"));
			doc1.put("text", result.getString("text"));
			collection.insertOne(doc1);
		}
	}
}
