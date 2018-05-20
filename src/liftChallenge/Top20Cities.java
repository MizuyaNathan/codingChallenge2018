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

public class Top20Cities {
	public static void main(String[] args) throws Exception {
		writeToMongoDB();
	}
	
	public static void writeToMongoDB() throws Exception{
		Connection con = SQLConnection.getSqlConnection();
		PreparedStatement statement = con.prepareStatement("SELECT city, SUM(review_count) as reviews, AVG(stars) as avgStars FROM `business` WHERE 1 GROUP BY city ORDER BY SUM(review_count) DESC LIMIT 20");
		
		ResultSet result = statement.executeQuery();
		
		MongoClientURI uri = new MongoClientURI("mongodb://root:root@ds019976.mlab.com:19976/mymongodb");
//		MongoClientURI uri = new MongoClientURI(
//		    "mongodb+srv://root:<root>@mymongodb-ixsrh.mongodb.net/test?retryWrites=true");

//		MongoClient mongoClient = new MongoClient(serverAddress, 
//				Collections.singletonList(mongoCredential), MongoClientOptions.builder().sslEnabled(true).socketFactory(getNoopSslSoketFactory()).build());
		MongoClient mongoClient = new MongoClient(uri);
		MongoDatabase database = mongoClient.getDatabase("mymongodb");
		
		database.getCollection("Top20Cities").drop();
		
		MongoCollection<Document> collection = database.getCollection("Top20Cities");
		
//		ArrayList<String> array = new ArrayList<String>();
		while(result.next()) {
//			System.out.println(result.getString("city"));
			
//			array.add(result.getString("city"));
			
			Document doc1 = new Document();
			doc1.put("city", result.getString("city"));
			doc1.put("reviews", result.getString("reviews"));
			doc1.put("avgStars", result.getString("avgStars"));
			collection.insertOne(doc1);
		}
	}
	
//	private static SSLSocketFactory getNoopSslSocketFactory() {
//	    SSLContext sslContext;
//	    try {
//	        sslContext = SSLContext.getInstance("SSL");
//
//	        // set up a TrustManager that trusts everything
//	        sslContext.init(null, new TrustManager[] { new X509TrustManager() {
//	            @Override
//	            public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException { }
//
//	            @Override
//	            public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException { }
//
//	            @Override
//	            public X509Certificate[] getAcceptedIssuers() {
//	                return new X509Certificate[0];
//	            }
//	        }}, new SecureRandom());
//	    } catch (NoSuchAlgorithmException | KeyManagementException e) {
//	        throw new RuntimeException(e);
//	    }
//	    return sslContext.getSocketFactory();
//	}
}
