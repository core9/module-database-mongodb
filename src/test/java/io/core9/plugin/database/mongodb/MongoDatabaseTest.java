package io.core9.plugin.database.mongodb;

import static org.junit.Assert.*;
import io.core9.plugin.database.mongodb.MongoDatabase;
import io.core9.plugin.database.mongodb.MongoDatabaseImpl;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
//import java.util.UUID;

import org.junit.Before;
import org.junit.Test;




import com.github.fakemongo.Fongo;
//import com.foursquare.fongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class MongoDatabaseTest {
	private Fongo fongo;
	private MongoDatabase db;
	
	@Before
	public void setUp() throws UnknownHostException {
		fongo = new Fongo("mongo server 1");
		
		db = new MongoDatabaseImpl();
		db.setBackend("test", fongo.getMongo());
	
		DBCollection coll = fongo.getDB("test").getCollection("products");
		coll.insert(new BasicDBObject("title", "test1"));
		coll.insert(new BasicDBObject("_id", "testguid"));
	}
	
	@Test
	public void testCount() {
		DBCollection coll = fongo.getDB("test").getCollection("products");
		assertEquals(coll.getCount(), 2);
	}
	
	@Test
	public void testFind() {
		Map<String, Object> query = new HashMap<String, Object>();
		query.put("title", "test1");
		Map<String, Object> result = db.getSingleResult("test", "products", query);
		assertEquals(result.get("title"), "test1");
	}
	
	@Test
	public void testRemove() {
		Map<String, Object> query = new HashMap<String, Object>();
		query.put("_id", "testguid");
		DBCollection coll = fongo.getDB("test").getCollection("products");
		assertEquals(coll.getCount(), 2);
		db.delete("test", "products", query);
		assertEquals(coll.getCount(), 1);
	}
	
	@Test
	public void testNonExistingFile(){
	    
	}

}
