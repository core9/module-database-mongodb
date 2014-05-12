package io.core9.plugin.database.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.xeoh.plugins.base.annotations.PluginImplementation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;
import com.mongodb.gridfs.GridFSInputFile;

@PluginImplementation
public class MongoDatabaseImpl implements MongoDatabase {
	
	private final Map<String,MongoClient> clients = new HashMap<String,MongoClient>();
	private String masterDB = "";
	
	@Override
	public String getMasterDBName() {
		return masterDB;
	}
	
	public void execute() {}
	
	public MongoDatabaseImpl() {
		MongoClientURI uri = null;
		if(System.getProperty("core9.dburi") != null) {
			uri = new MongoClientURI(System.getProperty("core9.dburi"));
		} else if(System.getenv("CORE9_DB_URI") != null) {
			uri = new MongoClientURI(System.getenv("CORE9_DB_URI"));
		} else {
			uri = new MongoClientURI("mongodb://localhost/core9");
		}
		try {
			this.masterDB = uri.getDatabase();
			this.clients.put(uri.getDatabase(), new MongoClient(uri));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public DBCollection getCollection(String db, String coll) {
		return clients.get(db).getDB(db).getCollection(coll);
	}
	
	@Override
	public  DB getDb(String db) {
		return clients.get(db).getDB(db);
	}
	
	@Override
	public void addDatabase(String db, String username, String password) throws UnknownHostException {
		if(username == null || username.equals("")) {
			this.clients.put(db, new MongoClient(this.clients.get(masterDB).getAddress()));
		} else {
			MongoCredential credential = MongoCredential.createMongoCRCredential(username, db, password.toCharArray());
			this.clients.put(db, new MongoClient(this.clients.get(masterDB).getAddress(), Arrays.asList(credential)));
		}
	}
	
	@Override
	public void addDatabase(String host, String db, String username, String password) throws UnknownHostException {
		ServerAddress add = new ServerAddress(host);
		if(username == null || username.equals("")) {
			this.clients.put(db, new MongoClient(add));
		} else {
			MongoCredential credential = MongoCredential.createMongoCRCredential(username, db, password.toCharArray());
			this.clients.put(db, new MongoClient(add, Arrays.asList(credential)));
		}
	}

	@Override
	public void setBackend(String db, MongoClient mongo) {
		this.clients.put(db, mongo);
	}

	@Override
	public void setCollection(String db, String coll) {
		this.clients.get(db).getDB(db).getCollection(coll);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> getSingleResult(String db, String coll, Map<String, Object> query) {
		DBObject obj = this.clients.get(db).getDB(db).getCollection(coll).findOne(new BasicDBObject(query));
		if(obj != null) {
			return obj.toMap();
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> getSingleResult(String db, String coll, Map<String, Object> query,  Map<String, Object> fields) {
		DBObject obj = this.clients.get(db).getDB(db).getCollection(coll).findOne(new BasicDBObject(query), new BasicDBObject(fields));
		if(obj != null) {
			return obj.toMap();
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Map<String, Object>> getMultipleResults(String db, String coll, Map<String, Object> query) {
		List<Map<String, Object>> result = new ArrayList<Map<String,Object>>();
		if(this.clients.get(db) == null) {
			return result;
		}
		DBCursor cursor = this.clients.get(db).getDB(db).getCollection(coll).find(new BasicDBObject(query));
		while(cursor.hasNext()) {
			result.add(cursor.next().toMap());
		}
		return result;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Map<String, Object>> getMultipleResults(String db, String coll, Map<String, Object> query, Map<String, Object> fields) {
		List<Map<String, Object>> result = new ArrayList<Map<String,Object>>();
		DBCursor cursor = this.clients.get(db).getDB(db).getCollection(coll).find(new BasicDBObject(query), new BasicDBObject(fields));
		while(cursor.hasNext()) {
			result.add(cursor.next().toMap());
		}
		return result;
	}
	

	@Override
	public Map<String, Object> findByID(String db, String coll, String id) {
		Map<String, Object> query = new HashMap<String,Object>();
		query.put("_id", id);
		return getSingleResult(db, coll, query);
	}
	
	@Override
	public Map<String, Object> findByField(String db, String collection, String field, String value) {
		Map<String, Object> query = new HashMap<String,Object>();
		query.put(field, value);
		return null;
	}
	
	@Override
	public List<Map<String,Object>> queryStaticFiles(String db, String bucket, Map<String,Object> query) {
		List<Map<String, Object>> result = new ArrayList<Map<String,Object>>();
		GridFS myFS = new GridFS(this.clients.get(db).getDB(db), bucket);
		for(GridFSDBFile file : myFS.find(new BasicDBObject(query))) {
			result.add(convertFileToMap(file));
		}
		return result;
	}

	@Override
	public Map<String,Object> queryStaticFile(String db, String bucket, Map<String,Object> query) {
		GridFS myFS = new GridFS(this.clients.get(db).getDB(db), bucket);
		GridFSDBFile file = myFS.findOne(new BasicDBObject(query));
		if(file == null) {
			return null;
		}
		return convertFileToMap(file);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Map<String,Object> saveStaticFile(String db, String bucket, Map<String,Object> file, String fileId) {
		Map<String,Object> query = new HashMap<String,Object>();
		query.put("_id", file.get("_id"));
		String folder = (String) ((Map<String,Object>) file.get("metadata")).get("folder");
		String filename = folder + file.get("filename");
		GridFS myFS = new GridFS(this.clients.get(db).getDB(db), bucket);
		GridFSFile foundFile = myFS.findOne(new BasicDBObject(query));
		foundFile.put("filename", filename);
		if(file.get("contentType") != null) {
			foundFile.put("contentType", (String) file.get("contentType"));
		}
		foundFile.setMetaData(new BasicDBObject((Map<String,Object>) file.get("metadata")));
		foundFile.save();
		return convertFileToMap(foundFile);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Map<String,Object> addStaticFile(String db, String bucket, Map<String,Object> file, InputStream stream) throws IOException {
		if(this.clients.get(db) == null) {
			return null;
		}
		GridFS myFS = new GridFS(this.clients.get(db).getDB(db), bucket);
		// TODO check if file is updated before removing and adding
		Map<String,Object> metadata = (Map<String,Object>) file.get("metadata");
		String folder = "";
		if(metadata != null && metadata.get("folder") != null) {
			folder = (String) metadata.get("folder");
		}
		String filename = folder + file.get("filename");
		myFS.remove(filename.replace("\\", "/"));
		GridFSInputFile newFile = myFS.createFile(stream);
		if(file.get("_id") != null) {
			newFile.setId(file.get("_id"));
		} else {
			newFile.setId(GUID.getUUID());
		}
		newFile.setFilename(filename.replace("\\", "/"));
		if(metadata != null) {
			newFile.setMetaData(new BasicDBObject((Map<String,Object>) file.get("metadata")));
		}
		if(file.get("contentType") != null) {
			newFile.setContentType((String) file.get("contentType"));
		}
		newFile.save();
		return convertFileToMap(newFile);
	}
	
	private Map<String,Object> convertFileToMap(GridFSFile file) {
		Map<String,Object> objMap = new HashMap<String,Object>();
		String filename = file.getFilename();
		if(file.getMetaData() != null) {
			if(file.getMetaData().get("folder") != null) {
				String folder = (String) file.getMetaData().get("folder");
				filename = filename.replace(folder, "");
			}
			objMap.put("metadata", file.getMetaData().toMap());
		}
		objMap.put("_id", file.getId());
		objMap.put("filename", filename);
		objMap.put("contentType", file.getContentType());
		objMap.put("uploadDate", file.getUploadDate());
		objMap.put("md5", file.getMD5());
		return objMap;
	}

	@Override
	@Deprecated
	public void addStaticFile(String db, String filename, InputStream stream) throws IOException {
		Map<String,Object> metadata = new HashMap<String,Object>();
		metadata.put("filename", filename);
		addStaticFile(db, "static", metadata, stream);
	}
	
	@Override
	public InputStream getStaticFile(String db, String bucket, String filename) {
		String file = filename.replace("\\", "/");
		GridFS myFS = new GridFS(this.clients.get(db).getDB(db), bucket);
		try {
			return myFS.findOne(file).getInputStream();
        } catch (NullPointerException e) {
	        System.out.println("trying to get file : " + file);
	        return null;
        }
	}
	

	@Override
	public void removeStaticFile(String db, String bucket, String fileId) {
		GridFS myFS = new GridFS(this.clients.get(db).getDB(db), bucket);
		DBObject query = new BasicDBObject();
		query.put("_id", fileId);
		myFS.remove(query);
	}

	@Deprecated
	@Override
	public InputStream getStaticFile(String db, String filename) {
		return getStaticFile(db, "static", filename);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public String upsert(String db, String collection, Map<String, Object> doc, Map<String, Object> query) {
		if(doc.get("$set") != null) {
			String id = (String) ((Map<String,Object>) doc.get("$set")).remove("_id");
			//Can be fixed with $setOnInsert -> [_id, GUID.getUUID()] on MongoDB 2.5.5+
			if(id != null) {
				query.put("_id", id);
			} else if(query.isEmpty()) {
				query.put("_id", GUID.getUUID());
			}
		}
		if(doc.get("$set") == null && doc.get("_id") == null) {
			doc.put("_id", GUID.getUUID());
		}
		if(query == null){
			query = new HashMap<>();
		}
		this.clients.get(db).getDB(db).getCollection(collection).update(new BasicDBObject(query), new BasicDBObject(doc), true, false);
		return (String) query.get("_id");
	}

	@Override
	public void delete(String db, String collection, Map<String, Object> query) {
		this.clients.get(db).getDB(db).getCollection(collection).remove(new BasicDBObject(query));		
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<String> getDistinctStrings(String db, String coll, String key, Map<String, Object> query) {
		return this.clients.get(db).getDB(db).getCollection(coll).distinct(key, new BasicDBObject(query));
	}

	@Override
	public InputStream getStaticFile(String db, String bucket, Map<String, Object> query) {
		GridFS myFS = new GridFS(this.clients.get(db).getDB(db), bucket);
		try {
			return myFS.findOne(new BasicDBObject(query)).getInputStream();
        } catch (NullPointerException e) {
	        return null;
        }
	}

	@Override
	public void saveStaticFileContents(String db, String bucket, Map<String, Object> query, InputStream stream) {
		if(this.clients.get(db) == null) {
			return;
		}
		GridFS myFS = new GridFS(this.clients.get(db).getDB(db), bucket);
		GridFSDBFile oldFile = myFS.findOne(new BasicDBObject(query));
		GridFSInputFile newFile = myFS.createFile(stream);
		newFile.setFilename(oldFile.getFilename());
		newFile.setId(oldFile.getId());
		newFile.setContentType(oldFile.getContentType());
		newFile.setMetaData(oldFile.getMetaData());
		myFS.remove(new BasicDBObject(query));
		newFile.save();
	}
}
