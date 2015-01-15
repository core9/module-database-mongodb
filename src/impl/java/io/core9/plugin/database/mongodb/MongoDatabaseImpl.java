package io.core9.plugin.database.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.minidev.json.JSONArray;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import net.xeoh.plugins.base.annotations.PluginImplementation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;
import com.mongodb.gridfs.GridFSInputFile;

/**
 * TODO:
 * 	- Phase out get(db).getDB(db) by storing 1 MongoClient
 *  - How do we store multiple hosts (via-via connection)?
 * @author mark
 *
 */
@PluginImplementation
public class MongoDatabaseImpl implements MongoDatabase {
	
	private final Map<String,MongoClient> clients = new HashMap<String,MongoClient>();
	private String masterDB = "server";
	
	@Override
	public String getMasterDBName() {
		return masterDB;
	}
	
	public void execute() {}
	
	public MongoDatabaseImpl() {
		try {
			List<ServerAddress> seeds = getSeeds();
			List<MongoCredential> credentials = getCredentials();
			for(MongoCredential credential : credentials) {
				this.clients.put(credential.getSource(), new MongoClient(seeds, credentials));
			}
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
			System.exit(1);
		}
//		MongoClientURI uri = null;
//		if(System.getProperty("core9.dburi") != null) {
//			uri = new MongoClientURI(System.getProperty("core9.dburi"));
//		} else if(System.getenv("CORE9_DB_URI") != null) {
//			uri = new MongoClientURI(System.getenv("CORE9_DB_URI"));
//		} else {
//			uri = new MongoClientURI("mongodb://localhost/core9");
//		}
//		try {
//			this.masterDB = uri.getDatabase();
//			MongoClient client = new MongoClient();
//			ReplicaSetStatus status = client.getReplicaSetStatus();
//			ServerAddress address = status.getMaster();
//			System.out.println(address.getHost() + ":" + address.getPort());
//			this.clients.put(uri.getDatabase(), new MongoClient(uri));
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
	}
	
	private List<MongoCredential> getCredentials() {
		List<MongoCredential> result = new ArrayList<MongoCredential>();
		String credentials = System.getProperty("core9.dbcredentials", System.getenv("CORE9_DB_CREDENTIALS"));
		if(credentials != null) {
			try {
				JSONArray credArray = (JSONArray) new JSONParser(JSONParser.MODE_PERMISSIVE).parse(credentials);
				for(Object cred : credArray) {
					if(!(cred instanceof JSONArray) || ((JSONArray) cred).size() != 3) {
						throw new ParseException(1, ParseException.ERROR_UNEXPECTED_TOKEN, "Use an array of arrays, like [[\"user\", \"db\", \"pass\"], ...]");
					} else {
						JSONArray credential = (JSONArray) cred;
						result.add(MongoCredential.createCredential((String) credential.get(0), (String) credential.get(1), ((String) credential.get(2)).toCharArray()));
					}
				}
			} catch (ParseException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		return result;
	}

	private List<ServerAddress> getSeeds() throws UnknownHostException {
		String hosts = System.getProperty("core9.dbhosts", System.getenv("CORE9_DB_HOSTS"));
		if(hosts != null) {
			 return MongoDatabaseImpl.parse(hosts);
		} else {
			return Arrays.asList(new ServerAddress());
		}
	}
	
	/**
     * Parse a string of addresses to a List of ServerAddress.
     *
     * @param addresses array of strings of form "host[:port],..."
     * @return a list of ServerAddress from the {@code addresses}
     * @throws UnknownHostException
     */
    public static List<ServerAddress> parse(String addresses) throws UnknownHostException {
        final String[] addrs = addresses.split(" *, *");
        final List<ServerAddress> result = new ArrayList<ServerAddress>(addrs.length);
        for (String addressString : addrs) {
            int idx = addressString.indexOf(':');
            result.add((idx == -1) ? new ServerAddress(addressString) : new ServerAddress(addressString.substring(0, idx),
                    Integer.parseInt(addressString.substring(idx + 1))));
        }
        return result;
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
		if(query == null){
			// Always use a default query
			query = new HashMap<>();
		}
		if(query.get("_id") == null) {
			// Set ID when not in query
			Map<String,Object> setOnInsert = (Map<String, Object>) doc.get("$setOnInsert");
			if(setOnInsert == null) {
				setOnInsert = new HashMap<String,Object>();
				doc.put("$setOnInsert", setOnInsert);
			}
			if(setOnInsert.get("_id") == null) {
				setOnInsert.put("_id", GUID.getUUID());
			}
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
