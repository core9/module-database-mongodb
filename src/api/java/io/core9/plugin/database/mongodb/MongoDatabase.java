package io.core9.plugin.database.mongodb;

import io.core9.core.plugin.Core9Plugin;
import io.core9.plugin.database.Database;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import com.mongodb.MongoClient;

public interface MongoDatabase extends Database, Core9Plugin {
	
	void setBackend(String db, MongoClient mongo);
	
	void addDatabase(String db, String username, String password) throws UnknownHostException;

	void setCollection(String db, String coll);
	
	@Deprecated
	void addStaticFile(String db, String filename, InputStream stream) throws IOException;
	
	@Deprecated
	InputStream getStaticFile(String db, String filename) throws IOException;

	List<Map<String, Object>> getMultipleResults(String db, String coll, Map<String, Object> query,
            Map<String, Object> fields);
	
	List<String> getDistinctStrings(String db, String coll, String key, Map<String,Object> query);

	InputStream getStaticFile(String db, String bucket, String filename);

	List<Map<String, Object>> queryStaticFiles(String db, String bucket, Map<String, Object> query);

	Map<String, Object> addStaticFile(String db, String bucket, Map<String, Object> file,
			InputStream stream) throws IOException;

	Map<String, Object> queryStaticFile(String db, String bucket,
			Map<String, Object> query);

	Map<String, Object> saveStaticFile(String db, String bucket,
			Map<String, Object> metadata, String fileId);

	void removeStaticFile(String db, String bucket, String fileId);

	InputStream getStaticFile(String db, String bucket, Map<String, Object> query);

	void saveStaticFileContents(String db, String bucket,
			Map<String, Object> query, InputStream stream);
}
