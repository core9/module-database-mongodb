package io.core9.plugin.database.mongodb.repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mongojack.DBQuery;
import org.mongojack.JacksonDBCollection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import io.core9.plugin.database.mongodb.MongoDatabase;
import io.core9.plugin.database.repository.CrudEntity;
import io.core9.plugin.database.repository.CrudRepository;
import io.core9.plugin.database.repository.DataUtils;
import io.core9.plugin.server.VirtualHost;

public class MongoCrudRepository<T extends CrudEntity> implements CrudRepository<T> {

	private final ObjectMapper mapper;
	private final String collection;
	private final Class<T> type;
	private MongoDatabase mongo;

	public MongoCrudRepository(Class<T> type, String collection, ObjectMapper mapper) {
		this.type = type;
		this.collection = collection;
		this.mapper = mapper;
	}
	
	public void setMongoDatabase(final MongoDatabase database) {
		this.mongo = database;
	}

	private String getCollectionName(T entity) {
		if (entity.retrieveCollectionOverride() != null) {
			return entity.retrieveCollectionOverride();
		}
		return collection;
	}

	@Override
	public T create(VirtualHost vhost, T entity) {
		return create((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), entity);
	}

	@Override
	public T create(String database, String prefix, T entity) {
		DBCollection collection = mongo.getCollection(database, prefix + getCollectionName(entity));
		JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
		return coll.insert(entity).getSavedObject();
	}

	@Override
	public T read(VirtualHost vhost, String id) {
		return read((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), id);
	}
	
	@Override
	public T read(VirtualHost vhost, String collection, String id) {
		return read((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), collection, id);
	}

	@Override
	public T read(String database, String prefix, String id) {
		if(this.collection == null) {
			throw new UnsupportedOperationException("You cannot use this method when no Collection annotation is present on the type");
		}
		return read(database, prefix, this.collection, id);
	}
	
	private T read(String database, String prefix, String strCollection, String id) {
		DBCollection collection = mongo.getCollection(database, prefix	+ strCollection);
		JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
		return coll.findOneById(id);
	}

	@Override
	public T update(VirtualHost vhost, T entity) {
		return update((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), entity);
	}

	@Override
	public T update(VirtualHost vhost, String id, T entity) {
		return update((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), id, entity);
	}

	@Override
	public T update(String database, String prefix, T entity) {
		DBCollection collection = mongo.getCollection(database, prefix	+ getCollectionName(entity));
		JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
		int n = coll.updateById(entity.getId(), entity).getN();
		if (n == 1) {
			return entity;
		}
		return null;
	}

	@Override
	public T update(String database, String prefix, String id, T entity) {
		DBCollection collection = mongo.getCollection(database, prefix + getCollectionName(entity));
		JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
		int n = coll.updateById(id, entity).getN();
		if (n == 1) {
			return entity;
		}
		return null;
	}

	@Override
	public T updateFields(VirtualHost vhost, T entity) {
		return updateFields((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), entity);
	}

	@Override
	public T updateFields(String database, String prefix, T entity) {
		DBCollection collection = mongo.getCollection(database, prefix + getCollectionName(entity));
		JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
		int n = coll.update(new BasicDBObject("_id", entity.getId()),
							new BasicDBObject("$set", DataUtils.toMap(entity)),
							true, 
							false).getN();
		if (n == 1) {
			return entity;
		}
		return null;
	}

	@Override
	public T upsert(VirtualHost vhost, T entity) {
		return upsert((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), entity);
	}

	@Override
	public T upsert(String database, String prefix, T entity) {
		DBCollection collection = mongo.getCollection(database, prefix + getCollectionName(entity));
		JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
		int n = coll.update(DBQuery.is("_id", entity.getId()), entity, true, false).getN();
		if (n == 1) {
			return entity;
		}
		return null;
	}

	@Override
	public void delete(VirtualHost vhost, T entity) {
		delete((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), entity);
	}

	@Override
	public void delete(String database, String prefix, T entity) {
		DBCollection collection = mongo.getCollection(database, prefix + getCollectionName(entity));
		JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
		coll.removeById(entity.getId());
	}

	@Override
	public void delete(VirtualHost vhost, String id) {
		delete((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), id);
	}

	@Override
	public void delete(String database, String prefix, String id) {
		if(this.collection == null) {
			throw new UnsupportedOperationException("You cannot use this method when no Collection annotation is present on the type");
		}
		DBCollection collection = mongo.getCollection(database, prefix + this.collection);
		JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
		coll.removeById(id);
	}

	@Override
	public List<T> getAll(VirtualHost vhost) {
		return getAll((String) vhost.getContext("database"), (String) vhost.getContext("prefix"));
	}

	@Override
	public List<T> getAll(String database, String prefix) {
		return query(database, prefix, null);
	}

	@Override
	public List<T> query(VirtualHost vhost, Map<String, Object> query) {
		return query((String) vhost.getContext("database"),	(String) vhost.getContext("prefix"), query);
	}

	@Override
	public List<T> query(String database, String prefix, Map<String, Object> query) {
		if(this.collection == null) {
			throw new UnsupportedOperationException("You cannot use this method when no Collection annotation is present on the type");
		}
		try {
			Map<String, Object> defQuery = type.newInstance().retrieveDefaultQuery();
			if (query == null) {
				query = new HashMap<String, Object>();
			}
			if (defQuery != null) {
				query.putAll(defQuery);
			}
		} catch (InstantiationException | IllegalAccessException e) {
			System.err.println("Couldn't merge queries: " + e.getMessage());
		}
		DBCollection collection = mongo.getCollection(database, prefix + this.collection);
		JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
		if (query == null) {
			return coll.find().toArray();
		}
		return coll.find(new BasicDBObject(query)).toArray();
	}
}
