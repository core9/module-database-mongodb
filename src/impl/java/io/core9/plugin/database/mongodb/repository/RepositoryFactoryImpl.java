package io.core9.plugin.database.mongodb.repository;

import java.util.List;
import java.util.Map;

import org.mongojack.JacksonDBCollection;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import io.core9.plugin.database.mongodb.MongoDatabase;
import io.core9.plugin.database.repository.Collection;
import io.core9.plugin.database.repository.CrudEntity;
import io.core9.plugin.database.repository.CrudRepository;
import io.core9.plugin.database.repository.NoCollectionNamePresentException;
import io.core9.plugin.database.repository.RepositoryFactory;
import io.core9.plugin.server.VirtualHost;
import net.xeoh.plugins.base.annotations.PluginImplementation;
import net.xeoh.plugins.base.annotations.injections.InjectPlugin;

@PluginImplementation
public class RepositoryFactoryImpl implements RepositoryFactory {
	
	@InjectPlugin
	private MongoDatabase mongo;

	@Override
	public <T extends CrudEntity> CrudRepository<T> getRepository(final Class<T> type) throws NoCollectionNamePresentException {
		
		final Collection classColl = type.getAnnotation(Collection.class);
		if(classColl == null) {
			throw new NoCollectionNamePresentException();
		}
		
		final ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		
		return new CrudRepository<T>(){
			String collectionName = classColl.value();

			@Override
			public T create(VirtualHost vhost, T entity) {
				DBCollection collection = mongo.getCollection((String) vhost.getContext("database"), vhost.getContext("prefix") + collectionName);
				JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
				return coll.insert(entity).getSavedObject();
			}

			@Override
			public T read(VirtualHost vhost, String id) {
				DBCollection collection = mongo.getCollection((String) vhost.getContext("database"), vhost.getContext("prefix") + collectionName);
				JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
				return coll.findOneById(id);
			}

			@Override
			public T update(VirtualHost vhost, String id, T entity) {
				DBCollection collection = mongo.getCollection((String) vhost.getContext("database"), vhost.getContext("prefix") + collectionName);
				JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
				return coll.updateById(id, entity).getSavedObject();
			}

			@Override
			public void delete(VirtualHost vhost, T entity) {
				DBCollection collection = mongo.getCollection((String) vhost.getContext("database"), vhost.getContext("prefix") + collectionName);
				JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
				coll.removeById(entity.getId());
			}

			@Override
			public List<T> query(VirtualHost vhost, Map<String, Object> query) {
				DBCollection collection = mongo.getCollection((String) vhost.getContext("database"), vhost.getContext("prefix") + collectionName);
				JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
				return coll.find(new BasicDBObject(query)).toArray();
			}
		};
	}

}
