package io.core9.plugin.database.mongodb.repository;

import io.core9.plugin.database.mongodb.MongoDatabase;
import io.core9.plugin.database.repository.Collection;
import io.core9.plugin.database.repository.CrudEntity;
import io.core9.plugin.database.repository.CrudRepository;
import io.core9.plugin.database.repository.DataUtils;
import io.core9.plugin.database.repository.NoCollectionNamePresentException;
import io.core9.plugin.database.repository.ObservableCrudRepository;
import io.core9.plugin.database.repository.RepositoryFactory;
import io.core9.plugin.server.VirtualHost;

import java.util.Map;

import net.xeoh.plugins.base.annotations.PluginImplementation;
import net.xeoh.plugins.base.annotations.events.PluginLoaded;

import org.mongojack.JacksonDBCollection;
import org.mongojack.internal.MongoJackModule;

import rx.Observable;
import rx.Observable.OnSubscribe;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;

@PluginImplementation
public class RepositoryFactoryImpl implements RepositoryFactory {
	
	private MongoDatabase mongo;
	
	@PluginLoaded
	public void onDatabase(MongoDatabase database) {
		this.mongo = database;
	}

	@Override
	public <T extends CrudEntity> CrudRepository<T> getRepository(final Class<T> type) throws NoCollectionNamePresentException {
		return getRepository(type, false);
	}

	@Override
	public <T extends CrudEntity> CrudRepository<T> getRepository(final Class<T> type, boolean dynamicCollectionName) throws NoCollectionNamePresentException {
		final Collection classColl = type.getAnnotation(Collection.class);
		String collection = null;
		if(!dynamicCollectionName) {
			if (classColl == null) {
				throw new NoCollectionNamePresentException();
			} else {
				collection = classColl.value();
			}
		}
		final ObjectMapper mapper = new ObjectMapper();
		MongoJackModule.configure(mapper);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		MongoCrudRepository<T> repo = new MongoCrudRepository<T>(type, collection, mapper);
		repo.setMongoDatabase(mongo);
		return repo;
	}
	
	@Override
	public <T extends CrudEntity> CrudRepository<T> getCachedRepository(Class<T> type)
			throws NoCollectionNamePresentException {
		return getRepository(type);
	}

	@Override
	public <T extends CrudEntity> ObservableCrudRepository<T> getObservableRepository(Class<T> type)
			throws NoCollectionNamePresentException {
		final Collection classColl = type.getAnnotation(Collection.class);
		if (classColl == null) {
			throw new NoCollectionNamePresentException();
		}

		final ObjectMapper mapper = new ObjectMapper();
		MongoJackModule.configure(mapper);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		return new ObservableCrudRepository<T>() {
			final String COLLECTION = classColl.value();

			@Override
			public Observable<T> create(VirtualHost vhost, T entity) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public Observable<T> create(String database, String prefix, T entity) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public Observable<T> read(VirtualHost vhost, String id) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public Observable<T> read(String database, String prefix, String id) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public T update(VirtualHost vhost, T entity) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public T update(String database, String prefix, T entity) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public T updateFields(VirtualHost vhost, T entity) {
				return updateFields((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), entity);
			}

			@Override
			public T updateFields(String database, String prefix, T entity) {
				DBCollection collection = mongo.getCollection(database, prefix + COLLECTION);
				JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class, mapper);
				Map<String, Object> fields = DataUtils.toMap(entity);
				fields.remove("_id");
				int n = coll.update(new BasicDBObject("_id", entity.getId()), new BasicDBObject("$set", fields), false,
						false).getN();
				if (n == 1) {
					return entity;
				}
				return null;
			}

			@Override
			public Observable<T> upsert(VirtualHost vhost, T entity) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public Observable<T> upsert(String database, String prefix, T entity) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public Observable<T> getAll(VirtualHost vhost) {
				return getAll((String) vhost.getContext("database"), (String) vhost.getContext("prefix"));
			}

			@Override
			public Observable<T> getAll(String database, String prefix) {
				return query(database, prefix, null);
			}

			@Override
			public Observable<T> query(VirtualHost vhost, Map<String, Object> query) {
				return query((String) vhost.getContext("database"), (String) vhost.getContext("prefix"), query);
			}

			@Override
			public Observable<T> query(String database, String prefix, Map<String, Object> query) {
				return Observable.create((OnSubscribe<T>) subscriber -> {
					try {
						Map<String, Object> defQuery = type.newInstance().retrieveDefaultQuery();
						BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
						if (defQuery != null) {
							defQuery.forEach((key, value) -> {
								builder.append(key, value);
							});
						}
						if (query != null) {
							query.forEach((key, value) -> {
								builder.append(key, value);
							});
						}
						DBCollection collection = mongo.getCollection(database, prefix + COLLECTION);
						JacksonDBCollection<T, String> coll = JacksonDBCollection.wrap(collection, type, String.class,
								mapper);
						coll.find(builder.get()).forEach(found -> {
							subscriber.onNext(found);
						});
					} catch (Exception e) {
						subscriber.onError(e);
					}
					subscriber.onCompleted();
				});
			}

			@Override
			public void delete(VirtualHost vhost, T entity) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public void delete(String database, String prefix, T entity) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public void delete(VirtualHost vhost, String id) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}

			@Override
			public void delete(String database, String prefix, String id) {
				throw new UnsupportedOperationException("Not yet implemented, sorry");
			}
		};
	}
}
