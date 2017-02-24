package org.log4j.csm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.LoggingEvent;
import org.log4mongo.MongoDbPatternLayoutAppender;

import com.ctfo.util.MongoDatasource;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;
/**
 *  async save l class
 * @author malongqing
 * @version 1.0
 */
public class AsyncMongoDbPatternLayoutAppender extends MongoDbPatternLayoutAppender{
		
	private ExecutorService es = null;
	
	@Override
	protected void append(final LoggingEvent loggingEvent) {

		//此处加多线程时行处理
		if(es==null){
			es = Executors.newFixedThreadPool(5);
			LogLog.warn("----->> init AsyncMongoDbPatternLayoutAppender thread Pool [5]");
		}
		MongoDatasource datasource = new MongoDatasource("LOGS");
		String dbName = datasource.getProps().get("LOGS").getDbName();
		es.execute(new SaveLogsToMongoDB(StringUtils.isBlank(dbName) ? getDatabaseName() : dbName, getCollectionName(), layout, loggingEvent, datasource.getMongo(), errorHandler));
	}
}

class SaveLogsToMongoDB implements Runnable {
	
	private String dbName;
	private String collectionName;
	private Layout layout;
	private LoggingEvent loggingEvent;
	private Mongo mongo;
	private ErrorHandler errorHandler;
	
	public SaveLogsToMongoDB(String dbName, String collectionName,
			Layout layout, LoggingEvent loggingEvent, Mongo mongo, ErrorHandler errorHandler) {
		this.dbName = dbName;
		this.collectionName = collectionName;
		this.layout = layout;
		this.loggingEvent = loggingEvent;
		this.mongo = mongo;
		this.errorHandler = errorHandler;
	}

	@Override
	public void run() {				
		try{
			DBObject bson = null;     				
			String json = layout.format(loggingEvent);     
			if (json.length() > 0) { 
				Object obj = JSON.parse(json);
				if (obj instanceof DBObject) {
					bson = (DBObject) obj;							
				}
			}
			if (bson != null) {						
				String systemName = (String)bson.get("systemName");
				String level = (String)bson.get("level");						
				String setLevel = this.getLogSet(systemName);						
				if(getLevelVal(setLevel) <= getLevelVal(level)){
					DB db = mongo.getDB(dbName);
					DBCollection dbCollection = db.getCollection(collectionName);
					dbCollection.insert(bson);
				}					
			}
		}catch(Exception e){
			errorHandler.error("Failed to insert document to MongoDB", e, ErrorCode.WRITE_FAILURE);
		}
	}
	
	// get log set level
	private String getLogSet(String systemName){
		try{
			DB db = mongo.getDB(dbName);
			DBCollection dbc = db.getCollection(collectionName + "_set");
			DBObject logset = new BasicDBObject();
			logset.put("systemName", systemName);
			DBObject data = dbc.findOne(logset);
			return data==null?"WARN":(String)data.get("level");
		}catch(Exception e){
			errorHandler.error("Failed log set level from MongoDB", e, ErrorCode.WRITE_FAILURE);
		}			 
		return "WARN";

	}
	//get level value
	private int getLevelVal(String level){				
		if ("DEBUG".equals(level)) return 20000;
		else if ("INFO".equals(level)) return 30000;
		else if ("WARN".equals(level)) return 40000;
		else if ("ERROR".equals(level)) return 50000;
		return 0;
	}
}