package com.ctfo.test;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * @author Bill Tu(tujiyue/iwtxokhtd) May 21, 2011[11:33:34 PM]
 * 
 */
public class MongoDBAdvancedQuery {
	private static Log logger = LogFactory.getLog(MongoDBAdvancedQuery.class);
	private static final String HOST = "192.168.5.87";
	private static final int PORT = 27001;
	// private static final String USER = "iwtxokhtd";
	// private static final String PASSWORD = "123456";
	private static final String DB_NAME = "test";
	private static final String COLLECTION = "Collections";
	private static Mongo conn = null;
	private static DB myDB = null;
	private static DBCollection myCollection = null;

	static {
		try {
			conn = new Mongo(HOST, PORT);// 建立数据库连接
			myDB = conn.getDB(DB_NAME);// 使用test数据库
//			boolean loginSuccess = myDB.authenticate(USER,
//					PASSWORD.toCharArray());// 用户验证
//			if (loginSuccess) {
			myCollection = myDB.getCollection(COLLECTION);
//			}
		} catch (MongoException e) {
			logger.error("EXCEPTION!", e);
		}
	}

	/**
	 * 查询数据
	 * 
	 * @param collection
	 *            　“表”名
	 */
	private static void queryData(DBCollection collection) {
		// count查询
//		System.out.println("表的总记录数为：");
//		System.out.println(collection.find().count());
//		// 分页查询
//		DBCursor findAll = collection.find();
//		DBCursor queryByPage = findAll.skip(3).limit(4);
//		printData("从第3条记录起取４条记录为：", queryByPage);
//		// order by操作
//		DBObject orderBy = new BasicDBObject();
//		orderBy.put("userName", -1);// 按userName倒序排
//		DBCursor orderByResult = collection.find().sort(orderBy);
//		printData("所有记录按userName倒序排为：", orderByResult);
//
//		// "!=" 和 ">" 操作
//		DBObject notEqual = new BasicDBObject();
//		notEqual.put("$ne", "Bill Tu10");
//
//		DBObject greatThan = new BasicDBObject();
//		greatThan.put("$gt", 7);
//
//		DBObject notEqualAndGreatThan = new BasicDBObject();
//		notEqualAndGreatThan.put("userName", notEqual);
//		notEqualAndGreatThan.put("age", greatThan);
//
//		DBCursor notEqualAndGreatThanResult = collection
//				.find(notEqualAndGreatThan);
//		printData("userName!='Bill Tu10' and age>7的记录为：",
//				notEqualAndGreatThanResult);
//
//		// ">=" 和"<="操作
//		DBObject greatThanEqualAndLessThanEqual = new BasicDBObject();
//		greatThanEqualAndLessThanEqual.put("$gte", 2);
//		greatThanEqualAndLessThanEqual.put("$lte", 7);
//
//		DBObject ageCompare = new BasicDBObject();
//		ageCompare.put("age", greatThanEqualAndLessThanEqual);
//
//		DBCursor compareResult = collection.find(ageCompare);
//		printData("age>=2 and age<=7的记录为：", compareResult);

		
//
//		// not in操作
//		DBObject notIn = new BasicDBObject();
//		notIn.put("$nin", new Object[] { 2, 3 });
//
//		DBObject ageNotIn = new BasicDBObject();
//		ageNotIn.put("age", notIn);
//
//		DBCursor ageNotInResult = collection.find(ageNotIn);
//		printData("age not in (2,3)的记录为：", ageNotInResult);
//
//		// or操作
//		DBObject orGreatThan = new BasicDBObject("$gt", 3);
//		DBObject orRankAll = new BasicDBObject("$all", new Object[] { 1, 1 });
//		DBObject ageOrGreatThan = new BasicDBObject();
//		ageOrGreatThan.put("age", orGreatThan);
//		DBObject rankOrAll = new BasicDBObject();
//		rankOrAll.put("rank", orRankAll);
//
//		DBObject orOperation = new BasicDBObject();
//		orOperation.put("$or", new Object[] { ageOrGreatThan, rankOrAll });
//
//		DBCursor orResult = collection.find(orOperation);
//		printData("age>3 or rank in all(1,1)的记录为：", orResult);
//
//		// not or操作
//		DBObject notOrOperation = new BasicDBObject();
//		notOrOperation.put("$nor", new Object[] { ageOrGreatThan, rankOrAll });
//
//		DBCursor notOrResult = collection.find(notOrOperation);
//		printData("not(age>3 or rank in all(1,1))的记录为：", notOrResult);
//
//		// size 操作
//		DBObject size = new BasicDBObject("$size", 3);
//		DBObject rankSize = new BasicDBObject();
//		rankSize.put("rank", size);
//
//		DBCursor sizeResult = collection.find(rankSize);
//		printData("rank数组大小为３的记录为：", sizeResult);

		// exists操作
		DBObject exists = new BasicDBObject("$exists", true);
		DBObject userNameExists = new BasicDBObject();
		userNameExists.put("clob", exists);

		DBCursor userNameExistsResult = collection.find(userNameExists);
		printData("userName exists false的记录为：", userNameExistsResult);
		System.out.println("-----------------------------------------------------------------------------------------");
		
		Pattern pattern = Pattern.compile("^.*"+ "72a359f5-b1d3-48c0-96ee-257cce953294" +".*$");
		
		// all操作
//		DBObject all = new BasicDBObject();
//		all.put("$all", new Object[] { "13911484765" });

		DBObject rankAll = new BasicDBObject();
		rankAll.put("clob", pattern);

		DBCursor rankAllResult = collection.find(rankAll);
		printData("clob in all(18611153002)的记录为：", rankAllResult);

		// mod 取模操作
//		DBObject modArray = new BasicDBObject("$mod", new Object[] { 2, 0 });
//		DBObject ageMod = new BasicDBObject();
//		ageMod.put("age", modArray);
//
//		DBCursor ageModResult = collection.find(ageMod);
//		printData("age%2==0的记录为：", ageModResult);

	}

	/**
	 * 打印结果数据
	 * 
	 * @param description
	 *            　结果数据相关描述
	 * @param recordResult
	 *            　结果集
	 */
	private static void printData(String description, DBCursor recordResult) {
		System.out.println(description);
		for (Iterator<DBObject> iter = recordResult.iterator(); iter.hasNext();) {
			System.out.println(iter.next());
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		queryData(myCollection);
	}
}