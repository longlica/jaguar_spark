import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection._


object TestDataFrame {

	def main(args: Array[String]) {
		sparkfunc()
	}

	def sparkfunc()
	{
		Class.forName("com.jaguar.jdbc.JaguarDriver");
		val sparkConf = new SparkConf().setAppName("TestScalaJDBC")
		val sc = new SparkContext(sparkConf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._

		val url = "jdbc:jaguar://127.0.0.1:8888/test";
		val jbench = sqlContext.read.format("jdbc")
		   .options(
		       Map( "url" -> url,
						 "dbtable" -> "int10k",
						 "user" -> "admin",
						 "password" -> "jaguar",
						 "partitionColumn" -> "k1",
						 "lowerBound" -> "2",
						 "upperBound" -> "2000000",
						 "numPartitions" -> "4",
						 "driver" -> "com.jaguar.jdbc.JaguarDriver"
		   )).load()
		    
		// works fine
		jbench.registerTempTable("jbench")
		jbench.printSchema()

		jbench.sort("k2").show()
		jbench.sort($"k2".desc).show()
		jbench.sort($"k2".desc, $"k1".asc).show()
		jbench.orderBy($"k2".desc, $"k1".asc).show()
		jbench.toJSON.coalesce(1).saveAsTextFile( "/tmp/jbench.txt" );

		// works fine
		// people.selectExpr("k2", "k1"  ).show()
		// people.selectExpr("k2", "k1 as keyone"  ).show()
		//jbench.selectExpr("k2", "k1 as keyone", "abs(k2)" ).show()

		// not work
		// people.insertInto( "int10k_2", false );
		// people.write().jdbc(url, "int10k_2", true );
		//jbench.insertIntoJDBC(url, "jbench_2", true );


		// works fine
//		val k12 = jbench.select("k1", "k2")
//		k12.show();
//
//		// works fine
//		val sk1 = jbench.select("k1")
//		sk1.show();
//
//		// works fine
//		val sk2 = jbench.select("k2")
//		sk2.show();
//
//		// works fine
//		val below60 = jbench.filter(jbench("k1") <= 3927570 ).show()

		// works fine
		jbench.groupBy("addr").count().show()

		// works fine
		jbench.groupBy("addr").avg().show()


//		// works great
//		jbench.describe( "k1", "k2").show()

		// works great 
		jbench.stat.freqItems( Seq("k1") ).show()

		jbench.sample( true, 0.1, 100 ).show()
		jbench.distinct.show()
		jbench.dropDuplicates().show()
		jbench.dropDuplicates.show()

		// works fine
		jbench.dropDuplicates.cache.show()
		jbench.dropDuplicates.persist.show()

		// works
		val df = sqlContext.sql("SELECT * FROM jbench where addr between 'NODE6' and 'NODE7' ")
		df.distinct.show()

	}

}

