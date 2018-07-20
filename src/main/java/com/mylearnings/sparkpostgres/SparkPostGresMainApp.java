package com.mylearnings.sparkpostgres;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import com.mylearnings.sparkpostgres.model.Customer;

/**
 * @author PGanta
 *
 */
public class SparkPostGresMainApp {
	public static void main( String[] args ) {

		
		SparkConf config = new SparkConf().setMaster("local[4]").setAppName("PostGresSparkLoader");
		SparkContext sc = new SparkContext(config );
		JavaSparkContext jsc = new JavaSparkContext(sc );
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		JavaRDD<String> filerdd = jsc.textFile("./src/main/resources/testdata.txt");
		
		filerdd.persist(StorageLevel.MEMORY_ONLY());
		System.out.println("count is"+filerdd.count());
		JavaRDD<Customer> customerRdd = filerdd.map(f-> {
			return convertRddToCustInfo(f);
		});
		
		Dataset<Row> custDs = sqlContext.createDataFrame(customerRdd, Customer.class);
		
		Properties connectionProperties = new Properties();
		connectionProperties.setProperty("username", "postgres");
		connectionProperties.setProperty("password", "tiger");
		connectionProperties.setProperty("port", "5432");
		custDs.write().mode(SaveMode.Append).jdbc("jdbc:postgresql://localhost:5432/test?user=postgres&password=tiger", "customer", connectionProperties );
		
		
		jsc.stop();

	}

	

	private static Customer convertRddToCustInfo(String f) {
		Customer c = new Customer();
		String[] vals = f.split(",");
		c.setId((int)Math.random());
		c.setName(vals[0]);
		c.setCity(vals[1]);
		c.setAge(Integer.parseInt(vals[2]));
		return c;
	}
}
