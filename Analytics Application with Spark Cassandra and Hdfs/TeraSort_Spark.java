


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;
import java.lang.Iterable;
import java.math.BigDecimal;
import java.text.MessageFormat;

import scala.Tuple1;
import scala.Tuple2;
import scala.tools.nsc.util.ClassPath.JavaContext;
import org.apache.spark.api.java.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mesos.protobuf.ByteString.Output;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.executor.Executor;
import org.apache.spark.rdd.RDD;
import org.glassfish.grizzly.nio.transport.DefaultStreamReader.Input;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.rdd.CassandraRDD;







public class TeraSort_Spark {

  @SuppressWarnings({ "unchecked", "rawtypes" })
public static void main(String[] args) throws Exception {
	  
    String inputFile = args[0]; //path for input
   // String outputFile = args[1]; //path for output
  
    // Create a Java Spark Context.
    SparkConf conf = new SparkConf().setAppName("TeraSort_Spark").setMaster(args[1]); // configure the spark framework on node 
    conf.set("spark.cassandra.connection.host", args[2]); // set the cassandra host in spark configuration for connecting with cassandra 
    JavaSparkContext sc = new JavaSparkContext(conf); //Initialize the spark context 
    CassandraConnector connector = CassandraConnector.apply(sc.getConf()); // connect to cassandra database with the datastax cassandra connector Api
    Session session=connector.openSession(); // connect to cassandra cluster and open the session for performing operations with cassandra schema
    
    try { 
          session.execute("DROP KEYSPACE IF EXISTS CS595Proj");  
          session.execute("CREATE KEYSPACE CS595Proj WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");//create a keyspace schema in cassandra
          session.execute("CREATE TABLE CS595Proj.OutputData (id UUID,key Text, data TEXT,PRIMARY KEY(id,key));"); //create table in cassandra using execute 
    }
      catch(Exception e)
      {
      	System.out.print(e.getMessage());
      }
    
    JavaRDD<String> input = sc.textFile(inputFile);
    JavaPairRDD<String, String> SortdataPair = input.mapToPair(
    	      new PairFunction<String, String,String>(){
    	        public Tuple2<String,String> call(String keyvalue){
    	          return new Tuple2<String,String>(keyvalue.substring(0,10),keyvalue.substring(10,98));

    	        }});
    JavaRDD<String> Output2 =SortdataPair.map(tuple->tuple._1+tuple._2);

    UUID id =UUID.randomUUID();
    JavaRDD<Output> Sortdata1 = Output2.map(new Function<String,Output>() {

    	@Override

    	 public Output call(String keyvalue) throws Exception {
    		return    new Output(id,keyvalue.substring(0,10),keyvalue.substring(10,98));
        }
    	
    });
  CassandraJavaUtil.javaFunctions(Sortdata1).saveToCassandra("cs595proj","outputdata",CassandraJavaUtil.mapToRow(Output.class),CassandraJavaUtil.someColumns("id","key","data"));
     sc.stop();

}



public static class input implements Serializable {
	
	private String key;
    private String value;

    public String getvalue() {
		return value;
	}

	public void setvalue(String value) {
		this.value = value;
	}

	public input() { }

    public input(String key, String value) {
    	
        this.key = key;
        this.value = value;
    }

    public String getkey() { return key; }
    public void setkey(String key) { this.key = key; }
    
    
}
  	       

public static class Output implements Serializable {
	private UUID id;
	private String key;
    private String data;

    public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

    public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public Output() { }

    public Output(UUID id,String key, String data) {
    	this.id=id;
        this.key = key;
        this.data = data;
    }

    public String getkey() { return key; }
    public void setkey(String key) { this.key = key; }
    
    
}
 		 
  
}   	
	


