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







public class TeraSort_Hdfs {

	
  @SuppressWarnings({ "unchecked", "rawtypes" })
public static void main(String[] args) throws Exception {
	  
    String inputFile = args[0]; //path for input
    String outputFile = args[1]; //path for output
    
    
    // Create a Java Spark Context.
    SparkConf conf = new SparkConf().setAppName("TeraSort_Hdfs").setMaster(args[2]);
 
    
    JavaSparkContext sc = new JavaSparkContext(conf);
    

    JavaRDD<String> input = sc.textFile(inputFile);
    JavaPairRDD<String, String> SortdataPair = input.mapToPair(
    	      new PairFunction<String, String,String>(){
    	        public Tuple2<String,String> call(String keyvalue){
    	          return new Tuple2<String,String>(keyvalue.substring(0,10),keyvalue.substring(10,98));

    	        }}).sortByKey();
    JavaRDD<String> Output2 =SortdataPair.map(tuple->tuple._1+tuple._2);

    Output2.saveAsTextFile(outputFile);
    sc.stop();

}
}
