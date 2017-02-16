


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.Iterable;

import scala.Tuple2;
import scala.tools.nsc.util.ClassPath.JavaContext;
import org.apache.spark.api.java.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
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


public class TeraSort_Spark {

	
	public TeraSort_Spark()
	{
		
	}
  @SuppressWarnings({ "unchecked", "rawtypes" })
public static void main(String[] args) throws Exception {
	  
    String inputFile = args[0]; //path for input
    String outputFile = args[1]; //path for output
    // Create a Java Spark Context.
    SparkConf conf = new SparkConf().setAppName("TeraSort_Spark").setMaster("spark://ec2-52-91-144-247.compute-1.amazonaws.com:7077");
       JavaSparkContext sc = new JavaSparkContext(conf);
    // Load our input data.
    JavaRDD<String> input = sc.textFile(inputFile);
   //Map the input data to key pair value using maptoPair and PairRdd
    JavaPairRDD<String, String> SortPair = input.mapToPair(
      new PairFunction<String, String,String>(){
        public Tuple2<String,String> call(String keyvalue){
          return new Tuple2<String,String>(keyvalue.substring(0,10),keyvalue.substring(10,98));

        }}).sortByKey();
   
//Combine the key pair RDD to a string Rdd
JavaRDD<String> Output=   SortPair.map(tuple->tuple._1+tuple._2).coalesce(1, true);
//Save the string Rdd to file
Output.saveAsTextFile(outputFile);
sc.stop();
}

}   	
  	       

    		 
  

	


