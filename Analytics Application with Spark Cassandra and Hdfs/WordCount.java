
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;




public class WordCount {
  public static void main(String[] args) throws Exception {
       // Create a Java Spark Context.
	    String inputFile = args[0]; //path for input

    SparkConf conf = new SparkConf().setAppName("wordCount");
    conf.setMaster(args[1]);

    conf.set("spark.cassandra.connection.host", args[2]);

    JavaSparkContext sc = new JavaSparkContext(conf);
    // Load our input data.
    // Split up into words.
    
CassandraConnector connector = CassandraConnector.apply(sc.getConf());
System.out.println("connector");
     // Prepare the schema
Session session=connector.openSession();

long startTime = System.nanoTime();


try { 
         session.execute("DROP KEYSPACE IF EXISTS cs595proj");
         session.execute("CREATE KEYSPACE cs595proj WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
         session.execute("CREATE TABLE cs595proj.wordcount (id UUID,key Text, data TEXT,PRIMARY KEY(id,key));");
         session.execute("CREATE TABLE cs595proj.finaldata (id UUID,key Text, data TEXT,PRIMARY KEY(id));");
         
     }
     catch(Exception e)
     {
     	
     }

    JavaRDD<String> input = sc.textFile(inputFile);
     
/*    JavaRDD<String> wordshdfs = input.flatMap(
      new FlatMapFunction<String, String>() {
        public Iterable<String> call(String x) {
          return Arrays.asList(x.split(" "));
        }});
    // Transform into word and count.
    JavaPairRDD<String, Integer> countshdfs = wordshdfs.mapToPair(
      new PairFunction<String, String, Integer>(){
        public Tuple2<String, Integer> call(String x){
          return new Tuple2(x, 1);
        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
            public Integer call(Integer x, Integer y){ return x + y;}});
    // Save the word count back out to a text file, causing evaluation.
   
    countshdfs.saveAsTextFile(outputFile);
  */  
	  
    UUID uid=UUID.randomUUID();
    JavaRDD<String> words = input.flatMap(
  	      new FlatMapFunction<String, String>() {
  	        public Iterable<String> call(String x) {
  	          return Arrays.asList(x.split("\n"));
  	        }});
    
    JavaRDD<Output> Sortfinal = words.map(new Function<String,Output>() {
        @Override
        public Output call(String out) throws Exception {
            return new Output(uid,out,1);
        }
    });
    

     
     CassandraJavaUtil.javaFunctions(Sortfinal).writerBuilder("cs595proj","wordcount",CassandraJavaUtil.mapToRow(Output.class)).saveToCassandra();
   

long endTime = System.nanoTime();

try {
    FileWriter resultsFile = new FileWriter(args[3], true);
    resultsFile.write("Total time: " + (endTime - startTime)  / 1000000000.0  + " seconds\n");
    resultsFile.flush();
    resultsFile.close();
} catch (IOException e){
    e.printStackTrace();
}


 startTime = System.nanoTime();

 		//read the data from cassandra table select specific column and store in rdd 
     JavaRDD<String> wcdata = CassandraJavaUtil.javaFunctions(sc).cassandraTable("cs595proj", "wordcount", CassandraJavaUtil.mapColumnTo(String.class)).select("key");
     
     //perform spark job for word count split the data by space
     JavaRDD<String> data = wcdata.flatMap(
    	      new FlatMapFunction<String, String>() {
    	        public Iterable<String> call(String x) {
    	          return Arrays.asList(x.split(" "));
    	        }});
    	   
    
    	    // Transform into word and count.
    	    JavaPairRDD<String, Integer> counts = data.mapToPair(
    	      new PairFunction<String, String, Integer>(){
    	        public Tuple2<String, Integer> call(String x){
    	          return new Tuple2(x, 1);
    	        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
    	            public Integer call(Integer x, Integer y){ return x + y;}});
    
    	    //create a Output class rdd and store each variable values using getter and setter that maps to cassandra table columns
    	    JavaRDD<Output> finaldata = counts.map(new Function<Tuple2<String,Integer>,Output>()
    	    		{
					@Override
					public Output call(Tuple2<String, Integer> arg0) throws Exception {
						// TODO Auto-generated method stub
						return new Output(UUID.randomUUID(),arg0._1,arg0._2.toString());
					}});
    	  
    	    //write the data to cassandra table and store the class rdd   	        
    	    CassandraJavaUtil.javaFunctions(finaldata).writerBuilder("cs595proj","finaldata",CassandraJavaUtil.mapToRow(Output.class)).saveToCassandra();
    	      
    	    endTime = System.nanoTime();
    	     try {
    	    	    FileWriter resultsFile = new FileWriter(args[3], true);
    	    	    resultsFile.append("Total time: " + (endTime - startTime)  / 1000000000.0  + " seconds\n");
    	    	    resultsFile.close();
    	    	} catch (IOException e){
    	    	    e.printStackTrace();
    	    	}    	     	    
    	sc.stop();
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
    public Output(UUID id,String key, Integer data) {
    	this.id=id;
        this.key = key;
        this.data = data.toString();
    }

    public String getkey() { return key; }
    public void setkey(String id) { this.key = id; }
    
    
}



}