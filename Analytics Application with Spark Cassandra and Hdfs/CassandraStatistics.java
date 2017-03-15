import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.StatCounter;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;
import scala.Tuple3;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class CassandraStatistics {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("spark grep").setMaster(args[0]);
        
        conf.set("spark.cassandra.connection.host", args[1]);

        JavaSparkContext sc = new JavaSparkContext(conf);
CassandraConnector connector = CassandraConnector.apply(sc.getConf());
System.out.println("connector");
     // Prepare the schema
Session session=connector.openSession();

        String inputFile = args[2]; //path for input


        try { 
         session.execute("DROP KEYSPACE IF EXISTS cs595proj");
         session.execute("CREATE KEYSPACE cs595proj WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
         session.execute("CREATE TABLE cs595proj.analytics (id Text,key Text,PRIMARY KEY(id,key));");
        }
        catch(Exception e)
        {
         	
        }
        long startTime = System.nanoTime();
        String uuid=UUID.randomUUID().toString();

        JavaRDD<input> inputdata = sc.textFile(inputFile).map(new Function<String,input>(){

			@Override
			public input call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				String[] parts=arg0.split(",");
				return new input(uuid,parts[9]);	
			}
        });
        
       
	    CassandraJavaUtil.javaFunctions(inputdata).writerBuilder("cs595proj","analytics",CassandraJavaUtil.mapToRow(input.class)).saveToCassandra();
	    long endTime = System.nanoTime();

	    try {
    	    FileWriter resultsFile = new FileWriter(args[3], true);
    	    resultsFile.append("Total time: " + (endTime - startTime)  / 1000000000.0  + " seconds\n");
    	    resultsFile.close();
    	} catch (IOException e){
    	    e.printStackTrace();
    	}    	     	    

	    
	    startTime = System.nanoTime();
        JavaRDD<Double> data = CassandraJavaUtil.javaFunctions(sc).cassandraTable("cs595proj", "analytics", CassandraJavaUtil.mapColumnTo(Double.class)).select("key");
        
          
      JavaDoubleRDD d=   data.mapToDouble(new DoubleFunction<Double>()
        		{


					@Override
					public double call(Double arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0;
					}
       	
        		})
    	 ;       
       double sum=d.sum();
       double count=d.count();
        double stddev=d.stdev();
       double mean=d.mean();
        double min=d.min();
       double max=d.max();
	     endTime = System.nanoTime();
        FileWriter resultsFile;
		try {
			resultsFile = new FileWriter(args[3], true);
			resultsFile.write(
"Standard Deviation is "+stddev + "\n min is "+min+"\n max is" +max        
+" \n mean is "+mean+"\n sum is "+sum+" \n count is"+count
+"\n avg"+sum/count);
			resultsFile.write("\n sum Total time: " + (endTime - startTime)  / 1000000000.0  + " seconds\n");
			resultsFile.flush();
			resultsFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}        
        
        
            sc.stop();

    }
    

	public static class input implements Serializable {
		
		private String id;

		private String key;
		
	    public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getid() {
			return id;
		}

		public void setvalue(String id) {
			this.id = id;
		}

		public input() { }

	    public input(String id,String key) {
	    	
	        this.id = id;
	        this.key=key;
	    }
	    
	    
	}
	  	       

    
}