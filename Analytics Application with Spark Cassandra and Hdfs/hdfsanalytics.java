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

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;
import scala.Tuple3;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class hdfsanalytics {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("spark grep").setMaster(args[1]);
        

        JavaSparkContext sc = new JavaSparkContext(conf);
	    String inputFile = args[0]; //path for input

	    long startTime = System.nanoTime();

        JavaRDD<Double> inputdata = sc.textFile(inputFile).map(new Function<String,Double>(){

			@Override
			public Double call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				String[] parts=arg0.split(",");
				return Double.parseDouble(parts[9]);
			}
        	
        });
        
        inputdata.saveAsTextFile("F:\\output");
        
        //System.out.println(inputdata.collect());
//JavaRDD<Double> input = CassandraJavaUtil.javaFunctions(sc).cassandraTable("isd_weather_data", "raw_weather_data", CassandraJavaUtil.mapColumnTo(Double.class)).select("temperature");

          
    /*    JavaDoubleRDD d=   inputdata.mapToDouble(new DoubleFunction<Double>()
        		{


					@Override
					public double call(Double arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0;
					}
        	
        		})  ;
       
        double sum=d.sum();
        double count=d.count();
        double stddev=d.stdev();
        double mean=d.mean();
        double min=d.min();
        double max=d.max();
	    long endTime = System.nanoTime();
        FileWriter resultsFile;
		try {
			resultsFile = new FileWriter(args[2], true);
			resultsFile.write(
"Standard Deviation is "+stddev + "\n min is "+min+"\n max is" +max        
+" \n mean is "+mean+"\n sum is "+sum+" \n count is"+count
+"\n avg"+sum/count);
			resultsFile.write("\n Total time: " + (endTime - startTime)  / 1000000000.0  + " seconds\n");
			resultsFile.flush();
			resultsFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}        
      */  
        
            sc.stop();

    }
}