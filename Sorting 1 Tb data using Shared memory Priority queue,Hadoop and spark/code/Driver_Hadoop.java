
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver_Hadoop {

	public Driver_Hadoop()
	{
		
	}
		   

	public static void main(String[] args) 
	{	
		try
		{
		   
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		
		 job.setJarByClass(Driver_Hadoop.class);
	    	
  job.setMapperClass(Mapper_TeraSort.class);
	 job.setReducerClass(Reducer_TeraSort.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(Text.class);
	 
	 

	 FileInputFormat.addInputPath(job, new Path(args[0]));

 FileOutputFormat.setOutputPath(job, new Path(args[1]));	    	  

	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
	catch(Exception e)
	{
		e.getMessage();
	}

	
	}



}
