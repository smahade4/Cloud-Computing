

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_TeraSort extends Mapper<LongWritable, Text, Text, Text>  {
	 public void map(LongWritable Key,Text input,Context context)
	     {
		 try
		 {
			String keysort=input.toString().substring(0, 10);
	        String value=input.toString().substring(11,98);
	        context.write(new Text(keysort), new Text(value));
		 }
		 catch(Exception e)
		 {
			 System.out.println(e.getMessage());
		 }
		 }
}
