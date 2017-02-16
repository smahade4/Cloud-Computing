
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_TeraSort extends Reducer<Text, Text, Text,Text> {
	public void reduce(Text key, Text value,Context context)
	{	
		try
	
	{
		 context.write(key,value);
	}
		catch(Exception e)
		{System.out.println(e.getMessage());			
		}
	}
	
}
