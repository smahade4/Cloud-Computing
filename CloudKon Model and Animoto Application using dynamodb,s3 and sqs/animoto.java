import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;


public class animoto {
	
	public static void main(String args[])
	{

int totalThreads=Integer.parseInt(args[0]);
SimpleQueueService tasks=new SimpleQueueService("RequestQueue");
		int totalTask = tasks.sizeofsqs();
		boolean flag=true;
			
		//process the tasks by remote worker by each thread till there is a task in sqs
				   ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
				   while(flag==true)
				   {
						TaskInfo task=tasks.processRequest();
						
				   Runnable worker = new RemoteAnimotoThread(task);  // execute the worker thread
				   totalTask--;
		           executor.execute(worker);
		           if(totalTask<=0)            
					   flag=false; // stop the process when all tasks are processed by thread
				   }
		           executor.shutdown();
		           while (!executor.isTerminated()) {
		           }
	}
    
    
	    
}




class RemoteAnimotoThread implements Runnable {

static int totalThreads=0;
	SimpleQueueService  results=new SimpleQueueService("ResponseQueue");
	AmazonDynamoDB dp=new AmazonDynamoDB();
int dupVal;
static BasicAWSCredentials credentials = new BasicAWSCredentials("AKIAIKJ7XKFQAKDOLAZA", "liQQanhZzwZu5NZZjATHdm4SYfotiR+118Ulgcdt");
private static String bucket;

	static AmazonS3 s3client = new AmazonS3Client(credentials);
	   int img=0;
	   
	   TaskInfo task;
	   
	   RemoteAnimotoThread(TaskInfo task)
	   {this.task=task;
	   }
	@Override
   public void run() {
	try {
		//check if there are duplicates in dynamo db 
		dupVal=dp.putddb(task.jobid);
	     if(dupVal==1)
	     {
	    	 //split the task and assign the task to string url
	    	String url[]=task.job.split("@");
	    	 System.out.println("url is"+url[1]);
	    	  String commands;
	    	  //create the url of each string in tasks
	    	 for(int i=1;i<url.length;i++)
	    	 {
	    		 URL urlno = new URL(url[i]);
	    		
	    		 img++;
	    		 //download the image into local folder
	    		 String commandString="wget "+url[i]+" -O "+img+".jpg";
	    		 Process process = Runtime.getRuntime().exec(commandString); 
	    		 
	    	   }
	    	 
	    	 //after images are downloaded to local folder merge them into video using ffmpeg coomand
	    	  commands = "ffmpeg  -i "
	    	            + "" + "img%2d.jpg"  + " " + "video.mpeg";


	    	 System.out.println(commands);
	    		Process p =Runtime.getRuntime().exec(commands);
	    	 //create s3 bucket
	            bucket="ass3";
	            s3client.createBucket(bucket);
	            File f=new File("video.mpeg");	            
//for each video set content length and meradata of video
	             ObjectMetadata omd = new ObjectMetadata();
	 	          omd.setContentLength(f.length());
	             InputStream in = new FileInputStream(f);

	 		   //put the video into s3 bucket using putobject command
	            s3client.putObject(new PutObjectRequest(bucket, "1", in,omd));
	            //generate url for s3 object in the bucket
	            GeneratePresignedUrlRequest generatePresignedUrlRequest = 
					    new GeneratePresignedUrlRequest(bucket, "1");
				generatePresignedUrlRequest.setMethod(HttpMethod.GET); 
//add the s3 bucket url to list of urls by generating url using command generatePresignedUrl
				URL urls = s3client.generatePresignedUrl(generatePresignedUrlRequest); 

				System.out.println("Pre-Signed URL = " + urls.toString());
		         results.insert_sqs(urls.toString());	    	    	 
	     }
	} catch (Exception e) {
		e.printStackTrace();
	}

   }}
