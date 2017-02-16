import java.util.List;
import java.util.Map;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class SimpleQueueService {

	String myQueueUrl;
	AmazonSQS sqs;
	BasicAWSCredentials credentials = new BasicAWSCredentials("AKIAIKJ7XKFQAKDOLAZA", "liQQanhZzwZu5NZZjATHdm4SYfotiR+118Ulgcdt");
	 CreateQueueRequest createQueueRequest;
	 //in constructor create queue for sqs using cerdentials 
	 SimpleQueueService(String QueueName)
		{
		 sqs = new AmazonSQSClient(credentials);
		 Region usWest2 = Region.getRegion(Regions.US_WEST_2);	        
	        sqs.setRegion(usWest2);
	         
		 createQueueRequest = new CreateQueueRequest(QueueName);
		    myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
	       
		}
	 
	 //insert message in sqs queue passed as parameter tasks
	    public void insert_sqs(String tasks) 
	    {  	 

	        sqs.sendMessage(new SendMessageRequest(myQueueUrl, tasks)); 
	    	
	    }
	    
	    //process the queue and get messages from the queue return job and jobid stored in class variable TaskInfo
	    public TaskInfo processRequest()
	    {
	   
	    ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest("RequestQueue");
	    List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
	    String job = null;
	    String jobid = null;
	    TaskInfo t=new TaskInfo();

        for(int i=0;i<messages.size();i++)
	    {
	Message m=messages.get(i);	
	job=m.getBody();
	jobid=m.getMessageId();
	t.jobid=jobid;
	t.job=job;
	
	if(messages.size()>0)
	{
	     CreateQueueRequest createQueueRequest = new CreateQueueRequest("RequestQueue");
         String messageRecieptHandle = messages.get(0).getReceiptHandle();
         sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
	}
	

	    }
	    return t;
		 }

	    //gives the size of approximate messages in sqs queue
public int sizeofsqs()
{
	  GetQueueAttributesRequest request = new GetQueueAttributesRequest();
      request = request.withAttributeNames("ApproximateNumberOfMessages");
    
       request = request.withQueueUrl(myQueueUrl);

          Map<String, String> attrs = sqs.getQueueAttributes(request).getAttributes();

          // get the approximate number of messages in the queue
          int sizeOfMessages = Integer.parseInt(attrs.get("ApproximateNumberOfMessages"));

    
	return sizeOfMessages;
}

//delete a queue
public void deleteQueue()
{
sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
}

}

class TaskInfo
{
	String jobid;
	String job;
	
}