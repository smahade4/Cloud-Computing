
public class RemoteWorkerThread implements Runnable {

	
	SimpleQueueService  results;
	AmazonDynamoDB dp=new AmazonDynamoDB();
	int dupVal;
	TaskInfo task;
	RemoteWorkerThread(TaskInfo task)
	{ 
		results=new SimpleQueueService("ResponseQueue");
		this.task=task;
	}
    @Override
    public void run() {
	try {
		dp.init();   // initliaze the amazon db table
		dupVal=dp.putddb(task.jobid);  // check in dyanamo db if there are any duplicate
		//System.out.println("dupval is"+dupVal);  
	     if(dupVal==1) // if task is not duplicate process the task
	     {
	   int time = Integer.parseInt(task.job.split(" ")[1]);
         Thread.sleep(time);    
         results.insert_sqs(task.jobid+"is Finished");  //add the output to result queue after task is done	    	    	 
	     }
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

    }
}
