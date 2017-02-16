

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class RemoteWorker  {

	public static void main(String args[]) {
	try {
boolean flag=true;
SimpleQueueService tasks=new SimpleQueueService("RequestQueue");
int totaTask=tasks.sizeofsqs();
//Scanner reader = new Scanner(System.in);
int totalThreads=Integer.parseInt(args[0]);

//create a pool of threads using executor service
		   ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
			//process the tasks by remote worker by each thread till there is a task in sqs
		   while(flag==true)
		   {
				TaskInfo task=tasks.processRequest();  // get the messages for each task
		   Runnable worker = new RemoteWorkerThread(task);
		   totaTask--;
           executor.execute(worker);  // execute the worker thread
           if(totaTask<=0)            
			   flag=false;// stop the process when all tasks are processed by thread
		   }
           executor.shutdown();
           while (!executor.isTerminated()) {
           }
      
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

    }

}

