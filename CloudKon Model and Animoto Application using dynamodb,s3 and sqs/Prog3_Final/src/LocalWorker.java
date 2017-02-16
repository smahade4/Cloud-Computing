import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
public class LocalWorker {
	   private LocalWorkerQueue RequestQueue;
    int totalThreads;
    LocalWorkerQueue ResponseQueue;
	Worker worker;
	boolean flag=true;
	public LocalWorker(LocalWorkerQueue RequestQueue,int t)
	{
		this.RequestQueue=RequestQueue;
		this.totalThreads=t;
	}
 
	public void process()
	{
	//create a pool of threads using executor service
		ExecutorService executor = Executors.newFixedThreadPool(totalThreads);		   
		int size=RequestQueue.getsize();
		
		//in a loop process the tasks in local queue by each thread
		ResponseQueue=new LocalWorkerQueue();
		
		for(int j=1;j<=size;j++)	
		{	
			String s=RequestQueue.deleteQueue(j); // get the data from local queue into string
			int time = Integer.parseInt(s.split(" ")[1]); // split the data to get the sleep time
			worker=new Worker(time,j,ResponseQueue);
			executor.execute(worker);  //each thread process the tasks
			ResponseQueue.insertData("task"+j+"completed");
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
        }
		flag=false;
  //System.out.println("j is"+ResponseQueue.getsize());
	}

	
}

 class Worker implements Runnable
{
int key;
int time;    


public Worker(int time,int key,LocalWorkerQueue ResponseQueue)
{
this.time=time;	
this.key=key;

}
	//perform the sleep task in thread
	@Override
	public   void run() {
try {

     
            
        	Thread.sleep(time);
        	System.out.println("sleeping" + Thread.currentThread()+" ");
        }
        	catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
	}
    
}
