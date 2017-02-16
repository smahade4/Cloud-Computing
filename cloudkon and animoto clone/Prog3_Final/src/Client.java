import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Client {

	
	public static void main(String args[])
	{
		
		 LocalWorkerQueue RequestQueue;
			SimpleQueueService s;
//take the file and workertype and nothreads
		String filename=args[0];
		String WorkerType=args[1];
		int noThreads=Integer.parseInt(args[2]);
		int noTasks=0;
    	BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(filename));
		
        String data,animototask ="";
        data=br.readLine();
        List<String> tasks=new ArrayList<String>(); // list of tasks
        int size=0;
        System.out.println("enter no of workers");
        Scanner scan=new Scanner(System.in);
        int noWorkers=scan.nextInt(); // no of workers to run
        
        while(data!=null &&!(data.isEmpty()))
        {
        	tasks.add(data);
        	data=br.readLine();
        }
        
        for(int i=0;i<tasks.size();i++)
        {
        	if(!(WorkerType.equals("animoto")))
        	{
        	int time=Integer.parseInt(tasks.get(i).split(" ")[1]);
        	
        	//based on sleep job set no of tasks
        	if(time==0)
        	{
        		noTasks=10000;
        		
        	}
        	if(time==10)
        	{
        		noTasks=1000;
        		
        	}if(time==1000)
        	{
        		noTasks=100;
        		
        	}if(time==10000)
        	{
        		noTasks=10;	
        	}
    		 size=size+(noTasks*noWorkers); //assign size of task for checking 
    		 			//if response queue has size no of records
    		 	
        	}
        	else
        	{
        		noTasks=160;	
        	}
        	// if workertype is local create local queue insert tasks into local queue
        //process the data 
        	//create object of local worker and process each task in local queue
        	if(WorkerType.equals("local"))
        	{

                long beginTime=System.currentTimeMillis();
        		RequestQueue=new LocalWorkerQueue();
//insert data to request queue with noof tasks as loop condition
        		for(int j=0;j<noTasks;j++)
        		{
        		RequestQueue.insertData(tasks.get(i));
        		}
        		size=RequestQueue.getsize();
//create local worker object and process the queue
        		LocalWorker l=new LocalWorker(RequestQueue,noThreads);
        		l.process();
        		while(l.ResponseQueue.getsize()!=size && l.flag!=false)
        		{
        			
        		}
        	//	System.out.println(size+"sie"+l.ResponseQueue.getsize());

        		if(l.ResponseQueue.getsize()==size)
        		{
        			System.out.println("process done");	
        			long endTime=System.currentTimeMillis();
                    System.out.println("time taken"+(endTime-beginTime));
        		}

                
        	}
        	

        	// if workertype is remote create sqs queue insert tasks into remote queue
        //process the data 
        	//Remote worker will process each task in sqs queue from different nodes
        	if(WorkerType.equals("remote"))
        	{	

                long beginTime=System.currentTimeMillis();
        		s=new SimpleQueueService("RequestQueue");
        		for(int j=0;j<noTasks*noWorkers;j++)
        		{
        		s.insert_sqs(tasks.get(i));
        		}
        		size=s.sizeofsqs();
        		s=new SimpleQueueService("ResponseQueue");
        		System.out.println(s+" "+size);
        		/*
        		while(s.sizeofsqs()!=size)
        		{
        			
        		}
        		
        		if(s.sizeofsqs()==size)
        		{
           			System.out.println("process done");	
        			long endTime=System.currentTimeMillis();
                    System.out.println("time taken"+(endTime-beginTime));
     
        			
        		}
        		*/
        	}
        	// if workertype is animoto create sqs queue insert url tasks into SQS queue
            	
        	if(WorkerType.equals("animoto"))
        	{
        		animototask=animototask+"@"+tasks.get(i); // append all 60 images as one task
        	}
        	
        }
        
      //add animoto url combined as one task to sqs
        
    	if(WorkerType.equals("animoto"))
    	{
    		animototask=animototask+"@";
    		s=new SimpleQueueService("RequestQueue");
		
    		for(int i=0;i<noTasks;i++)
    		{
    			s.insert_sqs(animototask);
    	        		
    		}
    		 size=s.sizeofsqs();
    		s=new SimpleQueueService("ResponseQueue");
    		/*while(s.sizeofsqs()!=size)
    		{
    			
    		}
    		if(s.sizeofsqs()==size)
    		{
    			System.out.println("process done");
    			s.deleteQueue();
    		}*/
    		
    	}
        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
