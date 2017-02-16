



import java.io.*;
import java.util.*;
public class SortFinal
{
	// int records =200000000;
	String inputpath;
	String outputpath;

	 public void mergesort()  
    {
		 int i;
	        long start1 = System.currentTimeMillis();
	        System.out.println("Enter No Of Threads");
	        Scanner reader = new Scanner(System.in);  // Reading from System.in

	        int noThreads=reader.nextInt();
	        int size=0;
	        MergeThread[] threads = new MergeThread[noThreads];
	        long beginTime=System.currentTimeMillis();
	         try
	        {
	        File f=new File(inputpath);
	        BufferedReader r =new BufferedReader(new FileReader(inputpath));
	        long records=f.length()/noThreads;
//run loop for no of threads
	        for(i = 0 ; i <noThreads ; i++)
	                {

	             threads[i] = new MergeThread(size,records,inputpath,outputpath,r);
	             size=size+(int)records;

	                }
            for(i = 0 ; i <noThreads ; i++)
            {

                threads[i].join();                                              
            }

	        
            MergeThread inputFiles;
            List<MergeThread> FileList = new ArrayList<MergeThread>();
//add all sorted files to a list
            for(i = 0 ; i <MergeThread.files; i++)
            {
                 int t=i+1;
          String FileName=outputpath+t+".txt";

               inputFiles=new MergeThread(new BufferedReader(new FileReader(FileName)));
         FileList.add(i,inputFiles);

            }
            //maintain a priority queue that gives sorted record by comparing string
            PriorityQueue<MergeThread> FileQueue = new PriorityQueue<MergeThread>(
                    new Comparator<MergeThread>() {
                           public int compare(MergeThread File1,
                                       MergeThread File2) {

                                   return File1.getData().compareTo(File2.getData());
//compare the two files and return the minimum value record
                           }
                   });

            //add the files to list
            for (i=0;i<FileList.size();i++)
            {

                MergeThread File= FileList.get(i);
               if (!File.equals(null))
                 {
                         FileQueue.add(File);

                 }
            }
            
            BufferedWriter bw=new BufferedWriter(new FileWriter(outputpath+"FinalData.txt"));
              //while priority queue is not empty and file has records
            //read the String top value for each file 
            //compare two  values 
            //store the minimum value and write it to the file
            //read new string value for that file
           while(FileQueue!=null)
            {

       MergeThread FileNo;
         if((FileNo=FileQueue.poll())!=null);
         {

               String output;
          if(!(FileNo.getData().equals("")||FileNo.getData().equals(null)))
               {
                       output=FileNo.getData();
            bw.write(output);
            bw.newLine();
            bw.flush();
               FileNo.putData();
               }
            if(FileNo.getData()==null) {
                FileNo.rf.close();
                FileQueue.remove(FileNo);
               }

             else if(!FileNo.equals(null)){
                    FileQueue.add(FileNo);
             }
         }



      }
      long endTime=System.currentTimeMillis();

 System.out.println("TimeTaken"+(endTime-beginTime)/60*1000);
	        }            
	         catch(Exception e)
	         {

	                  long endTime=System.currentTimeMillis();

	         System.out.println("TimeTaken"+((float)(endTime-beginTime)/(1000)));
	         }
          
        }
    
   	          
    public static void main(String[] args) {
    	
    	SortFinal t =new SortFinal();
    	t.inputpath=args[0];
    	t.outputpath=args[1];
    	t.mergesort() ;
    }
}