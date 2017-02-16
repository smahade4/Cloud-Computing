



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


public class MergeThread extends Thread {
    
    private   int  begin=0;
    private   long end=0;
    int t;

	BufferedReader rf1;

      BufferedReader rf;
      public static int files=0;
     int test=0;
     String data=null;
     String inputpath;
    String outputpath;
     List <Map<String, String>> maplist=new ArrayList<Map<String, String>>();
	
	 MergeThread(BufferedReader reader)
     {
             this.rf=reader;
          
     try {


             data=rf.readLine();

     } catch (IOException e) {
             // TODO Auto-generated catch block
             e.printStackTrace();
     }
     }

	
	 MergeThread(int begin,long end,String inputpath,String outputpath,BufferedReader r) {

	        synchronized (this)
	         {
	                
	        this.begin =begin;
	        this.end =end;       
	       this.inputpath=inputpath;
	       this.outputpath=outputpath;

	                        rf1 =r;


	       this.start();


	         }
	    }
     public String putData()
     {
                     String s= data;
                     try {
                             data=rf.readLine();

                     } catch (Exception e) {

                     e.printStackTrace();
                     }
                     return s;
     }
	public String getData()
	{
	try {
		return data;
		} catch (Exception e) {
		e.printStackTrace();
		return null;
	  }
	}

     @Override
     public synchronized void run() {
    	 
    	 try
    	 {
    	 
    	 Map<String, String> inputmap=new HashMap<String, String>();

         String line=rf1.readLine(); 
                 test =begin;
              while((test<=begin+end)) //run loop till maximum records set for each thread
                  {

                inputmap.put(line.substring(0,10),line.substring(10,98)); //store the line read into map 
                         t=t+100; 
                 if(t==10000000)  //loop condition for map to check if 
                	 //contains maximum records possible in memory sort data and write it back to disk
                 {
      
                	 	t=0;
                   	Set<String> keys = inputmap.keySet();//store the map key

        	        ArrayList<String> keyList = new ArrayList<String>(); 
        	        keyList.addAll(keys); 
        	     
        	       quickSort(keyList,0,keyList.size()-1); //sort the map keys

               		files++;
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputpath+files+".txt"));
//write back the key and value pairs to disk
               		try
          	      {
          	        for(int i=0;i<keyList.size();i++)
          	        {
          	      	writer.write(keyList.get(i));
          	      	writer.flush();
          	      	writer.write(inputmap.get(keyList.get(i)));	
          	      	writer.flush();
          	      	
          	      	if(i<100000)
          	      	{
          	      	writer.newLine();      
          	         writer.flush();
          	      	} 
          	      }
                    inputmap.clear();
                    keyList.clear();
                    keys.clear();
                   // System.out.println(Thread.currentThread());
                      }

                        catch(Exception e)
                        {
                                throw new RuntimeException(e);
                        }
                        finally
                        {

                                writer.close();
                        }
                     }
                line=rf1.readLine();
                         test=test+100;

            
                 }  
        } catch (Exception ex) {
        
        }
            }

     boolean less(String v, String w) {
         return v.substring(0, 10).compareTo(w.substring(0, 10)) < 0;
     }
     boolean isSorted(ArrayList<String> Arr,int lo, int hi) {
         for (int i = lo + 1; i <= hi; i++)
         if (less(Arr.get(i), Arr.get(i-1)))
               return false;
         return true;
     }

public static void  quickSort(ArrayList<String> keyColList,int first,int last)
{
String pivot = keyColList.get(first+((last-first)/2)); 
      int low = first ; 
      int high = last; 

         
	do
{	 while (low < high && keyColList.get(low).compareTo(pivot) < 0)
          {

              low++;
          }
		  
      while (high>low && keyColList.get(high).compareTo(pivot) > 0)
      {
          high--;
      }
	  
      if (low <= high) 
      {

          String temp = keyColList.get(high);
          keyColList.set(high,keyColList.get(low));
          keyColList.set(low,temp);
	low++;
	high--;

      }
 } while(low<=high) ;
  
   if(first < high) {
     quickSort(keyColList, first, high);
  }
  if(low < last) {
     quickSort(keyColList, low, last);
  }
}

     }

    

