import java.util.HashMap;

public class LocalWorkerQueue {

	  HashMap<Integer,String> Tasks=new HashMap<Integer,String>();	
	 int key=1;
	 public LocalWorkerQueue()
	 {
		 key=1;
	 }
	 
	 // inserts the records into hashmap
	 public void insertData(String job)
	 {

		 Tasks.put(key, job);
		 key=key+1;
		 
	 }
	 //get the size of hashmap
	 public int getsize()
	 {	 
		   return Tasks.size();		 
	 }


	 // deletes the records from hashmap
	 public String deleteQueue(int key)
	 { 
		    String s=Tasks.get(key);
		    Tasks.remove(key);
		    return s;	 
	 }
	 
	


}