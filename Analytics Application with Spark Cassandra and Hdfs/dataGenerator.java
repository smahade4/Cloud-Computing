import java.awt.List;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class dataGenerator {

	
	public static void main(String args[])
	{
		 long filesize=1000000;
		int[] a = new int[100000];
		int test=1;
		System.out.println(filesize);
		for(int i=0;i<filesize;i++)
		{
			//System.out.println(test);
			a[test]=i;
			if(test==99999)
			{
				 FileWriter resultsFile;
					try {

						resultsFile = new FileWriter(args[0], true);
						for(int j=1;j<a.length;j++)
						{
						//	System.out.println(a[j]);
							resultsFile.write(String.valueOf(a[j]));
							resultsFile.write("\n");
						}
						resultsFile.flush();
						resultsFile.close();
				
						test=1;
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}  
			}
			test++;
		}
	}
}

