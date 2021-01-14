package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class QueryFromFile {
	public static final int AND=1;
	public static final int OR=0;
	
	//Line 1
	public String rel1;
	public int col1;
	
	
	public String rel2;
	public int col2;
	
	//Line 3
	public int col1ToCompare;
	public int operator;
	public int col2ToCompare;
	
	public int and_or;

	public int col1ToCompare2;
	public int operator2;
	public int col2ToCompare2;
	
	
	public QueryFromFile(String filename) {
		File file = new File(filename);
		if (file.exists()) {
			try {
				FileReader fr = new FileReader(file);
	            BufferedReader br = new BufferedReader(fr);
            
	            //1st line
                String[] projection = br.readLine().trim().split("\\s+");
                rel1 = projection[0].split("_", 0)[0];
                col1 = Integer.parseInt(projection[0].split("_", 0)[1]);
                
                rel2 = projection[1].split("_", 0)[0];
                col2 = Integer.parseInt(projection[1].split("_", 0)[1]);
            	
                String relation = br.readLine();
                
                String[] conditions = br.readLine().trim().split("\\s+");
                col1ToCompare = Integer.parseInt(conditions[0].split("_", 0)[1]);
                operator = Integer.parseInt(conditions[1]);
                col2ToCompare = Integer.parseInt(conditions[2].split("_", 0)[1]);
                
                String and = br.readLine();
                if(and != null) {
                	String[] conditions2 = br.readLine().trim().split("\\s+");
                    col1ToCompare2 = Integer.parseInt(conditions2[0].split("_", 0)[1]);
                    operator2 = Integer.parseInt(conditions2[1]);
                    col2ToCompare2 = Integer.parseInt(conditions2[2].split("_", 0)[1]);
                }
                
            }catch(Exception e) {
            	
            }
		}
		
	}
}
