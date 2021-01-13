package tests;

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;


class JoinsLab3Driver implements GlobalConst {
  private final String DATA_DIR_PATH = "/home/julien/Documents/EURECOM/DBSys/LAB3/queriesdata/";
  private boolean OK = true;
  private boolean FAIL = false;

  /** Constructor
   */
  public JoinsLab3Driver() {
    

    boolean status = OK;
    int numsailors = 25;
    int numsailors_attrs = 4;
    int numreserves = 10;
    int numreserves_attrs = 3;
    int numboats = 5;
    int numboats_attrs = 3;
    
    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

    String remove_cmd = "/bin/rm -rf ";
    String remove_logcmd = remove_cmd + logpath;
    String remove_dbcmd = remove_cmd + dbpath;
    String remove_joincmd = remove_cmd + dbpath;

    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
      Runtime.getRuntime().exec(remove_joincmd);
    }
    catch (IOException e) {
      System.err.println (""+e);
    }
    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

    
    //Build S.in
    System.out.println("dir : "+DATA_DIR_PATH+"S.txt");
    File2Heap(DATA_DIR_PATH+"S.txt", "S.in", 100);
    
    //Build R.in database
    File2Heap(DATA_DIR_PATH+"R.txt", "R.in", 100);
    
    //Build Q.in database
    File2Heap(DATA_DIR_PATH+"q_small.txt", "Q.in", 1000);
   
  }
  
 
  public boolean runTests() {
    
    Disclaimer();
   // Query1a("query_1a.txt");
   // Query1b("query_1b.txt");

    Query2a("query_2a.txt");
    
    System.out.print ("Finished joins testing"+"\n");
   
    
    return true;
  }
  private void CondExpr_Query1a(CondExpr[] outFilter, int operator, int outerSymbol, int innerSymbol) {
	outFilter[0].next  = null;
	outFilter[0].op    = new AttrOperator(operator);
	outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
	outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
	outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),outerSymbol);
	outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),innerSymbol);

	outFilter[1] = null;
}
  private void CondExpr_Query1b(CondExpr[] outFilter, int operator, int outerSymbol, int innerSymbol, int outer2Symbol, int inner2Symbol, int operator2  ) {
	outFilter[0].next  = null;
	outFilter[0].op    = new AttrOperator(operator);
	outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
	outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
	outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),outerSymbol);
	outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),innerSymbol);

	outFilter[1].next  = null;
	outFilter[1].op    = new AttrOperator(operator2);
	outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
	outFilter[1].type2 = new AttrType(AttrType.attrSymbol);
	outFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),outer2Symbol);
	outFilter[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),inner2Symbol);

	outFilter[2] = null;
	}
  
  private void Query1a(String queryFile) {
	  System.out.print("**********************Query1a strating *********************\n");
	    boolean status = OK;

	    // Sailors, Boats, Reserves Queries.
	    System.out.print 
	      ("Query:\n"
	      		+ "SELECT R.c1, S.c1\n"
	      		+ "FROM   R, S\n"
	      		+ "WHERE R.c3 < S.c3\n"
	       + "using simple nested-loop join.)\n\n");
	    
	    
	    CondExpr [] outFilter  = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		
		QueryFromFile query = new QueryFromFile(DATA_DIR_PATH+queryFile);
		
		CondExpr_Query1a(outFilter, query.operator, query.col1ToCompare, query.col2ToCompare );
		
		Tuple t = new Tuple();
		t = null;
		AttrType Stypes[] = {
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger)
		};
		short[] Ssizes = null;
		
		AttrType [] Rtypes = {
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger)
		};
		short[] Rsizes = null;
		FldSpec [] Sprojection = {
				new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2),
				new FldSpec(new RelSpec(RelSpec.outer), 3),
				new FldSpec(new RelSpec(RelSpec.outer), 4),
		};
		
		FldSpec [] Projection = {
				new FldSpec(new RelSpec(RelSpec.outer), query.col1),
				new FldSpec(new RelSpec(RelSpec.innerRel), query.col2)
		};
	    
		iterator.Iterator am = null;

		try {
			//TODO : implement from txt file
			am = new FileScan(query.rel1+".in", 
					Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		// Nested Loop Join
		NestedLoopsJoins nlj = null;
		try {
			nlj = new NestedLoopsJoins (Stypes, 4, Ssizes,
					Rtypes, 4, Rsizes,
					10,
					am, query.rel2+".in",
					outFilter, null, Projection, 2);
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for nested_loop_join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		AttrType[] jtype = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};

		t = null;
		int i = 0;
		PrintWriter pw;
		try {
			
			pw = new PrintWriter(DATA_DIR_PATH+"output_query1a.txt");
			
			while ((t = nlj.get_next()) != null) {
				i++;
				//t.print(jtype); // print results
				pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // get tuples in .txt file
			}
			pw.close();
			// print the total number of returned tuples
			System.out.println("Output Tuples for query 1a: " + i);
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try {
			nlj.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		if (status != OK) {
			//bail out
			Runtime.getRuntime().exit(1);
		}
  }

  private void Query1b(String queryFile) {

	  
	  System.out.print("**********************Query1b starting *********************\n");
	    boolean status = OK;

	    // Sailors, Boats, Reserves Queries.
	    System.out.print 
	      ("Query:\n"
	      		+ "SELECT R.c1, S.c1\n"
	      		+ "FROM   R, S\n"
	      		+ "WHERE R.c3 < S.c3\n"
	      		+ "AND\n"
	      		+ "R.c4 <= S.c4\n"
	      		
	       + "using simple nested-loop join.)\n\n");
	    
	    
	    CondExpr [] outFilter  = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();
		
		QueryFromFile query = new QueryFromFile(DATA_DIR_PATH+queryFile);
		
		CondExpr_Query1b(
				outFilter, 
				query.operator, query.col1ToCompare, query.col2ToCompare,  
				query.operator2, query.col1ToCompare2, query.col2ToCompare2);
		
		Tuple t = new Tuple();
		t = null;
		AttrType Stypes[] = {
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger)
		};
		short[] Ssizes = null;
		
		AttrType [] Rtypes = {
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger)
		};
		short[] Rsizes = null;
		FldSpec [] Sprojection = {
				new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2),
				new FldSpec(new RelSpec(RelSpec.outer), 3),
				new FldSpec(new RelSpec(RelSpec.outer), 4),
		};
		
		FldSpec [] Projection = {
				new FldSpec(new RelSpec(RelSpec.outer), query.col1),
				new FldSpec(new RelSpec(RelSpec.innerRel), query.col2)
		};
	    
		iterator.Iterator am = null;

		try {
			//TODO : implement from txt file
			am = new FileScan(query.rel1+".in", 
					Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		// Nested Loop Join
		NestedLoopsJoins nlj = null;
		try {
			nlj = new NestedLoopsJoins (Stypes, 4, Ssizes,
					Rtypes, 4, Rsizes,
					10,
					am, query.rel2+".in",
					outFilter, null, Projection, 2);
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for nested_loop_join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		AttrType[] jtype = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};

		t = null;
		int i = 0;
		PrintWriter pw;
		try {
			
			pw = new PrintWriter(DATA_DIR_PATH+"output_query1b.txt");
			
			while ((t = nlj.get_next()) != null) {
				i++;
				//t.print(jtype); // print results
				pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // get tuples in .txt file
			}
			pw.close();
			// print the total number of returned tuples
			System.out.println("Output Tuples for query 1b: " + i);
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try {
			nlj.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		if (status != OK) {
			//bail out
			Runtime.getRuntime().exit(1);
		}
  }

  private void Query2a(String queryFile) {
	  System.out.print("**********************Query2a string *********************\n");
	    boolean status = OK;

	    // Sailors, Boats, Reserves Queries.
	    System.out.print 
	      ("Query:\n"
	      		+ "SELECT R.c1, S.c1\n"
	      		+ "FROM   R, S\n"
	      		+ "WHERE R.c3 < S.c3\n"
	       + "using self join.)\n\n");
	    
	    
	    CondExpr [] outFilter  = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		
		QueryFromFile query = new QueryFromFile(DATA_DIR_PATH+queryFile);
		
		CondExpr_Query1a(outFilter, query.operator, query.col1ToCompare, query.col2ToCompare );
		
		Tuple t = new Tuple();
		t = null;
		AttrType Stypes[] = {
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger)
		};
		short[] Ssizes = null;
		
		AttrType [] Rtypes = {
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger)
		};
		short[] Rsizes = null;
		FldSpec [] Sprojection = {
				new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2),
				new FldSpec(new RelSpec(RelSpec.outer), 3),
				new FldSpec(new RelSpec(RelSpec.outer), 4),
		};
		
		FldSpec [] Projection = {
				new FldSpec(new RelSpec(RelSpec.outer), query.col1),
				new FldSpec(new RelSpec(RelSpec.innerRel), query.col2)
		};
	    
		iterator.Iterator am = null;

		try {
			//TODO : implement from txt file
			am = new FileScan(query.rel1+".in", 
					Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		// Nested Loop Join
		NestedLoopsJoins nlj = null;
		try {
			nlj = new NestedLoopsJoins (Stypes, 4, Ssizes,
					Rtypes, 4, Rsizes,
					10,
					am, query.rel2+".in",
					outFilter, null, Projection, 2);
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for nested_loop_join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		AttrType[] jtype = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};

		t = null;
		int i = 0;
		PrintWriter pw;
		try {
			
			pw = new PrintWriter(DATA_DIR_PATH+"output_query1a.txt");
			
			while ((t = nlj.get_next()) != null) {
				i++;
				//t.print(jtype); // print results
				pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // get tuples in .txt file
			}
			pw.close();
			// print the total number of returned tuples
			System.out.println("Output Tuples for query 2a: " + i);
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try {
			nlj.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		if (status != OK) {
			//bail out
			Runtime.getRuntime().exit(1);
		}
  }
  private boolean File2Heap(String fileNameInput, String fileNameOutput, int max_num_tuples){
	     
	    /**
	    * 
	    * BUILD table from "fileInput"
	    * 
	    *  @parameter fileNameInput Name of file containing data
	    *  @parameter fileNameOutput Name of table saved in the DB
	    *  @parameter max_num_tuples Max number of tuple to load from the file in case ofbig files. 
	    *  
	    * **/  
	   
	    if(fileNameInput==null || fileNameOutput==null) {
	     return false;
	    }
	    
	    if(max_num_tuples<=0) {
	     max_num_tuples=Integer.MAX_VALUE; // Load tuples until the HeapFile can contain them
	    }
	     /* Create relation */
	     
	     AttrType [] types = new AttrType[4];
	     types[0] = new AttrType (AttrType.attrInteger);
	     types[1] = new AttrType (AttrType.attrInteger);
	     types[2] = new AttrType (AttrType.attrInteger);
	     types[3] = new AttrType (AttrType.attrInteger);
	     
	     short numField=4;
	       
	     Tuple t = new Tuple();
	       
	     try {
	      t.setHdr(numField,types, null);
	     }
	     catch (Exception e) {
	       
	      System.err.println("*** error in Tuple.setHdr() ***");
	         e.printStackTrace();
	         return false;
	     }
	       
	      int t_size = t.size();
	       
	      RID rid;
	       
	      Heapfile f = null;
	       
	      try {
	       f = new Heapfile(fileNameOutput);

	      }
	       
	      catch (Exception e) {
	       System.err.println("*** error in Heapfile constructor ***");
	       e.printStackTrace();
	       return false;
	      }
	       
	       
	      t = new Tuple(t_size);
	      
	      try {
	       t.setHdr((short) 4, types, null);
	      }
	      catch (Exception e) {
	       System.err.println("*** error in Tuple.setHdr() ***");
	       e.printStackTrace();
	       return false;
	      }
	      
	      int cont=0; // To limit the size of table
	      
	      try {
	    
	       File file = new File(fileNameInput);
	       BufferedReader reader=null;
	       reader = new BufferedReader(new FileReader(file));
	    
	       String text = null;
	       text = reader.readLine(); //To skip header
	       text="";
	       
	       while ((text = reader.readLine()) != null && cont!=max_num_tuples) {
	         
	        String[] attributes=text.split(",");
	        t.setIntFld(1, Integer.parseInt(attributes[0]));
	        t.setIntFld(2, Integer.parseInt(attributes[1]));    
	        t.setIntFld(3, Integer.parseInt(attributes[2]));
	        t.setIntFld(4, Integer.parseInt(attributes[3]));
	        f.insertRecord(t.getTupleByteArray());
	        cont++;
	    }
	    reader.close();
	      }
	      catch(FileNotFoundException e1) {
	       System.err.println("*** File "+fileNameInput+" ***");
	       e1.printStackTrace();
	       return false;
	      }
	      catch (Exception e) {
	       
	       System.err.println("*** Heapfile error in Tuple.setIntFld() ***");
	       e.printStackTrace();
	       return false;
	        
	      }   
	      
	      System.out.println("Number of tuple inserted: "+cont);  
	      return true;
	      
	}
  private void Query1_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

    expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[1].next  = null;
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    expr[1].type2 = new AttrType(AttrType.attrInteger);
    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
    expr[1].operand2.integer = 1;
 
    expr[2] = null;
  }

  private void Query2_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    
    expr[1] = null;
 
    expr2[0].next  = null;
    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ); 
    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
    expr2[0].type2 = new AttrType(AttrType.attrSymbol);   
    expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    
    expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
    expr2[1].next = null;
    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
    expr2[1].type2 = new AttrType(AttrType.attrString);
    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
    expr2[1].operand2.string = "red";
 
    expr2[2] = null;
  }

  private void Query3_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    expr[1] = null;
  }

  private CondExpr[] Query5_CondExpr() {
    CondExpr [] expr2 = new CondExpr[3];
    expr2[0] = new CondExpr();
    
   
    expr2[0].next  = null;
    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    
    expr2[1] = new CondExpr();
    expr2[1].op   = new AttrOperator(AttrOperator.aopGT);
    expr2[1].next = null;
    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
   
    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
    expr2[1].type2 = new AttrType(AttrType.attrReal);
    expr2[1].operand2.real = (float)40.0;
    

    expr2[1].next = new CondExpr();
    expr2[1].next.op   = new AttrOperator(AttrOperator.aopLT);
    expr2[1].next.next = null;
    expr2[1].next.type1 = new AttrType(AttrType.attrSymbol); // rating
    expr2[1].next.operand1.symbol = new FldSpec ( new RelSpec(RelSpec.outer),3);
    expr2[1].next.type2 = new AttrType(AttrType.attrInteger);
    expr2[1].next.operand2.integer = 7;
 
    expr2[2] = null;
    return expr2;
  }

  private void Query6_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
   
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

    expr[1].next  = null;
    expr[1].op    = new AttrOperator(AttrOperator.aopGT);
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    
    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
    expr[1].type2 = new AttrType(AttrType.attrInteger);
    expr[1].operand2.integer = 7;
 
    expr[2] = null;
 
    expr2[0].next  = null;
    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

    expr2[1].next = null;
    expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
    
    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
    expr2[1].type2 = new AttrType(AttrType.attrString);
    expr2[1].operand2.string = "red";
 
    expr2[2] = null;
  }

  public void Query1() {
    
    System.out.print("**********************Query1 strating *********************\n");
    boolean status = OK;
    
    // Sailors, Boats, Reserves Queries.
    System.out.print ("Query: Find the names of sailors who have reserved "
		      + "boat number 1.\n"
		      + "       and print out the date of reservation.\n\n"
		      + "  SELECT S.sname, R.date\n"
		      + "  FROM   Sailors S, Reserves R\n"
		      + "  WHERE  S.sid = R.sid AND R.bid = 1\n\n");
    
    System.out.print ("\n(Tests FileScan, Projection, and Sort-Merge Join)\n");
 
    CondExpr[] outFilter = new CondExpr[3];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
    outFilter[2] = new CondExpr();
 
    Query1_CondExpr(outFilter);
 
    Tuple t = new Tuple();
    
    AttrType [] Stypes = new AttrType[4];
    Stypes[0] = new AttrType (AttrType.attrInteger);
    Stypes[1] = new AttrType (AttrType.attrString);
    Stypes[2] = new AttrType (AttrType.attrInteger);
    Stypes[3] = new AttrType (AttrType.attrReal);

    //SOS
    short [] Ssizes = new short[1];
    Ssizes[0] = 30; //first elt. is 30
    
    FldSpec [] Sprojection = new FldSpec[4];
    Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
    Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

    CondExpr [] selects = new CondExpr [1];
    selects = null;
    
 
    FileScan am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes, 
				  (short)4, (short)4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
    
    AttrType [] Rtypes = new AttrType[3];
    Rtypes[0] = new AttrType (AttrType.attrInteger);
    Rtypes[1] = new AttrType (AttrType.attrInteger);
    Rtypes[2] = new AttrType (AttrType.attrString);

    short [] Rsizes = new short[1];
    Rsizes[0] = 15; 
    FldSpec [] Rprojection = new FldSpec[3];
    Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
 
    FileScan am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
				  (short)3, (short) 3,
				  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }
   
    
    FldSpec [] proj_list = new FldSpec[2];
    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

    AttrType [] jtype = new AttrType[2];
    jtype[0] = new AttrType (AttrType.attrString);
    jtype[1] = new AttrType (AttrType.attrString);
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4, 
			 1, 4, 
			 10,
			 am, am2, 
			 false, false, ascending,
			 outFilter, proj_list, 2);
    }
    catch (Exception e) {
      System.err.println("*** join error in SortMerge constructor ***"); 
      status = FAIL;
      System.err.println (""+e);
      e.printStackTrace();
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }

   
 
    QueryCheck qcheck1 = new QueryCheck(1);
 
   
    t = null;
 
    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);

        qcheck1.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
       e.printStackTrace();
       status = FAIL;
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in get next tuple ");
      Runtime.getRuntime().exit(1);
    }
    
    qcheck1.report(1);
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    System.out.println ("\n"); 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in closing ");
      Runtime.getRuntime().exit(1);
    }
  }
  
  public void Query2() {
    System.out.print("**********************Query2 strating *********************\n");
    boolean status = OK;

    // Sailors, Boats, Reserves Queries.
    System.out.print 
      ("Query: Find the names of sailors who have reserved "
       + "a red boat\n"
       + "       and return them in alphabetical order.\n\n"
       + "  SELECT   S.sname\n"
       + "  FROM     Sailors S, Boats B, Reserves R\n"
       + "  WHERE    S.sid = R.sid AND R.bid = B.bid AND B.color = 'red'\n"
       + "  ORDER BY S.sname\n"
       + "Plan used:\n"
       + " Sort (Pi(sname) (Sigma(B.color='red')  "
       + "|><|  Pi(sname, bid) (S  |><|  R)))\n\n"
       + "(Tests File scan, Index scan ,Projection,  index selection,\n "
       + "sort and simple nested-loop join.)\n\n");
    
    // Build Index first
    IndexType b_index = new IndexType (IndexType.B_Index);

   
    //ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index, 1);
    // }
    //catch (Exception e) {
    // e.printStackTrace();
    // System.err.print ("Failure to add index.\n");
      //  Runtime.getRuntime().exit(1);
    // }
    
    


    CondExpr [] outFilter  = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();

    CondExpr [] outFilter2 = new CondExpr[3];
    outFilter2[0] = new CondExpr();
    outFilter2[1] = new CondExpr();
    outFilter2[2] = new CondExpr();

    Query2_CondExpr(outFilter, outFilter2);
    Tuple t = new Tuple();
    t = null;

    AttrType [] Stypes = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrReal)
    };

    AttrType [] Stypes2 = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString), 
    };

    short []   Ssizes = new short[1];
    Ssizes[0] = 30;
    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString), 
    };

    short  []  Rsizes = new short[1] ;
    Rsizes[0] = 15;
    AttrType [] Btypes = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrString), 
    };

    short  []  Bsizes = new short[2];
    Bsizes[0] =30;
    Bsizes[1] =20;
    AttrType [] Jtypes = {
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrInteger), 
    };

    short  []  Jsizes = new short[1];
    Jsizes[0] = 30;
    AttrType [] JJtype = {
      new AttrType(AttrType.attrString), 
    };

    short [] JJsize = new short[1];
    JJsize[0] = 30;
    FldSpec []  proj1 = {
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.innerRel), 2)
    }; // S.sname, R.bid

    FldSpec [] proj2  = {
       new FldSpec(new RelSpec(RelSpec.outer), 1)
    };
 
    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       // new FldSpec(new RelSpec(RelSpec.outer), 3),
       // new FldSpec(new RelSpec(RelSpec.outer), 4)
    };
 
    CondExpr [] selects = new CondExpr[1];
    selects[0] = null;
    
    
    //IndexType b_index = new IndexType(IndexType.B_Index);
    iterator.Iterator am = null;
   

    //_______________________________________________________________
    //*******************create an scan on the heapfile**************
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // create a tuple of appropriate size
        Tuple tt = new Tuple();
    try {
      tt.setHdr((short) 4, Stypes, Ssizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int sizett = tt.size();
    tt = new Tuple(sizett);
    try {
      tt.setHdr((short) 4, Stypes, Ssizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    Heapfile        f = null;
    try {
      f = new Heapfile("sailors.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    Scan scan = null;
    
    try {
      scan = new Scan(f);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // create the index file
    BTreeFile btf = null;
    try {
      btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1); 
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }
    
    RID rid = new RID();
    int key =0;
    Tuple temp = null;
    
    try {
      temp = scan.getNext(rid);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    while ( temp != null) {
      tt.tupleCopy(temp);
      
      try {
	key = tt.getIntFld(1);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	btf.insert(new IntegerKey(key), rid); 
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	temp = scan.getNext(rid);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    
    // close the file scan
    scan.closescan();
    
    
    //_______________________________________________________________
    //*******************close an scan on the heapfile**************
    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    System.out.print ("After Building btree index on sailors.sid.\n\n");
    try {
      am = new IndexScan ( b_index, "sailors.in",
			   "BTreeIndex", Stypes, Ssizes, 4, 2,
			   Sprojection, null, 1, false);
    }
    
    catch (Exception e) {
      System.err.println ("*** Error creating scan for Index scan");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
   
    
    NestedLoopsJoins nlj = null;
    try {
      nlj = new NestedLoopsJoins (Stypes2, 2, Ssizes,
				  Rtypes, 3, Rsizes,
				  10,
				  am, "reserves.in",
				  outFilter, null, proj1, 2);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

     NestedLoopsJoins nlj2 = null ; 
    try {
      nlj2 = new NestedLoopsJoins (Jtypes, 2, Jsizes,
				   Btypes, 3, Bsizes,
				   10,
				   nlj, "boats.in",
				   outFilter2, null, proj2, 1);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    Sort sort_names = null;
    try {
      sort_names = new Sort (JJtype,(short)1, JJsize,
			     (iterator.Iterator) nlj2, 1, ascending, JJsize[0], 10);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    
    QueryCheck qcheck2 = new QueryCheck(2);
    
   
    t = null;
    try {
      while ((t = sort_names.get_next()) != null) {
        t.print(JJtype);
        qcheck2.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    qcheck2.report(2);

    System.out.println ("\n"); 
    try {
      sort_names.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    if (status != OK) {
      //bail out
   
      Runtime.getRuntime().exit(1);
      }
  }
  

   public void Query3() {
    System.out.print("**********************Query3 strating *********************\n"); 
    boolean status = OK;

        // Sailors, Boats, Reserves Queries.
 
    System.out.print 
      ( "Query: Find the names of sailors who have reserved a boat.\n\n"
	+ "  SELECT S.sname\n"
	+ "  FROM   Sailors S, Reserves R\n"
	+ "  WHERE  S.sid = R.sid\n\n"
	+ "(Tests FileScan, Projection, and SortMerge Join.)\n\n");
    
    CondExpr [] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
 
    Query3_CondExpr(outFilter);
 
    Tuple t = new Tuple();
    t = null;
 
    AttrType Stypes[] = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrReal)
    };
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
    };
    short  []  Rsizes = new short[1];
    Rsizes[0] =15;
 
    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3),
       new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    CondExpr[] selects = new CondExpr [1];
    selects = null;
 
    iterator.Iterator am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes,
				  (short)4, (short) 4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] Rprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3)
    }; 
 
    iterator.Iterator am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
				  (short)3, (short)3,
				  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] proj_list = {
      new FldSpec(new RelSpec(RelSpec.outer), 2)
    };

    AttrType [] jtype     = { new AttrType(AttrType.attrString) };
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4,
			 1, 4,
			 10,
			 am, am2,
			 false, false, ascending,
			 outFilter, proj_list, 1);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }
 
    QueryCheck qcheck3 = new QueryCheck(3);
 
   
    t = null;
 
    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);
        qcheck3.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace();
       Runtime.getRuntime().exit(1);
    }
 
 
    qcheck3.report(3);
 
    System.out.println ("\n"); 
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
  }

   public void Query4() {
     System.out.print("**********************Query4 strating *********************\n");
    boolean status = OK;

    // Sailors, Boats, Reserves Queries.
 
    System.out.print 
      ("Query: Find the names of sailors who have reserved a boat\n"
       + "       and print each name once.\n\n"
       + "  SELECT DISTINCT S.sname\n"
       + "  FROM   Sailors S, Reserves R\n"
       + "  WHERE  S.sid = R.sid\n\n"
       + "(Tests FileScan, Projection, Sort-Merge Join and "
       + "Duplication elimination.)\n\n");
 
    CondExpr [] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
 
    Query3_CondExpr(outFilter);
 
    Tuple t = new Tuple();
    t = null;
 
    AttrType Stypes[] = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrReal)
    };
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
    };
    short  []  Rsizes = new short[1];
    Rsizes[0] =15;
 
    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3),
       new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    CondExpr[] selects = new CondExpr [1];
    selects = null;
 
    iterator.Iterator am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes,
				  (short)4, (short) 4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] Rprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3)
    }; 
 
    iterator.Iterator am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
				  (short)3, (short)3,
				  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] proj_list = {
      new FldSpec(new RelSpec(RelSpec.outer), 2)
    };

    AttrType [] jtype     = { new AttrType(AttrType.attrString) };
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    short  []  jsizes    = new short[1];
    jsizes[0] = 30;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4,
			 1, 4,
			 10,
			 am, am2,
			 false, false, ascending,
			 outFilter, proj_list, 1);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }
    
   

    DuplElim ed = null;
    try {
      ed = new DuplElim(jtype, (short)1, jsizes, sm, 10, false);
    }
    catch (Exception e) {
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
 
    QueryCheck qcheck4 = new QueryCheck(4);

    
    t = null;
 
    try {
      while ((t = ed.get_next()) != null) {
        t.print(jtype);
        qcheck4.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace(); 
      Runtime.getRuntime().exit(1);
      }
    
    qcheck4.report(4);
    try {
      ed.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
   System.out.println ("\n");  
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
 }

   public void Query5() {
   System.out.print("**********************Query5 strating *********************\n");  
    boolean status = OK;
        // Sailors, Boats, Reserves Queries.
 
    System.out.print 
      ("Query: Find the names of old sailors or sailors with "
       + "a rating less\n       than 7, who have reserved a boat, "
       + "(perhaps to increase the\n       amount they have to "
       + "pay to make a reservation).\n\n"
       + "  SELECT S.sname, S.rating, S.age\n"
       + "  FROM   Sailors S, Reserves R\n"
       + "  WHERE  S.sid = R.sid and (S.age > 40 || S.rating < 7)\n\n"
       + "(Tests FileScan, Multiple Selection, Projection, "
       + "and Sort-Merge Join.)\n\n");

   
    CondExpr [] outFilter;
    outFilter = Query5_CondExpr();
 
    Tuple t = new Tuple();
    t = null;
 
    AttrType Stypes[] = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrReal)
    };
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
    };
    short  []  Rsizes = new short[1];
    Rsizes[0] = 15;

    FldSpec [] Sprojection = {
      new FldSpec(new RelSpec(RelSpec.outer), 1),
      new FldSpec(new RelSpec(RelSpec.outer), 2),
      new FldSpec(new RelSpec(RelSpec.outer), 3),
      new FldSpec(new RelSpec(RelSpec.outer), 4)
    };
    
    CondExpr[] selects = new CondExpr [1];
    selects[0] = null;
 
    FldSpec [] proj_list = {
      new FldSpec(new RelSpec(RelSpec.outer), 2),
      new FldSpec(new RelSpec(RelSpec.outer), 3),
      new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    FldSpec [] Rprojection = {
      new FldSpec(new RelSpec(RelSpec.outer), 1),
      new FldSpec(new RelSpec(RelSpec.outer), 2),
      new FldSpec(new RelSpec(RelSpec.outer), 3)
    };
  
    AttrType [] jtype     = { 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrReal)
    };


    iterator.Iterator am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes, 
				  (short)4, (short)4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }

    iterator.Iterator am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
			 (short)3, (short)3,
			 Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4,
			 1, 4,
			 10,
			 am, am2,
			 false, false, ascending,
			 outFilter, proj_list, 3);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }

    QueryCheck qcheck5 = new QueryCheck(5);
    //Tuple t = new Tuple();
    t = null;
 
    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);
        qcheck5.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    qcheck5.report(5);
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    System.out.println ("\n"); 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error close for sortmerge");
      Runtime.getRuntime().exit(1);
    }
 }

  public void Query6()
    {
      System.out.print("**********************Query6 strating *********************\n");
      boolean status = OK;
      // Sailors, Boats, Reserves Queries.
      System.out.print( "Query: Find the names of sailors with a rating greater than 7\n"
			+ "  who have reserved a red boat, and print them out in sorted order.\n\n"
			+ "  SELECT   S.sname\n"
			+ "  FROM     Sailors S, Boats B, Reserves R\n"
			+ "  WHERE    S.sid = R.sid AND S.rating > 7 AND R.bid = B.bid \n"
			+ "           AND B.color = 'red'\n"
			+ "  ORDER BY S.name\n\n"
			
			+ "Plan used:\n"
			+" Sort(Pi(sname) (Sigma(B.color='red')  |><|  Pi(sname, bid) (Sigma(S.rating > 7)  |><|  R)))\n\n"
			
			+ "(Tests FileScan, Multiple Selection, Projection,sort and nested-loop join.)\n\n");
      
      CondExpr [] outFilter  = new CondExpr[3];
      outFilter[0] = new CondExpr();
      outFilter[1] = new CondExpr();
      outFilter[2] = new CondExpr();
      CondExpr [] outFilter2 = new CondExpr[3];
      outFilter2[0] = new CondExpr();
      outFilter2[1] = new CondExpr();
      outFilter2[2] = new CondExpr();
      
      Query6_CondExpr(outFilter, outFilter2);
      Tuple t = new Tuple();
      t = null;
      
      AttrType [] Stypes = {
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrReal)
      };
      
      
      
      short []   Ssizes = new short[1];
      Ssizes[0] = 30;
      AttrType [] Rtypes = {
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrString), 
      };
      
      short  []  Rsizes = new short[1] ;
      Rsizes[0] = 15;
      AttrType [] Btypes = {
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrString), 
      };
      
      short  []  Bsizes = new short[2];
      Bsizes[0] =30;
      Bsizes[1] =20;
      
      
      AttrType [] Jtypes = {
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrInteger), 
      };
      
      short  []  Jsizes = new short[1];
      Jsizes[0] = 30;
      AttrType [] JJtype = {
	new AttrType(AttrType.attrString), 
      };
      
      short [] JJsize = new short[1];
      JJsize[0] = 30; 
      
      
      
      FldSpec []  proj1 = {
	new FldSpec(new RelSpec(RelSpec.outer), 2),
	new FldSpec(new RelSpec(RelSpec.innerRel), 2)
      }; // S.sname, R.bid
      
      FldSpec [] proj2  = {
	new FldSpec(new RelSpec(RelSpec.outer), 1)
      };
      
      FldSpec [] Sprojection = {
	new FldSpec(new RelSpec(RelSpec.outer), 1),
	new FldSpec(new RelSpec(RelSpec.outer), 2),
        new FldSpec(new RelSpec(RelSpec.outer), 3),
        new FldSpec(new RelSpec(RelSpec.outer), 4)
      };
      
      
      
      
      
      FileScan am = null;
      try {
	am  = new FileScan("sailors.in", Stypes, Ssizes, 
			   (short)4, (short)4,
			   Sprojection, null);
      }
      catch (Exception e) {
	status = FAIL;
	System.err.println (""+e);
	e.printStackTrace();
      }
      
      if (status != OK) {
	//bail out
	
	System.err.println ("*** Error setting up scan for sailors");
	Runtime.getRuntime().exit(1);
      }
      
  
      
      NestedLoopsJoins inl = null;
      try {
	inl = new NestedLoopsJoins (Stypes, 4, Ssizes,
				    Rtypes, 3, Rsizes,
				    10,
				  am, "reserves.in",
				    outFilter, null, proj1, 2);
      }
      catch (Exception e) {
	System.err.println ("*** Error preparing for nested_loop_join");
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
      }
     
      System.out.print( "After nested loop join S.sid|><|R.sid.\n");
	
      NestedLoopsJoins nlj = null;
      try {
	nlj = new NestedLoopsJoins (Jtypes, 2, Jsizes,
				    Btypes, 3, Bsizes,
				    10,
				    inl, "boats.in",
				    outFilter2, null, proj2, 1);
      }
      catch (Exception e) {
	System.err.println ("*** Error preparing for nested_loop_join");
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
      }
      
      System.out.print( "After nested loop join R.bid|><|B.bid AND B.color=red.\n");
      
      TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
      Sort sort_names = null;
      try {
	sort_names = new Sort (JJtype,(short)1, JJsize,
			       (iterator.Iterator) nlj, 1, ascending, JJsize[0], 10);
      }
      catch (Exception e) {
	System.err.println ("*** Error preparing for sorting");
	System.err.println (""+e);
	Runtime.getRuntime().exit(1);
      }
      
      
      System.out.print( "After sorting the output tuples.\n");
   
      
      QueryCheck qcheck6 = new QueryCheck(6);
      
      try {
	while ((t =sort_names.get_next()) !=null) {
	  t.print(JJtype);
	  qcheck6.Check(t);
	}
      }catch (Exception e) {
	System.err.println ("*** Error preparing for get_next tuple");
	System.err.println (""+e);
	Runtime.getRuntime().exit(1);
      }
      
      qcheck6.report(6);
      
      System.out.println ("\n"); 
      try {
	sort_names.close();
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      if (status != OK) {
	//bail out
	
	Runtime.getRuntime().exit(1);
      }
      
    }
  
  
  private void Disclaimer() {
    System.out.print ("\n\nAny resemblance of persons in this database to"
         + " people living or dead\nis purely coincidental. The contents of "
         + "this database do not reflect\nthe views of the University,"
         + " the Computer  Sciences Department or the\n"
         + "developers...\n\n");
  }
}

public class Lab3Test
{
  public static void main(String argv[])
  {
    boolean sortstatus;

    JoinsLab3Driver jjoin = new JoinsLab3Driver();

    sortstatus = jjoin.runTests();
    if (sortstatus != true) {
      System.out.println("Error ocurred during join tests");
    }
    else {
      System.out.println("join tests completed successfully");
    }
  }
}