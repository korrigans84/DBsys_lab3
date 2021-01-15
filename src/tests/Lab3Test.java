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
  private final boolean WRITE_TO_CSV = true;
  private int data_rows_number = 7000;
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

    
    informations();


    
    
  //Build S.in
    File2Heap(DATA_DIR_PATH+"S.txt", "S.in", data_rows_number);
    
    //Build R.in database
    File2Heap(DATA_DIR_PATH+"R.txt", "R.in", data_rows_number);
    
    //Build Q.in database
    File2Heap(DATA_DIR_PATH+"q.txt", "Q.in", data_rows_number);
   
  }
  
 
  public boolean runTests() {
   
	Query1a("query_2a.txt");
	 //Query1b("query_2b.txt");
   	
    //Query2a("query_2a.txt");
    //Query2b("query_2b.txt");
    //Query2c("query_2b.txt");
    
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
  private void CondExpr_Query1b(CondExpr[] outFilter, int operator, int outerSymbol, int innerSymbol, int operator2, int outer2Symbol, int inner2Symbol  ) {

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
  /**
   * This method test Simple Join using Nested Loops Joins iterator
   * The result of the query are stored in output_query1a.txt file.
   * The execution time is stored in csv
   * @param queryFile
   */
  private void Query1a(String queryFile) {
	  System.out.print("**********************Query1a is starting *********************\n\n");
	    boolean status = OK;
	    
	    
	    CondExpr [] outFilter  = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		
		QueryFromFile query = new QueryFromFile(DATA_DIR_PATH+queryFile);
		query.print();
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
		long start_time = System.currentTimeMillis();
		try {
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
			
			pw = new PrintWriter(DATA_DIR_PATH+"output/output_query1a.txt");
			
			while ((t = nlj.get_next()) != null) {
				i++;
				//t.print(jtype); // print results
				pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // get tuples in .txt file
			}
			pw.close();
			// print the total number of returned tuples
			long query_time = System.currentTimeMillis() - start_time;
			if(WRITE_TO_CSV)
				write_time_to_csv(query_time, "query_1a.csv");
			System.out.println("Duration of the query : "+ query_time + " ms");
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
	  System.out.print("**********************Query1a finished successfully *********************\n\n");

  }

  /**
   * This method test Double predicates Join using Nested Loops Joins iterator
   * The result of the query are stored in output_query1b.txt file.
   * The execution time is stored in csv
   * @param queryFile
   */
  private void Query1b(String queryFile) {

	  
	  System.out.print("**********************Query1b is starting *********************\n");
	    boolean status = OK;
	    
	    
	    CondExpr [] outFilter  = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();
		
		QueryFromFile query = new QueryFromFile(DATA_DIR_PATH+queryFile);
		query.print();

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
		long start_time = System.currentTimeMillis();
		try {
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
			
			pw = new PrintWriter(DATA_DIR_PATH+"output/output_query1b.txt");
			
			while ((t = nlj.get_next()) != null) {
				i++;
				//t.print(jtype); // print results
				pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // get tuples in .txt file
			}
			pw.close();
			// print the total number of returned tuples
			long query_time = System.currentTimeMillis() - start_time;
			if(WRITE_TO_CSV)
				write_time_to_csv(query_time, "query_1b.csv");
			System.out.println("Duration of the query : "+ query_time + " ms");
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
		  System.out.print("**********************Query1b finished successfully *********************\n\n");

  }

  /**
   * This method test IESelfJoinSinglePredicate iterator
   * The result of the query are stored in output_query2a.txt file.
   * The execution time is stored in csv
   * @param queryFile
   */
  private void Query2a(String queryFile) {
	  System.out.print("**********************Query2a is starting *********************\n");
	    boolean status = OK;

	    CondExpr [] outFilter  = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		
		QueryFromFile query = new QueryFromFile(DATA_DIR_PATH+queryFile);
		query.print();

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
		long start_time = System.currentTimeMillis();

		try {
			am = new FileScan(query.rel1+".in", 
					Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		// IESelfJoin
		SelfJoinSinglePredicate sjsp = null;
		try {
			sjsp = new SelfJoinSinglePredicate (Stypes, 4, Ssizes,
					Rtypes, 4, Rsizes,
					10,
					am,
					query.rel2+".in",
					outFilter, null, Projection, 2
					);
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
			
			pw = new PrintWriter(DATA_DIR_PATH+"output/output_query2a.txt");
			
			while ((t = sjsp.get_next()) != null) {
				i++;
				//t.print(jtype); // print results
				pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // get tuples in .txt file
			}
			pw.close();
			// print the total number of returned tuples
			long query_time = System.currentTimeMillis() - start_time;
			if(WRITE_TO_CSV)
				write_time_to_csv(query_time, "query_2a.csv");
			System.out.println("Output Tuples for query 2a: " + i);
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try {
			sjsp.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		if (status != OK) {
			//bail out
			Runtime.getRuntime().exit(1);
		}
		  System.out.print("**********************Query2a finished successfully *********************\n\n");

  }
  
  
  /**
   * This method test IESelfJoin iterator 
   * (two predicates inequality join)
   * The result of the query are stored in output_query2a.txt file.
   * The execution time is stored in csv
   * @param queryFile
   */
  private void Query2b(String queryFile) {
	  System.out.print("**********************Query2b is starting *********************\n");
	    boolean status = OK;
	    
	    
	    CondExpr [] outFilter  = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();
		
		QueryFromFile query = new QueryFromFile(DATA_DIR_PATH+queryFile);
		query.print();

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
		iterator.Iterator am2 = null;
		long start_time = System.currentTimeMillis();
		
		try {
			am = new FileScan(query.rel1+".in", 
					Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
			am2 = new FileScan(query.rel1+".in", 
					Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		long time_start = System.currentTimeMillis();
		SelfJoin sj = null;
		try {
			sj = new SelfJoin (Stypes, 4, Ssizes,
					Rtypes, 4, Rsizes,
					10,
					am,
					am2,
					query.rel2+".in",
					outFilter, null, Projection, 2);
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for Self Join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		t = null;
		int i = 0;
		PrintWriter pw;
		try {
			
			pw = new PrintWriter(DATA_DIR_PATH+"output/output_query2b.txt");
			
			while ((t = sj.get_next()) != null) {
				i++;
				//t.print(jtype); // print results
				pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // get tuples in .txt file
				}
			pw.close();
			// print the total number of returned tuples
			
			
			long query_time = System.currentTimeMillis() - start_time;
			if(WRITE_TO_CSV)
				write_time_to_csv(query_time, "query_2b.csv");
			System.out.println("Duration of the query : "+ query_time + " ms");
			System.out.println("Output Tuples for query 2b: " + i);
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try {
			sj.close();
			
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		if (status != OK) {
			//bail out
			Runtime.getRuntime().exit(1);
		}
		System.out.print("**********************Query2b finished successfully *********************\n\n");

  }
  
  /**
   * This method test the IEJoin implemeentation
   * @param queryFile
   */
  private void Query2c(String queryFile) {

	  System.out.print("**********************Query2c is starting *********************\n");
	    boolean status = OK;
	    
	    
	    
	    CondExpr [] outFilter  = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();
		
		QueryFromFile query = new QueryFromFile(DATA_DIR_PATH+queryFile);
		query.print();

		
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
		
		clear_csv();
	    
		iterator.Iterator am = null;
		iterator.Iterator am_copy = null;
		iterator.Iterator am2 = null;
		iterator.Iterator am2_copy = null;

		long start_time = System.currentTimeMillis();
		
		try {
			am = new FileScan(query.rel1+".in", 
					Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
			am_copy = new FileScan(query.rel1+".in", 
					Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
			am2 = new FileScan(query.rel2+".in", 
					Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
			am2_copy = new FileScan(query.rel2+".in", 
					Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		long timeStart = System.currentTimeMillis();
		IEJoin iej = null;
		try {
			iej = new IEJoin (Stypes, 4, Ssizes,
					Rtypes, 4, Rsizes,
					10,
					am,
					am_copy,
					am2,
					am2_copy,
					query.rel2+".in",
					outFilter, Projection, 2);
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for Self Join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		t = null;
		int i = 0;
		PrintWriter pw;
		try {
			
			pw = new PrintWriter(DATA_DIR_PATH+"output/output_query2c.txt");
			
			while ((t = iej.get_next()) != null) {
				i++;
				//t.print(jtype); // print results
				pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // get tuples in .txt file
			}
			pw.close();
			// print the total number of returned tuples
			long query_time = System.currentTimeMillis() - start_time;
			if(WRITE_TO_CSV)
				write_time_to_csv(query_time, "query_2c.csv");
			System.out.println("Output Tuples for query 2c: " + i);
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try {
			iej.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		if (status != OK) {
			//bail out
			Runtime.getRuntime().exit(1);
		}
		  System.out.print("**********************Query2c finished successfully *********************\n\n");

  }
  
  /**
   * Delete csv files if necessary
   */
  private void clear_csv() {
	  File csv = new File(DATA_DIR_PATH+"time.csv");
	  csv.delete();
  }
  
  /**
   * Write execution time and the number of data rows
   * in csv
   * @param time
   * @param file
   */
  private void write_time_to_csv( long time, String file) 
  {
	  try {
		FileWriter csv = new FileWriter(DATA_DIR_PATH+"csv/"+file, true);
		
		csv.write(data_rows_number + "," + time + "\n");
		csv.close();
	} catch (IOException e) {
		System.out.println("Enable to write result time in file "+ file);
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
  
  private void informations() {
	    System.out.println("********************************************************************\n"
	    		+"				Lab3Test                          \n"
	    		+ "********************************************************************\n");
	    
	    System.out.println("Informations : ");
	    System.out.println("Data stored in the dir : "+DATA_DIR_PATH);
	    System.out.println("Number of data rows : "+data_rows_number);
	    System.out.println("You can play with the number of rows by changing the value of data_rows_number variable\n\n");
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