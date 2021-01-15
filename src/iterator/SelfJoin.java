package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

public class SelfJoin extends Iterator{
	
	
	private Tuple[] results;
	private Iterator outer, outer2;
	private TupleOrder order;
	private Tuple JTuple;
	private int eqOff;
	private Sort L1;
	private Sort L2;
	private ArrayList<Tuple> L1_array, L2_array, result;
	private int pointer = 0;
	
	
	  /**constructor
	   *Initialize the two relations which are joined, including relation type,
	   *@param in1  Array containing field types of R.
	   *@param len_in1  # of columns in R.
	   *@param t1_str_sizes shows the length of the string fields.
	   *@param in2  Array containing field types of S
	   *@param len_in2  # of columns in S
	   *@param  t2_str_sizes shows the length of the string fields.
	   *@param amt_of_mem  IN PAGES
	   *@param am1  access method for left i/p to join
	   *@param relationName  access hfapfile for right i/p to join
	   *@param outFilter   select expressions
	   *@param rightFilter reference to filter applied on right i/p
	   *@param proj_list shows what input fields go where in the output tuple
	   *@param n_out_flds number of outer relation fileds
	   *@exception IOException some I/O fault
	   *@exception NestedLoopException exception from this class
	 * @throws TupleUtilsException 
	 * @throws UnknowAttrType 
	   */
	public SelfJoin( AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],         
			   int     len_in2,           
			   short   t2_str_sizes[],   
			   int     amt_of_mem,        
			   Iterator     am1,
			   Iterator		am2,
			   String relationName,      
			   CondExpr outFilter[],      
			   CondExpr rightFilter[],    
			   FldSpec   proj_list[],
			   int        n_out_flds
			   ) throws IOException,NestedLoopException, UnknowAttrType, TupleUtilsException {
		//Setup for close call
		outer=am1;
		outer2=am2;
		
		

		/***************************************************************************************
		 * 
		 *								Start of the IESelfJoin algorithm	
		 *
		 ***************************************************************************************/
		

		/***************************************************************************************
	 							if (op 1 ∈ {>, ≥}) sort L 1 in ascending order		
		 ***************************************************************************************/
		
		TupleOrder order;
		TupleOrder order2 ;

		if (outFilter[0].op.attrOperator == AttrOperator.aopGT || outFilter[0].op.attrOperator == AttrOperator.aopGE) {
			order = new TupleOrder(TupleOrder.Ascending);
			//sort
			try {
			L1 = new Sort (in1, (short) len_in1, t1_str_sizes,
					(iterator.Iterator) am1, outFilter[0].operand1.symbol.offset, order, 0, amt_of_mem);
			}catch(SortException e) {
				System.out.println("An error occured during sort of SelfJoin Method");
			}
			

			/***************************************************************************************
		 							else if (op 1 ∈ {<, ≤}) sort L 1 in descending order	
			 ***************************************************************************************/
		} else{
			
			order = new TupleOrder(TupleOrder.Descending);
			//sort
			try {
			L1 = new Sort (in1, (short) len_in1, t1_str_sizes,
					(iterator.Iterator) am1, outFilter[0].operand1.symbol.offset, order, 0, amt_of_mem);
			}catch(SortException e) {
				System.out.println("An error occured during sort of SelfJoin Method");
			}
		}
		
		
		//we put L1 in a array to access Tuples more than once
		L1_array = new ArrayList<Tuple>();
		Tuple tuple ;
		try {
			while ((tuple = L1.get_next()) != null)
			{	
				L1_array.add(new Tuple(tuple));
			}
			L1.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
			/***************************************************************************************
							if (op 2 ∈ {>, ≥}) sort L 2 in descending order
			 ***************************************************************************************/
			if (outFilter[1].op.attrOperator == AttrOperator.aopGT || outFilter[1].op.attrOperator == AttrOperator.aopGE) {

				order2 = new TupleOrder(TupleOrder.Descending);
				try {
					L2 = new Sort (in1, (short) len_in1, t1_str_sizes,
							(iterator.Iterator) am2, outFilter[1].operand1.symbol.offset, order2, 0, amt_of_mem);
				} catch (SortException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} 
			

			/***************************************************************************************
			 * 				else if (op 2 ∈ {<, ≤}) sort L 2 in ascending order
			 ***************************************************************************************/
			else
			{

				order2 = new TupleOrder(TupleOrder.Ascending);
				
				try {
					L2 = new Sort (in1, (short) len_in1, t1_str_sizes,
							(iterator.Iterator) am2, outFilter[1].operand1.symbol.offset, order2, 0, amt_of_mem);
				} catch (SortException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} 
			
		
		
		
		//We use an array
		L2_array = new ArrayList<Tuple>();
		try {
			while ((tuple = L2.get_next()) != null)
			{	
				L2_array.add(new Tuple(tuple));
			}
			L2.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		

		
		//initialization of the list of data, using arrayList
		
		int N = L1_array.size();
		

		/***************************************************************************************
		 * 						compute the permutation array P of L 2 w.r.t. L 1
		 ***************************************************************************************/
		//compute of permutation
		/*
		int[] P = new int[N];

		for(int i=0; i<N; i++) {
			for(int j=0; j<N; j++) {
				if(TupleUtils.Equal(L1_array.get(i), L2_array.get(j), in1, len_in1)) {
					P[i] = j;
				}
			}
		}*/
		
		//Better method
		
		int[] P = new int[N];
		if(order.tupleOrder == order2.tupleOrder) {
			for(int i = 0; i<N; i++)
				P[i]=i;
		}else {
			for(int i = 0; i<N; i++)
				P[i]=N-i-1;
		}
		
			
		
		

		/***************************************************************************************
		 *								Declaration of Objects required &&
		 *						initialize bit-array B (|B| = n), and set all bits to 0
		 *						initialize join result as an empty list for tuple pairs
		 ***************************************************************************************/
		int[] B = new int[N]; // Bit array
		Arrays.fill(B, 0);
		try {
			JTuple = new Tuple();
			AttrType[] Jtypes = new AttrType[n_out_flds];
			TupleUtils.setup_op_tuple(JTuple, Jtypes,
					in1, len_in1, in1, len_in1,
					t1_str_sizes, t1_str_sizes,
					proj_list, n_out_flds);
			
			
		}catch (TupleUtilsException e){
			throw new NestedLoopException(e, e.getMessage());
		}

		result = new ArrayList<Tuple>();
		
		/***************************************************************************************	
		 * 					if (op 1 ∈ {≤, ≥} and op 2 ∈ {≤, ≥}) eqOff = 0
							else eqOff = 1
		 ***************************************************************************************/
		if (
				(outFilter[0].op.attrOperator == AttrOperator.aopGE || outFilter[0].op.attrOperator == AttrOperator.aopLE) 
				&&
				(outFilter[1].op.attrOperator == AttrOperator.aopGE || outFilter[1].op.attrOperator == AttrOperator.aopLE)
				)
			eqOff=0;
		else
			eqOff=1;
		
		/***************************************************************************************	
		 * 					loop for of the algorithm
		 ***************************************************************************************/
		try {
			for(int i=0; i<N; i++) {
				int pos = P[i];
				B[pos]=1;
				for(int j= pos+eqOff; j<N; j++) {
					if(B[j] == 1) {
						Projection.Join(L1_array.get(j), in1, 
										L1_array.get(P[i]), in1, 
										JTuple, proj_list, n_out_flds);
							
						result.add(new Tuple(JTuple));
					}
				}
			}
		} catch (UnknowAttrType | FieldNumberOutOfBoundException | IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		}
		
		pointer = result.size()-1;
	}
	@Override
	
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

		if(result.isEmpty())
			return null;
		Tuple next = result.remove(pointer);
		pointer--;
		return next;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
	      if (!closeFlag) {
		
		try {
			L1_array.clear();
			L2_array.clear();
		  outer.close();
		  outer2.close();
		}catch (Exception e) {
		  throw new JoinsException(e, "SelfJoin.java: error in closing iterator.");
		}
		closeFlag = true;
	      }
	    }
		
	

}
