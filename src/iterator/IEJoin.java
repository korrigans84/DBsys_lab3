package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

public class IEJoin extends Iterator{

	private Iterator outer, outer2, outer3, outer4;
	private Sort L1, L1_prim, L2, L2_prim;
	private ArrayList<Tuple> L1_array, L2_array, L1_prim_array, L2_prim_array, result;
	private Tuple JTuple;
	

	
	public IEJoin(
			AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],         
			   int     len_in2,           
			   short   t2_str_sizes[],   
			   int     amt_of_mem,        
			   Iterator     am1,
			   Iterator 	am1_copy,
			   Iterator		am2,
			   Iterator 	am2_copy,
			   String relationName,      
			   CondExpr outFilter[],      
			   FldSpec   proj_list[],
			   int        n_out_flds
			) throws Exception {
		
		/*
		 * Init of the Jtuple for the result
		 */
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
		 * 
		 *								Start of the IESelfJoin algorithm	
		 *
		 ***************************************************************************************/
		

		/***************************************************************************************
		 * if (op 1 ∈ {>, ≥}) sort L 1 , L 0 1 in descending order		
		 ***************************************************************************************/
		
		TupleOrder order;
		if (outFilter[0].op.attrOperator == AttrOperator.aopGT || outFilter[0].op.attrOperator == AttrOperator.aopGE) {
			order = new TupleOrder(TupleOrder.Ascending);
			/***************************************************************************************
		 							else if (op 1 ∈ {<, ≤}) sort L1, L'1 in descending order	
			 ***************************************************************************************/
		} else{
			order = new TupleOrder(TupleOrder.Descending);
		}

		try {
		L1 = new Sort (in1, (short) len_in1, t1_str_sizes,
				(iterator.Iterator) am1, outFilter[0].operand1.symbol.offset, order, 0, amt_of_mem);
		L1_prim = new Sort (in1, (short) len_in1, t1_str_sizes,
				(iterator.Iterator) am2, outFilter[0].operand1.symbol.offset, order, 0, amt_of_mem);
		}catch(SortException e) {
			System.out.println("An error occured during sort of SelfJoin Method");
		}

		/***************************************************************************************
		 * if (op 2 ∈ {>, ≥}) sort L 2 , L'2 in descending order		
		 ***************************************************************************************/
		TupleOrder order2;
		if (outFilter[1].op.attrOperator == AttrOperator.aopGT || outFilter[1].op.attrOperator == AttrOperator.aopGE) {
			order2 = new TupleOrder(TupleOrder.Ascending);	

			/***************************************************************************************
		 							else if (op 2 ∈ {<, ≤}) sort L2, L'2 in descending order	
			 ***************************************************************************************/
		} else{			
			order2 = new TupleOrder(TupleOrder.Descending);
		}
		
		try {
			L2 = new Sort (in1, (short) len_in1, t1_str_sizes,
					(iterator.Iterator) am1_copy, outFilter[0].operand1.symbol.offset, order2, 0, amt_of_mem);
			L2_prim = new Sort (in1, (short) len_in1, t1_str_sizes,
					(iterator.Iterator) am2_copy, outFilter[0].operand1.symbol.offset, order2, 0, amt_of_mem);
			}catch(SortException e) {
				System.out.println("An error occured during sort of SelfJoin Method");
			}
		/***************************************************************************************
	 							Conversion of Iterators to arrays
		 ***************************************************************************************/
		Tuple tuple ;
		
		L1_array = new ArrayList<Tuple>();
			while ((tuple = L1.get_next()) != null)
			{	
				L1_array.add(new Tuple(tuple));
			}
			L1.close();
			
		L1_prim_array = new ArrayList<Tuple>();
			while ((tuple = L1_prim.get_next()) != null)
			{	
				L1_prim_array.add(new Tuple(tuple));
			}
			L1_prim.close();
			
		L2_array = new ArrayList<Tuple>();
			while ((tuple = L2.get_next()) != null)
			{	
				L2_array.add(new Tuple(tuple));
			}
			L2.close();
				
		L2_prim_array = new ArrayList<Tuple>();
		while ((tuple = L2_prim.get_next()) != null)
		{	
			L2_prim_array.add(new Tuple(tuple));
		}
		L2_prim.close();
		

		int M = L1_array.size();
		int N = L1_prim_array.size();
		/***************************************************************************************
	 							compute the permutation array P of L 2 w.r.t. L 1
		 ***************************************************************************************/
		int[] P = new int[M];
		for(int i=0; i<M; i++) {
			for(int j=0; j<M; j++) {
				if(TupleUtils.Equal(L1_array.get(i), L2_array.get(j), in1, len_in1)) {
					P[i] = j;
					break;
				}
			}
		}
		
		/***************************************************************************************
			compute the permutation array P 0 of L 0 2 w.r.t. L 0 1		 
		***************************************************************************************/
		int[] P_prim = new int[N];
			for(int i=0; i<N; i++) {
				for(int j=0; j<N; j++) {
					if(TupleUtils.Equal(L1_prim_array.get(i), L2_prim_array.get(j), in1, len_in1)) {
						P_prim[j] = i;
						break;
				}
			}
		}

		/***************************************************************************************
			compute the offset array O 1 of L 1 w.r.t. L 0 1		
		***************************************************************************************/
			int[] O_1 = new int[M];
			
			for(int i=0; i<M; i++) {
				for(int j=0; j<N; j++) {
					if(TupleUtils.CompareTupleWithTuple(
							new AttrType(AttrType.attrInteger), 
							L1_array.get(i), proj_list[0].offset, 
							L1_prim_array.get(j), proj_list[1].offset)>=0) {
						O_1[i] = j;
						break;
					}
				}
			}

		/***************************************************************************************
			compute the offset array O 2 of L 2 w.r.t. L 0 2		 
		***************************************************************************************/
			int[] O_2 = new int[M];
			for(int i=0; i<M; i++) {
				for(int j=0; j<N; j++) {
					if(TupleUtils.CompareTupleWithTuple(
							new AttrType(AttrType.attrInteger),
							L2_array.get(i), proj_list[0].offset, 
							L2_prim_array.get(j), proj_list[1].offset)>=0) {
						O_2[i] = j;
						break;
					}
				}
			}

			/***************************************************************************************
			*	initialize bit-array B 0 (|B 0 | = n), and set all bits to 0		 
			***************************************************************************************/
			int[] B_prim = new int[N];
			Arrays.fill(B_prim, 0);
			
			
			/***************************************************************************************	
			 * 					if (op 1 ∈ {≤, ≥} and op 2 ∈ {≤, ≥}) eqOff = 0
			 *					else eqOff = 1
			 ***************************************************************************************/
			int eqOff;
			if (
					(outFilter[0].op.attrOperator == AttrOperator.aopGE || outFilter[0].op.attrOperator == AttrOperator.aopLE) 
					&&
					(outFilter[1].op.attrOperator == AttrOperator.aopGE || outFilter[1].op.attrOperator == AttrOperator.aopLE)
					)
				eqOff=0;
			else
				eqOff=1;
			
			/***************************************************************************************	
			 *				LOOP FOR OF THE ALGORITHM
			 ***************************************************************************************/
			
			for(int i=0; i<M; i++) {
				int off2 = O_2[i];
				for(int j=0; j< Math.min(off2, L2_array.size()); j++) {
					B_prim[P_prim[j]]=1;
				}
				int off1 = O_1[P[i]];
				for(int k = off1+eqOff; k<N; k++ ) {
					if(B_prim[k]==1){
						Projection.Join(L2_array.get(i), in1, 
								L1_prim_array.get(k), in1, 
								JTuple, proj_list, n_out_flds);
						result.add(new Tuple(JTuple));
					}
				}
			}
		
	}
	
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		if(result.isEmpty())
			return null;
		Tuple next = result.get(0);
		result.remove(0);
		return next;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
	    if (!closeFlag) {
			
			try {
			}catch (Exception e) {
			  throw new JoinsException(e, "SelfJoin.java: error in closing iterator.");
			}
			closeFlag = true;
	      }		
	}

}
