package iterator;

import java.io.IOException;
import java.util.ArrayList;

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
	private ArrayList<Tuple> data, secondData, result;
	
	
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
			   int        n_out_flds, 
			   int conditions
			   ) throws IOException,NestedLoopException {
		
		//Setup for close call
		outer=am1;
		outer2=am2;
		//Setupp of the JTuple 
		
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
		
		//order to sort
		if (outFilter[0].op.attrOperator == AttrOperator.aopGT || outFilter[0].op.attrOperator == AttrOperator.aopGE) {
			TupleOrder order = new TupleOrder(TupleOrder.Ascending);
			//sort
			try {
			L1 = new Sort (in1, (short) len_in1, t1_str_sizes,
					(iterator.Iterator) am1, outFilter[0].operand1.symbol.offset, order, 0, amt_of_mem);
			}catch(SortException e) {
				System.out.println("An error occured during sort of SelfJoin Method");
			}
			
		} else{
			
			TupleOrder order = new TupleOrder(TupleOrder.Descending);
			//sort
			try {
			L1 = new Sort (in1, (short) len_in1, t1_str_sizes,
					(iterator.Iterator) am1, outFilter[0].operand1.symbol.offset, order, 0, amt_of_mem);
			}catch(SortException e) {
				System.out.println("An error occured during sort of SelfJoin Method");
			}
		}
		
		
		data = new ArrayList<Tuple>();
		Tuple tuple ;
		try {
			while ((tuple = L1.get_next()) != null)
			{	
				data.add(tuple);
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		if (conditions == 2 ) {
			try {
				// inner table sorted to get L2 iterator
				if (outFilter[1].op.attrOperator == AttrOperator.aopGT || outFilter[1].op.attrOperator == AttrOperator.aopGE) {

					TupleOrder order = new TupleOrder(TupleOrder.Descending);
					L2 = new Sort (in1, (short) len_in1, t1_str_sizes,
							(iterator.Iterator) am2, outFilter[1].operand1.symbol.offset, order, t1_str_sizes[0], amt_of_mem);
				} else if (outFilter[1].op.attrOperator == AttrOperator.aopLT || outFilter[1].op.attrOperator == AttrOperator.aopLE)
				{

					TupleOrder order = new TupleOrder(TupleOrder.Ascending);
					
					L2 = new Sort (in1, (short) len_in1, t1_str_sizes,
							(iterator.Iterator) am2, outFilter[1].operand1.symbol.offset, order, 10, amt_of_mem);

				} else {
					System.out.println("Unknown operand");
				}
			} catch (SortException | IOException e) {
				e.printStackTrace();
			}
			
			secondData = new ArrayList<Tuple>();
			try {
				while ((tuple = L2.get_next()) != null)
				{	
					secondData.add(tuple);
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			int N = 0;
			N = data.size();

			int[] P = new int[N];
			int[] B = new int[N]; // Bit array
			
			int i = 0;
			for (Tuple tupleFromSecondData : secondData) {
				int j = 0;
				for (Tuple tupleFromData: data) {
					try {
						if (areEquals(tupleFromSecondData, tupleFromData)) {
							P[i] = j;
							break;
						}
					} catch (FieldNumberOutOfBoundException | IOException e) {
						e.printStackTrace();
					}
					j++;
				}
				B[i] = 0;
				i++;
			}
		}

		
		
		
		
		//offset or not
		if (outFilter[0].op.toString() == "aopGE" || outFilter[0].op.toString() == "aopLE")
			eqOff=0;
		else
			eqOff=1;
		//initialization of the list of data, using arrayList
		
		result = new ArrayList<Tuple>();
		

		

		for (int i=0; i<data.size(); i++) {
			for (int j=0; j <= i-1+eqOff; j++) {
				try {
					Tuple t1 = data.get(i);
					Tuple t2 = data.get(j);
					Projection.Join(t1, in1, 
							t2, in1, 
							JTuple, proj_list, n_out_flds);

					Tuple jtuple = new Tuple(JTuple);
					result.add(jtuple);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}	
	}
		

		public boolean areEquals(Tuple T1, Tuple T2) throws FieldNumberOutOfBoundException, IOException {
			return (T1.getIntFld(1) == T2.getIntFld(1) &&
				T1.getIntFld(2) == T2.getIntFld(2) &&
				T1.getIntFld(3) == T2.getIntFld(3) &&
				T1.getIntFld(4) == T2.getIntFld(4));
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
			L1.close();
			L2.close();
		  outer.close();
		  outer2.close();
		}catch (Exception e) {
		  throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
		}
		closeFlag = true;
	      }
	    }
		
	

}
