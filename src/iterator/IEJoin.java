package iterator;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrType;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

public class IEJoin extends Iterator{

	
	public IEJoin(
			AttrType    in1[],    
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
			) {
		
	}
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		return null
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		
	}

}
