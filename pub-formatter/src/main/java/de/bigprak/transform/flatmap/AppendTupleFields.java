package de.bigprak.transform.flatmap;

import java.lang.reflect.Constructor;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.NullFieldException;
import org.apache.flink.util.Collector;

public class AppendTupleFields<T0 extends Tuple, T1 extends Tuple> implements FlatMapFunction<T0, T1> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 34697566984061309L;

	private T1 newObject;
	
	/**
	 * 
	 * @param newObject Object to fill
	 */
	public AppendTupleFields(T1 newObject) {
		this.newObject = newObject;
	}
	
	/**
	 * append additional fields for the final tables
	 * for now it only handles String.class and Long.class
	 * @param value
	 * @param out
	 * @throws Exception
	 */
	@Override
	public void flatMap(T0 value, Collector<T1> out) throws Exception {
		
		
		//fill old data
		for(int i = 0; i < value.getArity(); i++)
		{
			newObject.setField(value.getField(i), i);
		}
		
		try {
			out.collect(newObject);
		} catch (NullFieldException e) {
			e.printStackTrace();
		}
	}
}
