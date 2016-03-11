package de.bigprak.transform.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

public final class KeyCleaner<T extends Tuple> implements FlatMapFunction<T, T>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 310145510485555611L;

	private int index;
	
	public KeyCleaner(int index) {
		this.index = index;
	}
	
	/**
	 * remove ':ref' from specific string, so flink can join on the string
	 */
	@Override
	public void flatMap(T value, Collector<T> out)
			throws Exception {
		//remove "ref:"
		String cleanedString;
		if(value.getField(index).toString().contains("ref:"))
		{
			cleanedString = value.getField(index).toString().substring(4);
			value.setField(cleanedString, index);
		}
			
		out.collect(value);
	}
	
}
