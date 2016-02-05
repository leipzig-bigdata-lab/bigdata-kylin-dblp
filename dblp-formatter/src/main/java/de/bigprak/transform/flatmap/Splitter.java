package de.bigprak.transform.flatmap;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

public final class Splitter<T extends Tuple> implements FlatMapFunction<T, T>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2867536301386194426L;
	
	private int index;
	
	public Splitter(int index) {
		super();
		this.index = index;
	}
	
	/**
	 * split data like authors or cites 
	 */
	@Override
	public void flatMap(T value, Collector<T> out) throws Exception {
		T newValue = value;
		List<String> splittedValues = Arrays.asList(value.getField(index).toString().split("[|]"));
		for(String splittedValue : splittedValues)
		{
			newValue.setField(splittedValue, index);
			out.collect(newValue);
		}
	}
}
