package de.bigprak.transform.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class IdExtractor<T extends Tuple> implements FlatMapFunction<Tuple2<Long, T>, T>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7218101125158000353L;

	/**
	 * Flink is able to generate unique ids, but it will stored as 'external' dataset
	 * so if you print the set it looks like '(0), (name, institute, ..)'
	 * to avoid this, this function extracts the id and prepends it to the original dataset
	 */
	@Override
	public void flatMap(Tuple2<Long, T> value, Collector<T> out) throws Exception {
		Long id = value.f0;
		T output = value.f1;
		output.setField(id, 0);
		out.collect(output);
	}
}
