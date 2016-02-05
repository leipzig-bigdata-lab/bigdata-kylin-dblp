package de.bigprak.transform.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

public final class DecadeCalculator implements FlatMapFunction<Tuple5<Long, Long, Long, Long, Long>, Tuple5<Long, Long, Long, Long, Long>>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1375521501446745137L;

	/**
	 * Calculate decade, e.g. 1994 -> 90
	 */
	@Override
	public void flatMap(Tuple5<Long, Long, Long, Long, Long> value,
			Collector<Tuple5<Long, Long, Long, Long, Long>> out) throws Exception {
		Long year = value.f1;
		year = (year % 100) / 10;
		Long decade = year * 10;
		value.setField(1L, 2);
		value.setField(1L, 3);
		value.setField(decade, 4);
		out.collect(value);
	}
}
