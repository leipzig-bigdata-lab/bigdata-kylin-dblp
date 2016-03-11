package de.bigprak.transform.cogroup;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class Join<T0 extends Tuple, T1 extends Tuple> implements CoGroupFunction<T0, T1, Tuple2<Long,Long>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5479225801717280691L;

	@Override
	public void coGroup(Iterable<T0> leftElements, 
			Iterable<T1> rightElements, 
			Collector<Tuple2<Long,Long>> out)
			throws Exception {
		
		Iterator<T1> it = rightElements.iterator();
		Long id = 0L;
		if(!it.hasNext())
			id = -1L;
		else
			id = it.next().getField(0);
		
		for (Tuple leftElem : leftElements) {
			out.collect(new Tuple2<Long, Long>((Long) leftElem.getField(0), id));
		}
	}
}
