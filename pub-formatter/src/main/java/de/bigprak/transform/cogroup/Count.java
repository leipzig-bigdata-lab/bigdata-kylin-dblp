package de.bigprak.transform.cogroup;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class Count<T0 extends Tuple, T1 extends Tuple> implements CoGroupFunction<T0, T1, Tuple2<Long,Long>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5479225801717280691L;

	@Override
	public void coGroup(Iterable<T0> leftElements, 
			Iterable<T1> rightElements, 
			Collector<Tuple2<Long,Long>> out)
			throws Exception {
		
		Iterator<T0> itRight = leftElements.iterator();
		Iterator<T1> itLeft = rightElements.iterator();

		Long id = 0L;
		int count = 0;
		
		if(!itRight.hasNext())
			return;
		else
			id = itRight.next().getField(0);
		
		while (itLeft.hasNext()) {
			itLeft.next();
			count++;
		}
		
		out.collect(new Tuple2<Long, Long>(id, new Long(count)));
		
		
	}
}
