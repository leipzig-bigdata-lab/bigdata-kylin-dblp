package de.bigprak.transform.cogroup;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

public final class PublicationMerge implements CoGroupFunction<Tuple9<Long, Long, Long, Long, Long, Long, Long, Long, Long>, Tuple2<Long, Long>, Tuple9<Long, Long, Long, Long, Long, Long, Long, Long, Long>>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3869927264362843210L;
	
	private int index;
	
	public PublicationMerge(int index) {
		this.index = index;
	}
	
	@Override
	public void coGroup(Iterable<Tuple9<Long, Long, Long, Long, Long, Long, Long, Long, Long>> first,
			Iterable<Tuple2<Long, Long>> second,
			Collector<Tuple9<Long, Long, Long, Long, Long, Long, Long, Long, Long>> out) throws Exception {
		Tuple9<Long, Long, Long, Long, Long, Long, Long, Long, Long> result = first.iterator().next();
		Tuple2<Long, Long> map = new Tuple2<>();
		try {
			map = second.iterator().next();
		} catch(Exception e) {
			
		}
		result.setField(map.f1, index);
		if(result.getField(index) == null)
			return;
		out.collect(result);
	}
}
