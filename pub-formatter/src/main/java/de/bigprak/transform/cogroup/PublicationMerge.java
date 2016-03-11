package de.bigprak.transform.cogroup;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class PublicationMerge<T> implements CoGroupFunction<Tuple11<Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, String>, Tuple2<Long, T>, Tuple11<Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, String>>
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
	public void coGroup(Iterable<Tuple11<Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, String>> first,
			Iterable<Tuple2<Long, T>> second,
			Collector<Tuple11<Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, String>> out) throws Exception {

		List<Tuple2<Long, T>> list = new ArrayList<>();
		for(Tuple2<Long, T> item : second)
		{
			list.add(item);
		}
		
		for(Tuple11 result : first)
		{
			for(Tuple2<Long, T> map : list)
			{
				result.setField(map.f1, index);
				out.collect(result);
			}
		}	
	}
}
