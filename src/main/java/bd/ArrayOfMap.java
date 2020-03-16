package bd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class ArrayOfMap extends ArrayWritable {

	public ArrayOfMap() {
		super(MyMap.class);
	}

	public static ArrayOfMap of (List<Map<String, Double>> ls) {
		ArrayOfMap arr = new ArrayOfMap();
		Writable[] writableArr = new Writable[ls.size()];
		int i = 0;
		for (Map<String, Double> m : ls) {
			writableArr[i++] = MyMap.of(m);
		}
		arr.set(writableArr);
		return arr;
	}

	public List<MyMap> toList() {
		Writable[] ls = get();
		List<MyMap> mapList = new ArrayList<>();
		for (Writable w : ls) {
			mapList.add((MyMap)w);
		}
		return mapList;
	}

	public void fromList(List<MyMap> mapList) {
		Writable[] ls = new Writable[mapList.size()];
		int i = 0;
		for (MyMap m : mapList) {
			ls[i++] = m;
		}
		set(ls);
	}

	public String toString() {
		Writable[] mwArr = get();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < mwArr.length; i++) {
			MyMap mw = (MyMap)mwArr[i];
			sb.append(mw.toString());
		}
		return "( " + sb.toString() + " )";
	  }

}
