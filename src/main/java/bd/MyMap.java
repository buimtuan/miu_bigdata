package bd;

import java.util.Map;
import java.util.StringJoiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MyMap extends MapWritable {

	public void put(String k, double v) {
		put(new Text(k), new DoubleWritable(v));
	}

	public static MyMap of(Map<String, Double> m) {
		MyMap map = new MyMap();
		for (String k : m.keySet()) {
			map.put(k, m.get(k));
		}
		return map;
	}

	public double get(String k) {
		Object obj = get(new Text(k));
		return ((DoubleWritable)obj).get();
	}

	public String toString() {
		StringJoiner subSj = new StringJoiner(", ");
		for (Writable k : keySet()) {
			subSj.add(k.toString() + ": " + get(k).toString());
		}
		return "{ " + subSj.toString() + " }";
	}

}
