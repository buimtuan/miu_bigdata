package bd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import lombok.NoArgsConstructor;


@NoArgsConstructor
@SuppressWarnings({"rawtypes", "unchecked"})
public class PairWritable<K extends WritableComparable, V extends WritableComparable>
		extends Pair<K, V>
		implements WritableComparable<PairWritable<K, V>> {

  	public PairWritable(Class<K> clazz1, Class<V> clazz2) {
 		try {
			left = clazz1.newInstance();
			right = clazz2.newInstance();
		} catch (Exception ex) {

		}
	}

	public PairWritable(K left, V right) {
		this.left = left;
		this.right = right;
   }

  	public PairWritable(Pair<K, V> value) { set(value); }

 	public void set(Pair<K, V> value) {
		left = value.getLeft();
		right = value.getRight();
	}

  	public Pair<K, V> get() {
		return Pair.of(left, right);
	}

	@Override
	public final void write(DataOutput out) throws IOException {
		left.write(out);
		right.write(out);
	}

	@Override
	public final void readFields(DataInput in) throws IOException {
		left.readFields(in);
		right.readFields(in);
	}

	@Override
	public int compareTo(PairWritable<K, V> o) {
		int k = left.compareTo(o.left);
		if (k != 0) return k;
		return right.compareTo(o.right);
	}

}
