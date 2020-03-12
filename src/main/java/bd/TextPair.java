package bd;

import org.apache.hadoop.io.Text;

class TextPair extends PairWritable<Text, Text> {

	public static TextPair of(Text left, Text right) {
		return new TextPair(left, right);
	}

	public static TextPair of(Pair<String, String> p) {
		return new TextPair(new Text(p.getLeft()), new Text(p.getRight()));
	}

	TextPair(Text left, Text right) {
		super();
		this.left = left;
		this.right = right;
	}

	TextPair() {
		super(Text.class, Text.class);
	}

}
