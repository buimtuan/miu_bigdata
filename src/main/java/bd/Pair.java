package bd;

import java.util.Objects;

public class Pair<K, V> {

	protected K left;
	protected V right;

	public Pair() {
	}

	public Pair(K left, V right) {
		this.left = left;
		this.right = right;
	}

	public static <K, V> Pair<K, V> of (K left, V right) {
		Pair<K, V> p = new Pair<K,V>(left, right);
		return p;
	}

	@Override
	public String toString() {
		return "(" + getLeft() + ',' + getRight() + ')';
	}

	@Override
	public int hashCode() {
		return (getLeft() == null ? 0 : getLeft().hashCode()) ^
				(getRight() == null ? 0 : getRight().hashCode());
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj instanceof Pair<?, ?>) {
			final Pair<?, ?> other = (Pair<?, ?>) obj;
			return Objects.equals(getLeft(), other.getLeft())
					&& Objects.equals(getRight(), other.getRight());
		}
		return false;
	}


	public K getLeft() {
		return this.left;
	}

	public void setLeft(K left) {
		this.left = left;
	}

	public V getRight() {
		return this.right;
	}

	public void setRight(V right) {
		this.right = right;
	}


}
