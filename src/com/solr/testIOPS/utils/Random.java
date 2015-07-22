package com.solr.testIOPS.utils;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;


public class Random extends java.util.Random {
	private static final long serialVersionUID = 1L;

	public Random() {
	}
	
	public Random(long seed) {
		super(seed);
	}

	public <E extends Enum<E>> E choice(Class<E> clazz) {
		return choice(CollectionUtil.asArray(EnumSet.allOf(clazz)));
	}
	
	public boolean choice(boolean... array) {
		return array[nextInt(0, array.length)];
	}

	public char choice(CharSequence string) {
		return string.charAt(nextInt(0, string.length()));
	}

	public double choice(double... array) {
		return array[nextInt(0, array.length)];
	}

	public <E> E choice(E[] array) {
		return array[nextInt(0, array.length)];
	}

	public float choice(float... array) {
		return array[nextInt(0, array.length)];
	}

	public int choice(int... array) {
		return array[nextInt(0, array.length)];
	}

	public <E> E choice(List<E> list) {
		return list.get(nextInt(0, list.size()));
	}

	public long choice(long... array) {
		return array[nextInt(0, array.length)];
	}

	public short choice(short... array) {
		return array[nextInt(0, array.length)];
	}

	@Override
	protected int next(int bits) {
		return super.next(bits);
	}

	public int nextInt(int min, int max) {
		return nextInt(max - min) + min;
	}

	public boolean[] sample(int length, boolean... array) {
		boolean[] result = new boolean[length];
		for (int i = 0, len = array.length; i < length; i++) {
			result[i] = array[nextInt(0, len)];
		}
		return result;
	}

	public char[] sample(int length, char... array) {
		char[] result = new char[length];
		for (int i = 0, len = array.length; i < length; i++) {
			result[i] = array[nextInt(0, len)];
		}
		return result;
	}

	public char[] sample(int length, CharSequence value) {
		char[] result = new char[length];
		for (int i = 0, len = value.length(); i < length; i++) {
			result[i] = value.charAt(nextInt(0, len));
		}
		return result;
	}

	public double[] sample(int length, double... array) {
		double[] result = new double[length];
		for (int i = 0, len = array.length; i < length; i++) {
			result[i] = array[nextInt(0, len)];
		}
		return result;
	}

	public <E> E[] sample(int length, E[] array) {
		@SuppressWarnings("unchecked")
		E[] result = (E[]) new Object[length];
		for (int i = 0, len = array.length; i < length; i++) {
			result[i] = array[nextInt(0, len)];
		}
		return result;
	}

	public float[] sample(int length, float... array) {
		float[] result = new float[length];
		for (int i = 0, len = array.length; i < length; i++) {
			result[i] = array[nextInt(0, len)];
		}
		return result;
	}

	public int[] sample(int length, int... array) {
		int[] result = new int[length];
		for (int i = 0, len = array.length; i < length; i++) {
			result[i] = array[nextInt(0, len)];
		}
		return result;
	}

	public <E> E[] sample(int length, List<E> list) {
		@SuppressWarnings("unchecked")
		E[] result = (E[]) new Object[length];
		for (int i = 0, len = list.size(); i < length; i++) {
			result[i] = list.get(nextInt(0, len));
		}
		return result;
	}

	public long[] sample(int length, long... array) {
		long[] result = new long[length];
		for (int i = 0, len = array.length; i < length; i++) {
			result[i] = array[nextInt(0, len)];
		}
		return result;
	}

	public short[] sample(int length, short... array) {
		short[] result = new short[length];
		for (int i = 0, len = array.length; i < length; i++) {
			result[i] = array[nextInt(0, len)];
		}
		return result;
	}

	public void shuffle(boolean... array) {
		for (int i = array.length; i > 1; i--) {
			ArrayUtil.swap(array, i - 1, nextInt(i));
		}
	}

	public void shuffle(char... array) {
		for (int i = array.length; i > 1; i--) {
			ArrayUtil.swap(array, i - 1, nextInt(i));
		}
	}

	public void shuffle(double... array) {
		for (int i = array.length; i > 1; i--) {
			ArrayUtil.swap(array, i - 1, nextInt(i));
		}
	}

	public <E> void shuffle(E[] array) {
		for (int i = array.length; i > 1; i--) {
			ArrayUtil.swap(array, i - 1, nextInt(i));
		}
	}

	public void shuffle(float... array) {
		for (int i = array.length; i > 1; i--) {
			ArrayUtil.swap(array, i - 1, nextInt(i));
		}
	}

	public void shuffle(int... array) {
		for (int i = array.length; i > 1; i--) {
			ArrayUtil.swap(array, i - 1, nextInt(i));
		}
	}

	public void shuffle(List<?> list) {
		for (int i = list.size(); i > 1; i--) {
			Collections.swap(list, i - 1, nextInt(i));
		}
	}

	public void shuffle(long... array) {
		for (int i = array.length; i > 1; i--) {
			ArrayUtil.swap(array, i - 1, nextInt(i));
		}
	}

	public void shuffle(short... array) {
		for (int i = array.length; i > 1; i--) {
			ArrayUtil.swap(array, i - 1, nextInt(i));
		}
	}
}
