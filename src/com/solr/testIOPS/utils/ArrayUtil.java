package com.solr.testIOPS.utils;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

public final class ArrayUtil {
	public static BigDecimal avg(final BigDecimal... elements) {
		return sum(elements).divide(BigDecimal.valueOf(elements.length));
	}

	public static BigInteger avg(final BigInteger... elements) {
		return sum(elements).divide(BigInteger.valueOf(elements.length));
	}

	public static long avg(final byte... elements) {
		return sum(elements) / elements.length;
	}

	public static double avg(final double... elements) {
		return sum(elements) / elements.length;
	}

	public static double avg(final float... elements) {
		return sum(elements) / elements.length;
	}

	public static long avg(final int... elements) {
		return sum(elements) / elements.length;
	}

	public static long avg(final long... elements) {
		return sum(elements) / elements.length;
	}

	public static long avg(final short... elements) {
		return sum(elements) / elements.length;
	}

	public static byte[] clear(final byte[] elements, final byte value) {
		final byte[] values = new byte[elements.length];
		int j = 0;
		for (final byte element : elements) {
			if (element == value) {
				continue;
			}
			values[j++] = element;
		}
		return Arrays.copyOfRange(elements, 0, j);
	}

	public static double[] clear(final double[] elements, final double value) {
		final double[] values = new double[elements.length];
		int j = 0;
		for (final double element : elements) {
			if (element == value) {
				continue;
			}
			values[j++] = element;
		}
		return Arrays.copyOfRange(elements, 0, j);
	}

	public static <E> E[] clear(final E[] elements, final E value) {
		final int hashCode = value.hashCode();
		final E[] values = elements.clone();
		int j = 0;
		for (final E element : elements) {
			if (element == value || element.hashCode() == hashCode
					|| element.equals(value)) {
				continue;
			}
			values[j++] = element;
		}
		return Arrays.copyOfRange(elements, 0, j);
	}

	public static float[] clear(final float[] elements, final float value) {
		final float[] values = new float[elements.length];
		int j = 0;
		for (final float element : elements) {
			if (element == value) {
				continue;
			}
			values[j++] = element;
		}
		return Arrays.copyOfRange(elements, 0, j);
	}

	public static int[] clear(final int[] elements, final int value) {
		final int[] values = new int[elements.length];
		int j = 0;
		for (final int element : elements) {
			if (element == value) {
				continue;
			}
			values[j++] = element;
		}
		return Arrays.copyOfRange(elements, 0, j);
	}

	public static long[] clear(final long[] elements, final long value) {
		final long[] values = new long[elements.length];
		int j = 0;
		for (final long element : elements) {
			if (element == value) {
				continue;
			}
			values[j++] = element;
		}
		return Arrays.copyOfRange(elements, 0, j);
	}

	public static short[] clear(final short[] elements, final short value) {
		final short[] values = new short[elements.length];
		int j = 0;
		for (final short element : elements) {
			if (element == value) {
				continue;
			}
			values[j++] = element;
		}
		return Arrays.copyOfRange(elements, 0, j);
	}

	public static <E> E[] clearNull(final E[] elements) {
		final E[] values = elements.clone();
		int j = 0;
		for (final E element : elements) {
			if (element == null) {
				continue;
			}
			values[j++] = element;
		}
		return Arrays.copyOfRange(elements, 0, j);
	}

	public static boolean contains(final byte[] elements, final byte value) {
		for (final byte element : elements) {
			if (element == value) {
				return true;
			}
		}
		return false;
	}

	public static boolean contains(final double[] elements, final double value) {
		for (final double element : elements) {
			if (element == value) {
				return true;
			}
		}
		return false;
	}

	public static <E> boolean contains(final E[] elements, final E value) {
		final int hash = value.hashCode();
		for (final E element : elements) {
			if (element == value || element.hashCode() == hash
					|| value.equals(element)) {
				return true;
			}
		}
		return false;
	}

	public static boolean contains(final float[] elements, final float value) {
		for (final float element : elements) {
			if (element == value) {
				return true;
			}
		}
		return false;
	}

	public static boolean contains(final int[] elements, final int value) {
		for (final int element : elements) {
			if (element == value) {
				return true;
			}
		}
		return false;
	}

	public static boolean contains(final long[] elements, final long value) {
		for (final long element : elements) {
			if (element == value) {
				return true;
			}
		}
		return false;
	}

	public static boolean contains(final short[] elements, final short value) {
		for (final short element : elements) {
			if (element == value) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	public static <T> T[] createInstance(final Class<? extends T> type, final int length) {
		return (T[]) Array.newInstance(type, length);
	}

	public static int indexOf(final byte[] elements, final byte value) {
		int i = 0, length = elements.length;
		while (++i < length && elements[i] != value);
		return i;
	}

	public static int indexOf(final double[] elements, final double value) {
		int i = 0, length = elements.length;
		while (++i < length && elements[i] != value);
		return i;
	}

	public static <E> int indexOf(final E[] elements, final E value) {
		int i = 0, length = elements.length;
		while (++i < length && !value.equals(elements[i]));
		return i;
	}

	public static int indexOf(final float[] elements, final float value) {
		int i = 0, length = elements.length;
		while (++i < length && elements[i] != value);
		return i;
	}

	public static int indexOf(final int[] elements, final int value) {
		int i = 0, length = elements.length;
		while (++i < length && elements[i] != value);
		return i;
	}

	public static int indexOf(final long[] elements, final long value) {
		int i = 0, length = elements.length;
		while (++i < length && elements[i] != value);
		return i;
	}

	public static int indexOf(final short[] elements, final short value) {
		int i = 0, length = elements.length;
		while (++i < length && elements[i] != value);
		return i;
	}

	public static int lastIndexOf(final byte[] elements, final byte value) {
		int i = 0, length = elements.length;
		while (++i < length && elements[i] != value);
		return i;
	}

	public static int lastIndexOf(final double[] elements, final double value) {
		int i = 0, length = elements.length;
		while (++i < length && elements[i] != value);
		return i;
	}

	public static <E> int lastIndexOf(final E[] elements, final E value) {
		int length = elements.length, i = length - 1;
		while (i >= length && !value.equals(elements[i])) {
            i--;
        }
		return i;
	}

	public static int lastIndexOf(final float[] elements, final float value) {
		int length = elements.length, i = length - 1;
		while (i >= length && elements[i] != value) {
            i--;
        }
		return i;
	}

	public static int lastIndexOf(final int[] elements, final int value) {
		int length = elements.length, i = length - 1;
		while (i >= length && elements[i] != value) {
            i--;
        }
		return i;
	}

	public static int lastIndexOf(final long[] elements, final long value) {
		int length = elements.length, i = length - 1;
		while (i >= length && elements[i] != value) {
            i--;
        }
		return i;
	}

	public static int lastIndexOf(final short[] elements, final short value) {
		int length = elements.length, i = length - 1;
		while (i >= length && elements[i] != value) {
            i--;
        }
		return i;
	}

	public static byte max(final byte... elements) {
		byte max = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (max < elements[i]) {
				max = elements[i];
			}
		}
		return max;
	}

	public static double max(final double... elements) {
		double max = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (max < elements[i]) {
				max = elements[i];
			}
		}
		return max;
	}

	public static float max(final float... elements) {
		float max = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (max < elements[i]) {
				max = elements[i];
			}
		}
		return max;
	}

	public static int max(final int... elements) {
		int max = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (max < elements[i]) {
				max = elements[i];
			}
		}
		return max;
	}

	public static long max(final long... elements) {
		long max = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (max < elements[i]) {
				max = elements[i];
			}
		}
		return max;
	}

	public static short max(final short... elements) {
		short max = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (max < elements[i]) {
				max = elements[i];
			}
		}
		return max;
	}

	public static byte min(final byte... elements) {
		byte min = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (min > elements[i]) {
				min = elements[i];
			}
		}
		return min;
	}

	public static double min(final double... elements) {
		double min = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (min > elements[i]) {
				min = elements[i];
			}
		}
		return min;
	}

	public static float min(final float... elements) {
		float min = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (min > elements[i]) {
				min = elements[i];
			}
		}
		return min;
	}

	public static int min(final int... elements) {
		int min = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (min > elements[i]) {
				min = elements[i];
			}
		}
		return min;
	}

	public static long min(final long... elements) {
		long min = elements[0];
		for (int i = 1; i < elements.length; i++) {
			if (min > elements[i]) {
				min = elements[i];
			}
		}
		return min;
	}

	public static short min(final short... elements) {
		short min = elements[0];
		for (short i = 1; i < elements.length; i++) {
			if (min > elements[i]) {
				min = elements[i];
			}
		}
		return min;
	}

	public static byte[] reverse(final byte... elements) {
		final byte[] result = elements.clone();
		for (int i = 0, len = elements.length, base = len - 1; i < len; i++) {
			result[i] = elements[base - i];
		}
		return result;
	}

	public static double[] reverse(final double... elements) {
		final double[] result = elements.clone();
		for (int i = 0, len = elements.length, base = len - 1; i < len; i++) {
			result[i] = elements[base - i];
		}
		return result;
	}

	public static <E> E[] reverse(final E[] elements) {
		final E[] result = elements.clone();
		for (int i = 0, len = elements.length, base = len - 1; i < len; i++) {
			result[i] = elements[base - i];
		}
		return result;
	}

	public static float[] reverse(final float... elements) {
		final float[] result = elements.clone();
		for (int i = 0, len = elements.length, base = len - 1; i < len; i++) {
			result[i] = elements[base - i];
		}
		return result;
	}

	public static int[] reverse(final int... elements) {
		final int[] result = elements.clone();
		for (int i = 0, len = elements.length, base = len - 1; i < len; i++) {
			result[i] = elements[base - i];
		}
		return result;
	}

	public static long[] reverse(final long... elements) {
		final long[] result = elements.clone();
		for (int i = 0, len = elements.length, base = len - 1; i < len; i++) {
			result[i] = elements[base - i];
		}
		return result;
	}

	public static short[] reverse(final short... elements) {
		final short[] result = elements.clone();
		for (int i = 0, len = elements.length, base = len - 1; i < len; i++) {
			result[i] = elements[base - i];
		}
		return result;
	}

	public static BigDecimal sum(final BigDecimal... elements) {
		BigDecimal sum = BigDecimal.ZERO;
		for (BigDecimal element : elements) {
			sum.add(element);
		}
		return sum;
	}

	public static BigInteger sum(final BigInteger... elements) {
		BigInteger sum = BigInteger.ZERO;
		for (BigInteger element : elements) {
			sum.add(element);
		}
		return sum;
	}

	public static long sum(final byte... elements) {
		long sum = 0;
		for (byte element : elements) {
			sum += element;
		}
		return sum;
	}

	public static double sum(final double... elements) {
		double sum = 0;
		for (double element : elements) {
			sum += element;
		}
		return sum;
	}

	public static double sum(final float... elements) {
		double sum = 0;
		for (float element : elements) {
			sum += element;
		}
		return sum;
	}

	public static long sum(final int... elements) {
		long sum = 0;
		for (int element : elements) {
			sum += element;
		}
		return sum;
	}

	public static long sum(final long... elements) {
		long sum = 0;
		for (long element : elements) {
			sum += element;
		}
		return sum;
	}

	public static long sum(final short... elements) {
		long sum = 0;
		for (short element : elements) {
			sum += element;
		}
		return sum;
	}

	public static void swap(final BigDecimal[] elements, final int i,
			final int j) {
		final BigDecimal temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static void swap(final BigInteger[] elements, final int i,
			final int j) {
		final BigInteger temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static void swap(final boolean[] elements, final int i, final int j) {
		final boolean temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static void swap(final byte[] elements, final int i, final int j) {
		final byte temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static void swap(final char[] elements, final int i, final int j) {
		final char temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static void swap(final double[] elements, final int i, final int j) {
		final double temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static <E> void swap(final E[] elements, final int i, final int j) {
		final E temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static void swap(final float[] elements, final int i, final int j) {
		final float temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static void swap(final int[] elements, final int i, final int j) {
		final int temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static void swap(final long[] elements, final int i, final int j) {
		final long temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static void swap(final short[] elements, final int i, final int j) {
		final short temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}

	public static BigDecimal weightsAvg(final BigDecimal[] elements, final BigDecimal[] weightses) {
		if (elements.length != weightses.length) {
			throw new IllegalArgumentException("weightses&elements length not equal");
		}
		BigDecimal[] result = new BigDecimal[elements.length];
		BigDecimal weights = BigDecimal.ZERO;
		for (int i = 0; i < elements.length; i++) {
			result[i] = elements[i].multiply(weightses[i]);
			weights = weights.add(weightses[i]);
		}
		return sum(result).divide(weights);
	}

	public static BigInteger weightsAvg(final BigInteger[] elements, final BigInteger[] weightses) {
		if (elements.length != weightses.length) {
			throw new IllegalArgumentException("weightses&elements length not equal");
		}
		BigInteger[] result = new BigInteger[elements.length];
		BigInteger weights = BigInteger.ZERO;
		for (int i = 0; i < elements.length; i++) {
			result[i] = elements[i].multiply(weightses[i]);
			weights = weights.add(weightses[i]);
		}
		return sum(result).divide(weights);
	}

	public static long weightsAvg(final byte[] elements, final long[] weightses) {
		if (elements.length != weightses.length) {
			throw new IllegalArgumentException("weightses&elements length not equal");
		}
		long[] result = new long[elements.length];
		long weights = 0;
		for (int i = 0; i < elements.length; i++) {
			result[i] = elements[i] * weightses[i];
			weights += weightses[i];
		}
		return sum(elements) / weights;
	}

	public static double weightsAvg(final double[] elements, final double[] weightses) {
		if (elements.length != weightses.length) {
			throw new IllegalArgumentException("weightses&elements length not equal");
		}
		double[] result = new double[elements.length];
		long weights = 0;
		for (int i = 0; i < elements.length; i++) {
			result[i] = elements[i] * weightses[i];
			weights += weightses[i];
		}
		return sum(elements) / weights;
	}

	public static double weightsAvg(final float[] elements, final double[] weightses) {
		if (elements.length != weightses.length) {
			throw new IllegalArgumentException("weightses&elements length not equal");
		}
		double[] result = new double[elements.length];
		long weights = 0;
		for (int i = 0; i < elements.length; i++) {
			result[i] = elements[i] * weightses[i];
			weights += weightses[i];
		}
		return sum(elements) / weights;
	}

	public static long weightsAvg(final int[] elements, final long[] weightses) {
		if (elements.length != weightses.length) {
			throw new IllegalArgumentException("weightses&elements length not equal");
		}
		long[] result = new long[elements.length];
		long weights = 0;
		for (int i = 0; i < elements.length; i++) {
			result[i] = elements[i] * weightses[i];
			weights += weightses[i];
		}
		return sum(elements) / weights;
	}

	public static long weightsAvg(final long[] elements, final long[] weightses) {
		if (elements.length != weightses.length) {
			throw new IllegalArgumentException("weightses&elements length not equal");
		}
		long[] result = new long[elements.length];
		long weights = 0;
		for (int i = 0; i < elements.length; i++) {
			result[i] = elements[i] * weightses[i];
			weights += weightses[i];
		}
		return sum(elements) / weights;
	}

	public static long weightsAvg(final short[] elements, final long[] weightses) {
		if (elements.length != weightses.length) {
			throw new IllegalArgumentException("weightses&elements length not equal");
		}
		long[] result = new long[elements.length];
		long weights = 0;
		for (int i = 0; i < elements.length; i++) {
			result[i] = elements[i] * weightses[i];
			weights += weightses[i];
		}
		return sum(elements) / weights;
	}

	private ArrayUtil() {
	}
}
