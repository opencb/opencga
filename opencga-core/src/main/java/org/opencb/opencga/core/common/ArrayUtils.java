/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.common;

import java.lang.reflect.Array;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class ArrayUtils {

    private static final double EPSILON = 0.000000000000001;

	/*
     *
	 * GETTING THE MAX OF A INTEGER ARRAY
	 * 
	 */

    public static int max(int[] values) {
        return max(values, 0, values.length);
    }

    public static int max(int[] values, int begin, int end) {
        int index = maxIndex(values, begin, end);
        if (index >= 0 && index < values.length) {
            return values[index];
        } else {
            return Integer.MIN_VALUE;
        }
    }

    public static int maxIndex(int[] values) {
        return maxIndex(values, 0, values.length);
    }

    public static int maxIndex(int[] values, int begin, int end) {
        int max = Integer.MIN_VALUE;
        int index = -1;
        if (checkArguments(values, begin, end)) {
            max = values[begin];
            index = begin;
            for (int i = begin; i < end; i++) {
                if (values[i] > max) {
                    max = values[i];
                    index = i;
                }
            }
        }
        return index;
    }

    /*
     * MAX - DOUBLE ARRAY
     */
    public static double max(double[] values) {
        return max(values, 0, values.length);
    }

    public static double max(double[] values, int begin, int end) {
        int index = maxIndex(values, begin, end);
        if (index >= 0 && index < values.length) {
            return values[index];
        } else {
            return Double.NaN;
        }
    }

    public static int maxIndex(double[] values) {
        return maxIndex(values, 0, values.length);
    }

    public static int maxIndex(double[] values, int begin, int end) {
        double max = Double.NaN;
        int index = -1;
        if (checkArguments(values, begin, end)) {
            max = values[begin];
            index = begin;
            for (int i = begin; i < end; i++) {
                if (!Double.isNaN(values[i])) {
                    if (values[i] > max) {
                        max = values[i];
                        index = i;
                    }
                }
            }
        }
        return index;
    }

	/*
     *
	 * GETTING THE MIN OF A VECTOR
	 * 
	 */

    public static int min(int[] values) {
        return min(values, 0, values.length);
    }

    public static int min(int[] values, int begin, int end) {
        int index = minIndex(values, begin, end);
        if (index >= 0 && index < values.length) {
            return values[index];
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public static int minIndex(int[] values) {
        return minIndex(values, 0, values.length);
    }

    public static int minIndex(int[] values, int begin, int end) {
        int min = Integer.MAX_VALUE;
        int index = -1;
        if (checkArguments(values, begin, end)) {
            min = values[begin];
            index = begin;
            for (int i = begin; i < end; i++) {
                if (values[i] < min) {
                    min = values[i];
                    index = i;
                }
            }
        }
        return index;
    }


    public static double min(double[] values) {
        return min(values, 0, values.length);
    }

    public static double min(double[] values, int begin, int end) {
        int index = minIndex(values, begin, end);
        if (index >= 0 && index < values.length) {
            return values[index];
        } else {
            return Double.NaN;
        }
    }

    public static int minIndex(double[] values) {
        return minIndex(values, 0, values.length);
    }

    public static int minIndex(double[] values, int begin, int end) {
        double min = Double.NaN;
        int index = -1;
        if (checkArguments(values, begin, end)) {
            min = values[begin];
            index = begin;
            for (int i = begin; i < end; i++) {
                if (!Double.isNaN(values[i])) {
                    if (values[i] < min) {
                        min = values[i];
                        index = i;
                    }
                }
            }
        }
        return index;
    }

    public static double[] abs(double[] in) {
        if (in != null && in.length > 0) {
            double[] out = new double[in.length];
            for (int i = 0; i < in.length; i++) {
                out[i] = Math.abs(in[i]);
            }
            return out;
        } else {
            return null;
        }
    }

    /*
     *
     * SEQUENCE METHODS
     *
     */
    public static int[] sequence(int start, int end) {
        return sequence(start, end, 1);
    }

    public static int[] sequence(int start, int end, int increment) {
        int[] data = new int[(end - start) / increment + 1];
        for (int i = start, index = 0; i <= end; i += increment, index++) {
            data[index] = i;
        }
        return data;
    }

    public static double[] sequence(double start, double end, double increment) {
        double[] data = new double[(int) (Math.round((end - start) / increment + 1))];
        int index = 0;
        DecimalFormat format = new DecimalFormat("#.####");
        for (double i = start; i <= end; i += increment) {
            i = Double.parseDouble(format.format(i));
            data[index] = i;
            index++;
        }
        return data;
    }

    /*
     *
     * SORT AND ORDER METHODS
     *
     */
    public static int[] sort(final int[] data) {
        return sort(data, true);
    }

    public static int[] sort(final int[] data, boolean ascending) {
        int[] sortedData = data.clone();
        Arrays.sort(sortedData);
        if (!ascending) {
            sortedData = reverse(sortedData);
        }
        return sortedData;
    }

    public static double[] sort(final double[] data) {
        return sort(data, true);
    }

    public static double[] sort(final double[] data, boolean ascending) {
        double[] sortedData = data.clone();
        Arrays.sort(sortedData);
        if (!ascending) {
            sortedData = reverse(sortedData);
        }
        return sortedData;
    }


    public static int[] reverse(final int[] data) {
        int[] reverseData = new int[data.length];
        for (int i = 0, j = data.length - 1; i < data.length; i++, j--) {
            reverseData[i] = data[j];
        }
        return reverseData;
    }

    public static double[] reverse(final double[] data) {
        double[] reverseData = new double[data.length];
        for (int i = 0, j = data.length - 1; i < data.length; i++, j--) {
            reverseData[i] = data[j];
        }
        return reverseData;
    }

    public static int[] order(final int[] data) {
        return order(toDoubleArray(data), false);
    }

    public static int[] order(final int[] data, boolean decreasing) {
        return order(toDoubleArray(data), decreasing);
    }

    public static int[] order(final double[] data) {
        return order(data, false);
    }

    public static int[] order(final double[] data, boolean decreasing) {
        int[] order = new int[data.length];
        double[] clonedData = data.clone();

        if (decreasing) {
            double min = nonInfinityAndMinValueMin(clonedData);
            // to avoid the NaN missorder
            for (int i = 0; i < clonedData.length; i++) {
                if (Double.isNaN(clonedData[i])) {
                    clonedData[i] = min - 3;
                }
                if (equals(clonedData[i],Double.NEGATIVE_INFINITY)) {
                    clonedData[i] = min - 2;
                }
                if (equals(clonedData[i], Double.MIN_VALUE)) {
                    clonedData[i] = min - 1;
                }
            }
            // get the order
            for (int i = 0; i < clonedData.length; i++) {
                order[i] = maxIndex(clonedData);
                clonedData[order[i]] = Double.NEGATIVE_INFINITY;
            }

        } else {
            double max = nonInfinityAndMaxValueMax(clonedData);
            // to avoid the NaN missorder
            for (int i = 0; i < clonedData.length; i++) {
                if (Double.isNaN(clonedData[i])) {
                    clonedData[i] = max + 3;
                }
                if (equals(clonedData[i], Double.POSITIVE_INFINITY)) {
                    clonedData[i] = max + 2;
                }
                if (equals(clonedData[i], Double.MAX_VALUE)) {
                    clonedData[i] = max + 1;
                }
            }
            // get the order
            for (int i = 0; i < clonedData.length; i++) {
                order[i] = minIndex(clonedData);
                clonedData[order[i]] = Double.POSITIVE_INFINITY;
            }
        }

        return order;
    }

    public static double nonInfinityAndMinValueMin(double[] data) {
        double min = Double.POSITIVE_INFINITY;
        for (int i = 0; i < data.length; i++) {
            if (data[i] < min && !equals(data[i], Double.NEGATIVE_INFINITY) && !equals(data[i], Double.MIN_VALUE)) {
                min = data[i];
            }
        }
        return min;
    }


    public static double nonInfinityAndMaxValueMax(double[] data) {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < data.length; i++) {
            if (data[i] > max && !equals(data[i], Double.POSITIVE_INFINITY) && !equals(data[i], Double.MAX_VALUE)) {
                max = data[i];
            }
        }
        return max;
    }

    public static int[] ordered(final int[] data, int[] positions) {
        int[] ordered = null;
        if (data.length == positions.length) {
            ordered = new int[data.length];
            for (int i = 0; i < positions.length; i++) {
                ordered[i] = data[positions[i]];
            }
        }
        return ordered;
    }

    public static double[] ordered(final double[] data, int[] positions) {
        double[] ordered = null;
        if (data.length == positions.length) {
            ordered = new double[data.length];
            for (int i = 0; i < positions.length; i++) {
                ordered[i] = data[positions[i]];
            }
        }
        return ordered;
    }

    public static double[] initialize(int numElements, double elem) {
        double[] array = new double[numElements];
        for (int i = 0; i < numElements; i++) {
            array[i] = elem;
        }
        return array;
    }

    @SuppressWarnings("unchecked")
    public static <E> E[] initialize(int numElements, E elem) {
        E[] array = (E[]) Array.newInstance(elem.getClass(), numElements);
        for (int i = 0; i < numElements; i++) {
            array[i] = elem;
        }
        return array;
    }

    public static <E> String[] initialize(int numElements, E prefix, int startNumber) {
        String[] array = new String[numElements];
        for (int i = 0; i < numElements; i++) {
            array[i] = (prefix.toString() + startNumber++);
        }
        return array;
    }

    public static double[] random(int numElements) {
        return random(numElements, 1.0);
    }

    public static double[] random(int numElements, double scaleFactor) {
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Random r = new Random(System.currentTimeMillis());
        double[] randomList = new double[numElements];
        for (int i = 0; i < numElements; i++) {
            randomList[i] = r.nextDouble() * scaleFactor;
        }
        return randomList;
    }

    public static double[] randomGaussian(int numElements) {
        return randomGaussian(numElements, 1.0);
    }

    public static double[] randomGaussian(int numElements, double scaleFactor) {
        Random r = new Random(System.currentTimeMillis());
        double[] randomList = new double[numElements];
        for (int i = 0; i < numElements; i++) {
            randomList[i] = r.nextGaussian() * scaleFactor;
        }
        return randomList;
    }

    /*
     *
     * Converters from Array to the same type of List, signature:  toList()
     *
     */
    public static List<Double> toList(final double[] array) {
        List<Double> list = new ArrayList<Double>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(array[i]);
        }
        return list;
    }

    public static List<Integer> toList(final int[] array) {
        List<Integer> list = new ArrayList<Integer>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(array[i]);
        }
        return list;
    }

    public static List<String> toList(final String[] array) {
        List<String> list = new ArrayList<String>(array.length);
        for (String s : array) {
            list.add(s);
        }
        return list;
    }

    public static <E> List<E> toList(final E[] array) {
        List<E> list = new ArrayList<E>(array.length);
        for (E e : array) {
            list.add(e);
        }
        return list;
    }

    /*
     *
     * Converters from Array to different TYPE of Arrays or List, signature:  toTYPEArray() or toTYPEList()
     *
     */
    public static <E> String[] toStringArray(final E[] array) {
        String[] stringList = null;
        if (array != null) {
            stringList = new String[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] != null) {
                    stringList[i] = array[i].toString();
                } else {
                    stringList[i] = null;
                }
            }
        }
        return stringList;
    }

    public static List<String> toStringList(final double[] array) {
        List<String> list = new ArrayList<String>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(String.valueOf(array[i]));
        }
        return list;
    }

    public static List<String> toStringList(final int[] array) {
        List<String> list = new ArrayList<String>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(String.valueOf(array[i]));
        }
        return list;
    }

    public static <E> List<String> toStringList(final E[] array) {
        List<String> stringList = null;
        if (array != null) {
            stringList = new ArrayList<String>(array.length);
            for (E e : array) {
                if (e != null) {
                    stringList.add(e.toString());
                } else {
                    stringList.add(null);
                }
            }
        }
        return stringList;
    }


    public static double[] toDoubleArray(final int[] intArray) {
        double[] array = null;
        if (intArray != null) {
            array = new double[intArray.length];
            for (int i = 0; i < intArray.length; i++) {
                array[i] = (double) intArray[i];
            }
        }
        return array;
    }

    public static double[] toDoubleArray(final String[] array) {
        double[] doubleArray = null;
        if (array != null) {
            doubleArray = new double[array.length];
            for (int i = 0; i < array.length; i++) {
                try {
                    doubleArray[i] = Double.parseDouble(array[i]);
                } catch (NumberFormatException e) {
                    doubleArray[i] = Double.NaN;
                }
            }
        }
        return doubleArray;
    }

    public static <E> double[] toDoubleArray(final E[] array) {
        double[] stringList = null;
        if (array != null) {
            stringList = new double[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] != null) {
                    try {
                        stringList[i] = Double.parseDouble(array[i].toString());
                    } catch (NumberFormatException nfe) {
                        stringList[i] = Double.NaN;
                    }
                } else {
                    stringList[i] = Double.NaN;
                }
            }
        }
        return stringList;
    }

    public static <E> List<Double> toDoubleList(final E[] array) {
        List<Double> doubleList = null;
        if (array != null) {
            doubleList = new ArrayList<Double>(array.length);
            for (E e : array) {
                if (e != null) {
                    try {
                        doubleList.add(Double.parseDouble(e.toString()));
                    } catch (NumberFormatException nfe) {
                        doubleList.add(null);
                    }
                } else {
                    doubleList.add(null);
                }
            }
        }
        return doubleList;
    }


    public static int[] toIntArray(final double[] doubleArray) {
        int[] dataToInt = null;
        if (doubleArray != null) {
            dataToInt = new int[doubleArray.length];
            for (int i = 0; i < doubleArray.length; i++) {
                dataToInt[i] = (int) Math.round(doubleArray[i]);
            }
        }
        return dataToInt;
    }

    public static int[] toIntArray(final String[] array, int defaultValue) {
        int[] intArray = null;
        if (array != null) {
            intArray = new int[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] != null) {
                    try {
                        intArray[i] = Integer.parseInt(array[i].toString());
                    } catch (NumberFormatException nfe) {
                        intArray[i] = defaultValue;
                    }
                } else {
                    intArray[i] = defaultValue;
                }
            }
        }
        return intArray;
    }

    public static <E> int[] toIntArray(final E[] array, int defaultValue) {
        int[] stringList = null;
        if (array != null) {
            stringList = new int[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] != null) {
                    try {
                        stringList[i] = Integer.parseInt(array[i].toString());
                    } catch (NumberFormatException nfe) {
                        stringList[i] = defaultValue;
                    }
                } else {
                    stringList[i] = defaultValue;
                }
            }
        }
        return stringList;
    }

    public static <E> List<Integer> toIntegerList(final E[] array) {
        List<Integer> integerList = null;
        if (array != null) {
            integerList = new ArrayList<Integer>(array.length);
            for (E e : array) {
                if (e != null) {
                    try {
                        integerList.add(Integer.parseInt(e.toString()));
                    } catch (NumberFormatException nfe) {
                        integerList.add(null);
                    }
                } else {
                    integerList.add(null);
                }
            }
        }
        return integerList;
    }


    public static <E> boolean[] toBooleanArray(final E[] array) {
        boolean[] stringList = null;
        if (array != null) {
            stringList = new boolean[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] != null) {
                    try {
                        stringList[i] = "true".equalsIgnoreCase(array[i].toString().trim()) || "1".equals(array[i].toString().trim());
                    } catch (NumberFormatException nfe) {
                        stringList[i] = false;
                    }
                } else {
                    stringList[i] = false;
                }
            }
        }
        return stringList;
    }

    public static <E> List<Boolean> toBooleanList(final E[] array) {
        List<Boolean> booleanList = null;
        if (array != null) {
            booleanList = new ArrayList<Boolean>(array.length);
            for (E e : array) {
                if (e != null) {
                    booleanList.add("true".equalsIgnoreCase(e.toString().trim()) || "1".equals(e.toString().trim()));
                } else {
                    booleanList.add(false);
                }
            }
        }
        return booleanList;
    }


    /*
     *
     * toString methods
     *
     */
    public static <E> String toString(final int[] list) {
        return toString(list, "\t");
    }

    public static <E> String toString(final int[] list, String separator) {
        StringBuilder sb = new StringBuilder();
        if (list != null && list.length > 0) {
            for (int i = 0; i < list.length - 1; i++) {
                sb.append(list[i]).append(separator);
            }
            sb.append(list[list.length - 1]);
        }
        return sb.toString();
    }

    public static <E> String toString(final double[] list) {
        return toString(list, "\t");
    }

    public static <E> String toString(final double[] list, String separator) {
        StringBuilder sb = new StringBuilder();
        if (list != null && list.length > 0) {
            for (int i = 0; i < list.length - 1; i++) {
                sb.append(list[i]).append(separator);
            }
            sb.append(list[list.length - 1]);
        }
        return sb.toString();
    }

    public static <E> String toString(final E[] list) {
        return toString(list, "\t");
    }

    public static <E> String toString(final E[] list, String separator) {
        StringBuilder sb = new StringBuilder();
        if (list != null && list.length > 0) {
            for (int i = 0; i < list.length - 1; i++) {
                if (list[i] != null) {
                    sb.append(list[i].toString()).append(separator);
                } else {
                    sb.append("null");
                }
            }
            if (list[list.length - 1] != null) {
                sb.append(list[list.length - 1].toString());
            } else {
                sb.append("null");
            }
        }
        return sb.toString();
    }

	/*
     *
	 * private methods
	 * 
	 */

    private static boolean checkArguments(final int[] values, final int begin, final int end) {
        if (values == null || values.length == 0) {
            return false;
        }
        if (begin < 0 || end > values.length) {
            return false;
        }
        return true;
    }

    private static boolean checkArguments(final double[] values, final int begin, final int end) {
        if (values == null || values.length == 0) {
            return false;
        }
        if (begin < 0 || end > values.length) {
            return false;
        }
        return true;
    }


    /*
     *
     * deprecated
     *
     */
    @Deprecated
    public static <E> String[] toStringArray(final List<E> list) {
        String[] array = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i).toString();
        }
        return array;
    }

    @Deprecated
    public static int[] toIntegerArray(final List<Integer> list) {
        int[] array = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    @Deprecated
    public static double[] toDoubleArray(final List<Double> list) {
        double[] array = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    public static boolean equals(final double first, final double second) {
        return (Math.abs(second - first) < EPSILON);
    }

    public static boolean equals(final float first, final float second) {
        return (Math.abs(second - first) < EPSILON);
    }

}
