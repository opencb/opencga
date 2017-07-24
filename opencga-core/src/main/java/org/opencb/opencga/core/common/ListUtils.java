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
import java.util.*;

/**
 * This class has been moved to java-common-libs.
 */
@Deprecated
public class ListUtils {

    public static <E> List<E> unique(List<E> array) {
        if (array == null)
            throw new NullPointerException("Not initizalize array");
        List<E> uniq = new ArrayList<E>();
        HashMap<E, Boolean> visited = new HashMap<E, Boolean>();
        for (E s : array) {
            if (!visited.containsKey(s)) {
                uniq.add(s);
                visited.put(s, true);
            }
        }
        return uniq;
    }


    public static <E> List<E> duplicated(List<E> array) {
        if (array == null)
            throw new NullPointerException("Not initizalize array");
        List<E> dupl = new ArrayList<E>();
        HashMap<E, Boolean> visited = new HashMap<E, Boolean>();
        for (E s : array) {
            if (!visited.containsKey(s)) {
                visited.put(s, true);
            } else {
                if (!dupl.contains(s)) {
                    dupl.add(s);
                }

            }
        }
        return dupl;
    }

    public static <E> List<E> intersection(List<E> list1, List<E> list2) {
        if (list1 == null || list2 == null)
            throw new NullPointerException("Not initizalized lists");
        List<E> intersection = new LinkedList<E>();
        E item;
        ListIterator<E> iter = list1.listIterator();
        while (iter.hasNext()) {
            item = iter.next();
            if (list2.contains(item)) {
                intersection.add(item);
            }
        }
        return intersection;
    }

    public static <E> List<E> concat(final List<E> list1, final List<E> list2) {
        if (list1 == null || list2 == null)
            throw new NullPointerException("Not initizalized lists");
        List<E> concat = new ArrayList<E>(list1.size() + list2.size());
        concat.addAll(list1);
        concat.addAll(list2);
        return concat;
    }

    public static <E> List<E> union(final List<E> list1, final List<E> list2) {
        if (list1 == null || list2 == null)
            throw new NullPointerException("Not initizalized lists");
        List<E> concat = new ArrayList<E>(list1.size() + list2.size());
        concat.addAll(list1);
        E item;
        ListIterator<E> iter = list1.listIterator();
        while (iter.hasNext()) {
            item = iter.next();
            if (!concat.contains(item)) {
                concat.add(item);
            }
        }
        return concat;
    }

    public static <E> int sizeNotNull(final List<E> list) {
        int count = 0;
        for (E e : list) {
            if (e != null) {
                count++;
            }
        }
        return count;
    }

    public static <E> List<E> notEmpty(final List<E> list) {
        List<E> noEmpty = new ArrayList<E>(list.size());
        for (E e : list) {
            if (e != null && !e.toString().equals("")) {
                noEmpty.add(e);
            }
        }
        return noEmpty;
    }

    @Deprecated
    public static <E> List<E> itemsToList(E... elements) {
        List<E> list = new ArrayList<E>(elements.length);
        for (E elem : elements) {
            list.add(elem);
        }
        return list;
    }

    /**
     * Copies the elements at the indexes positions
     *
     * @param <E>
     * @param oriList
     * @param indexes
     * @return
     */
    public static <E> List<E> subList(final List<E> oriList, int[] indexes) {
        List<E> list = new ArrayList<E>(indexes.length);
        for (int index : indexes) {
            if (index >= 0 && index < oriList.size()) {
                list.add(oriList.get(index));
            }
        }
        return list;
    }

    /**
     * Copies all the elements form start (excluded) to end (included)
     *
     * @param <E>
     * @param oriList
     * @param start
     * @param end
     * @return The list with the elements
     */
    public static <E> List<E> subList(final List<E> oriList, int start, int end) {
        if (start < 0 || end >= oriList.size()) {
            return null;
        }
        List<E> list = new ArrayList<E>(end - start + 1);
        for (int i = start; i <= end; i++) {
            list.add(oriList.get(i));
        }
        return list;
    }

    @SuppressWarnings({"rawtypes"})
    public static <E extends Comparable> int[] order(final List<E> array) {
        return order(array, false);
    }

    @SuppressWarnings({"rawtypes"})
    public static <E extends Comparable> int[] order(List<E> array, boolean desc) {
        Map<E, List<Integer>> map = new TreeMap<E, List<Integer>>();
        for (int i = 0; i < array.size(); ++i) {
            if (map.containsKey(array.get(i))) {
                map.get(array.get(i)).add(i);
            } else {
                List<Integer> list = new ArrayList<Integer>();
                list.add(i);
                map.put(array.get(i), list);
            }
        }
        List<Integer> indices = new ArrayList<Integer>();
        for (List<Integer> list : map.values()) {
            indices.addAll(list);
        }

        if (desc) {
            Collections.reverse(indices);
        }
        return toIntArray(indices);
    }


    @SuppressWarnings({"rawtypes"})
    public static <E extends Comparable> List<E> ordered(final List<E> array, int[] order) {
        if (array.size() != order.length) {
            return null;
        }
        List<E> list = new ArrayList<E>(array.size());
        for (int i : order) {
            list.add(array.get(i));
        }
        return list;
    }

    public static <E> List<E> initialize(int numElements, E elem) {
        List<E> list = new ArrayList<E>(numElements);
        for (int i = 0; i < numElements; i++) {
            list.add(elem);
        }
        return list;
    }

    public static <E> List<String> initialize(int numElements, E prefix, int startNumber) {
        List<String> list = new ArrayList<String>(numElements);
        for (int i = 0; i < numElements; i++) {
            list.add(prefix.toString() + startNumber++);
        }
        return list;
    }

    public static List<Double> random(int numElements) {
        return random(numElements, 1.0);
    }

    public static List<Double> random(int numElements, double scaleFactor) {
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Random r = new Random(System.currentTimeMillis());
        List<Double> randomList = new ArrayList<Double>(numElements);
        for (int i = 0; i < numElements; i++) {
            randomList.add(r.nextDouble() * scaleFactor);
        }
        return randomList;
    }

    public static List<Double> randomGaussian(int numElements) {
        return randomGaussian(numElements, 1.0);
    }

    public static List<Double> randomGaussian(int numElements, double scaleFactor) {
        Random r = new Random(System.currentTimeMillis());
        List<Double> randomList = new ArrayList<Double>(numElements);
        for (int i = 0; i < numElements; i++) {
            randomList.add(r.nextGaussian() * scaleFactor);
        }
        return randomList;
    }

    /*
     *
     * Converters from List to the same type of Array, signature:  toArray()
     *
     */
//	public static double[] toArray(final List<Double> list) {
//		double[] array = null;
//		if(list != null) {
//			array = new double[list.size()];
//			for(int i=0 ; i<list.size() ; i++) {
//				array[i] = list.get(i);
//			}
//		}
//		return array;
//	}
//	public static int[] toArray(final List<Integer> list) {
//		int[] array = null;
//		if(list != null) {
//			array = new int[list.size()];
//			for(int i=0 ; i<list.size() ; i++) {
//				array[i] = list.get(i);
//			}
//		}
//		return array;
//	}
//	public static String[] toArray(final List<String> list) {
//		String[] array = null;
//		if(list != null) {
//			array = new String[list.size()];
//			for(int i=0 ; i<list.size() ; i++) {
//				array[i] = list.get(i);
//			}
//		}
//		return array;
//	}
    @SuppressWarnings("unchecked")
    public static <E> E[] toArray(final List<E> list) {
        E[] array = null;
        if (list != null && list.size() > 0) {
            array = (E[]) Array.newInstance(list.get(0).getClass(), list.size());
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) != null) {
                    array[i] = list.get(i);
                } else {
                    array[i] = null;
                }
            }
        }
        return array;
    }

    public static <E> List<E> toList(final Enumeration<E> enumeration) {
        return Collections.list(enumeration);
    }

    /*
     *
     * Converters from List to different TYPE of Arrays or List, method name:  toTYPEArray() or toTYPEList()
     *
     */
    public static <E> double[] toDoubleArray(final List<E> list) {
        return toDoubleArray(list, Double.NaN);
    }

    public static <E> double[] toDoubleArray(final List<E> list, Double defaultValue) {
        double[] doubleArray = null;
        if (list != null) {
            doubleArray = new double[list.size()];
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) != null) {
                    try {
                        doubleArray[i] = Double.parseDouble(list.get(i).toString());
                    } catch (NumberFormatException nfe) {
                        doubleArray[i] = defaultValue;
                    }
                } else {
                    doubleArray[i] = defaultValue;
                }
            }
        }
        return doubleArray;
    }

    public static <E> List<Double> toDoubleList(final List<E> list) {
        List<Double> doubleList = null;
        if (list != null) {
            doubleList = new ArrayList<Double>(list.size());
            for (E e : list) {
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


    public static <E> int[] toIntArray(final List<E> list) {
        return toIntArray(list, -1);
    }

    public static <E> int[] toIntArray(final List<E> list, int defaultValue) {
        int[] intArray = null;
        if (list != null) {
            intArray = new int[list.size()];
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) != null) {
                    try {
                        intArray[i] = Integer.parseInt(list.get(i).toString());
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

    public static <E> List<Integer> toIntegerList(final List<E> list) {
        List<Integer> integerList = null;
        if (list != null) {
            integerList = new ArrayList<Integer>(list.size());
            for (E e : list) {
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

    public static <E> boolean[] toBooleanArray(final List<E> list) {
        return toBooleanArray(list, false);
    }

    public static <E> boolean[] toBooleanArray(final List<E> list, boolean defaultValue) {
        boolean[] booleanArray = null;
        if (list != null) {
            booleanArray = new boolean[list.size()];
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) != null) {
                    try {
                        booleanArray[i] = "true".equalsIgnoreCase(list.get(i).toString().trim()) || "1".equals(list.get(i).toString().trim());
                    } catch (NumberFormatException nfe) {
                        booleanArray[i] = defaultValue;
                    }
                } else {
                    booleanArray[i] = defaultValue;
                }
            }
        }
        return booleanArray;
    }

    public static <E> List<Boolean> toBooleanList(final List<E> list) {
        List<Boolean> booleanList = null;
        if (list != null) {
            booleanList = new ArrayList<Boolean>(list.size());
            for (E e : list) {
                if (e != null) {
                    booleanList.add("true".equalsIgnoreCase(e.toString().trim()) || "1".equals(e.toString().trim()));
                } else {
                    booleanList.add(null);
                }
            }
        }
        return booleanList;
    }


    public static <E> String[] toStringArray(final List<E> list) {
        return toStringArray(list, null);
    }

    public static <E> String[] toStringArray(final List<E> list, String defaultValue) {
        String[] array = null;
        if (list != null) {
            array = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) != null) {
                    array[i] = list.get(i).toString();
                } else {
                    array[i] = defaultValue;
                }
            }
        }
        return array;
    }

    public static <E> List<String> toStringList(final List<E> list) {
        List<String> stringList = null;
        if (list != null) {
            stringList = new ArrayList<String>(list.size());
            for (E e : list) {
                if (e != null) {
                    stringList.add(e.toString());
                } else {
                    stringList.add(null);
                }
            }
        }
        return stringList;
    }


    /*
     *
     * toString methods
     *
     */
    public static <E> String toString(final List<E> list) {
        return toString(list, "\t");
    }

    public static <E> String toString(final List<E> list, String separator) {
        StringBuilder sb = new StringBuilder();
        if (list != null && list.size() > 0) {
            for (int i = 0; i < list.size() - 1; i++) {
                if (list.get(i) != null) {
                    sb.append(list.get(i).toString()).append(separator);
                } else {
                    sb.append("null").append(separator);
                }
            }
            if (list.get(list.size() - 1) != null) {
                sb.append(list.get(list.size() - 1).toString());
            } else {
                sb.append("null");
            }
        }
        return sb.toString();
    }


    @Deprecated
    public static <E> List<E> toList(final E[] array) {
        List<E> list = new ArrayList<E>(array.length);
        for (E e : array) {
            list.add(e);
        }
        return list;
    }

    @Deprecated
    public static List<Double> toList(final double[] array) {
        List<Double> list = new ArrayList<Double>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(array[i]);
        }
        return list;
    }

    @Deprecated
    public static List<Integer> toList(final int[] array) {
        List<Integer> list = new ArrayList<Integer>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(array[i]);
        }
        return list;
    }

    @Deprecated
    public static <E> List<String> toStringList(final E[] array) {
        List<String> list = new ArrayList<String>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(array[i].toString());
        }
        return list;
    }

    @Deprecated
    public static List<String> toStringList(final double[] array) {
        List<String> list = new ArrayList<String>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(String.valueOf(array[i]));
        }
        return list;
    }

    @Deprecated
    public static List<String> toStringList(final int[] array) {
        List<String> list = new ArrayList<String>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(String.valueOf(array[i]));
        }
        return list;
    }
}
