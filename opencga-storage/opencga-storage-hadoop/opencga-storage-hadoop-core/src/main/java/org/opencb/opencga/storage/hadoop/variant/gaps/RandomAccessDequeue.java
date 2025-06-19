package org.opencb.opencga.storage.hadoop.variant.gaps;

import java.util.AbstractList;
import java.util.ListIterator;

/**
 * The RandomAccessDequeue class is a circular buffer implementation that extends AbstractList.
 * It supports random access to elements and allows concurrent modifications while iterating.
 *
 * @param <T> the type of elements held in this collection
 */
public class RandomAccessDequeue<T> extends AbstractList<T> {

    private final T[] buffer;
    private int size = 0;
    private int head = 0;
    private int tail = 0;

    public RandomAccessDequeue() {
        this(1000);
    }

    public RandomAccessDequeue(int size) {
        buffer = (T[]) new Object[size];
    }

    @Override
    public T set(int index, T element) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        int realIndex = (head + index) % buffer.length;
        T old = buffer[realIndex];
        buffer[realIndex] = element;
        return old;
    }

    @Override
    public boolean add(T elem) {
        return addLast(elem);
    }

    public boolean addLast(T elem) {
        if (size + 1 == buffer.length) {
            throw new IllegalStateException("Buffer is full. Buffer length: " + buffer.length + ", Current size: " + size);
        }
        buffer[tail] = elem;
        tail = (tail + 1) % buffer.length;
        size++;
        return true;
    }

    public T element() {
        return buffer[head];
    }

    public T getTail() {
        return get(size - 1);
    }

    @Override
    public T get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        return buffer[(head + index) % buffer.length];
    }

    public T remove() {
        return removeHead();
    }

    public T removeHead() {
        if (size <= 0) {
            throw new IllegalStateException("Buffer is empty");
        }
        T elem = buffer[head];
        head = (head + 1) % buffer.length;
        size--;
        return elem;
    }

    public T removeTail() {
        if (size <= 0) {
            throw new IllegalStateException("Buffer is empty");
        }
        tail = (tail - 1 + buffer.length) % buffer.length;
        size--;
        return buffer[tail];
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public void clear() {
        head = 0;
        tail = 0;
        size = 0;
    }

    @Override
    public ListIterator<T> listIterator() {
        return new ListIterator<T>() {
            private final int initialHead = RandomAccessDequeue.this.head;
            private int cursor = 0;

            private int cursor() {
                return ((initialHead - RandomAccessDequeue.this.head) + cursor) % buffer.length;
            }

            @Override
            public boolean hasNext() {
                return cursor() < RandomAccessDequeue.this.size;
            }

            @Override
            public T next() {
                int next = cursor();
                cursor++;
                return get(next);
            }

            @Override
            public boolean hasPrevious() {
                return cursor() > 0;
            }

            @Override
            public T previous() {
                cursor--;
                return get(cursor());
            }

            @Override
            public int nextIndex() {
                return cursor();
            }

            @Override
            public int previousIndex() {
                return cursor() - 1;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void set(T t) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void add(T t) {
                throw new UnsupportedOperationException();
            }
        };
    }


}
