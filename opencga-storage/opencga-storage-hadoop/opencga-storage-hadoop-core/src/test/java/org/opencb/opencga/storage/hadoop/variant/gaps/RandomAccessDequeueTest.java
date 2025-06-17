package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.junit.Test;

import java.util.ListIterator;

import static org.junit.Assert.*;

public class RandomAccessDequeueTest {
    @Test
    public void randomAccessDequeueShouldBeEmptyInitially() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>();
        assertTrue(dequeue.isEmpty());
    }

    @Test
    public void addLastShouldAddElementToEnd() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>();
        dequeue.addLast(1);
        assertEquals(Integer.valueOf(1), dequeue.getTail());
    }

    @Test
    public void removeLastShouldRemoveElementFromEnd() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>();
        dequeue.addLast(1);
        assertEquals(Integer.valueOf(1), dequeue.removeTail());
        assertTrue(dequeue.isEmpty());
    }

    @Test
    public void getShouldReturnElementAtIndex() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>();
        dequeue.addLast(1);
        dequeue.addLast(2);
        assertEquals(Integer.valueOf(1), dequeue.get(0));
        assertEquals(Integer.valueOf(2), dequeue.get(1));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getShouldThrowExceptionForInvalidIndex() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>();
        dequeue.get(0);
    }

    @Test
    public void sizeShouldReturnNumberOfElements() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>();
        assertEquals(0, dequeue.size());
        dequeue.add(1);
        assertEquals(1, dequeue.size());
        dequeue.addLast(2);
        assertEquals(2, dequeue.size());
    }

    @Test
    public void listIteratorShouldIterateForwards() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>();
        for (int i = 0; i < 5; i++) {
            dequeue.addLast(i);
        }
        ListIterator<Integer> it = dequeue.listIterator();
        for (int i = 0; i < 5; i++) {
            assertTrue(it.hasNext());
            assertEquals(Integer.valueOf(i), it.next());
        }
        assertFalse(it.hasNext());
    }

    @Test
    public void listIteratorShouldIterateBackwards() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>();
        for (int i = 0; i < 5; i++) {
            dequeue.addLast(i);
        }
        ListIterator<Integer> it = dequeue.listIterator();
        while (it.hasNext()) {
            it.next();
        }
        for (int i = 4; i >= 0; i--) {
            assertTrue(it.hasPrevious());
            assertEquals(Integer.valueOf(i), it.previous());
        }
        assertFalse(it.hasPrevious());
    }

    @Test
    public void listIteratorNextIndexShouldReturnCorrectIndex() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>();
        for (int i = 0; i < 5; i++) {
            dequeue.addLast(i);
        }
        ListIterator<Integer> it = dequeue.listIterator();
        for (int i = 0; i < 5; i++) {
            assertEquals(i, it.nextIndex());
            it.next();
        }
    }

    @Test
    public void listIteratorPreviousIndexShouldReturnCorrectIndex() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>();
        for (int i = 0; i < 5; i++) {
            dequeue.addLast(i);
        }
        ListIterator<Integer> it = dequeue.listIterator();
        while (it.hasNext()) {
            it.next();
        }
        for (int i = 4; i >= 0; i--) {
            assertEquals(i, it.previousIndex());
            it.previous();
        }
    }

    @Test
    public void listIteratorNextIndexShouldReturnCorrectIndexWhileRemoving() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>(9);
        for (int batch = 0; batch < 10; batch++) {
            for (int i = 0; i < 5; i++) {
                dequeue.addLast(i);
            }
            ListIterator<Integer> it = dequeue.listIterator();
            for (int i = 0; i < 5; i++) {
                assertEquals(i, it.nextIndex());
                it.next();
            }
            assertEquals(dequeue.size(), it.nextIndex());
            for (int i = 0; i < 5; i++) {
                dequeue.removeHead();
                assertEquals(dequeue.size(), it.nextIndex());
            }
            assertTrue(dequeue.isEmpty());
        }
    }

    @Test
    public void listIteratorShouldHandleWrapAroundMultipleTimes() {
        RandomAccessDequeue<Integer> dequeue = new RandomAccessDequeue<>(6);
        for (int batch = 0; batch < 10; batch++) {
            for (int i = 0; i < 5; i++) {
                dequeue.addLast(i);
            }
            dequeue.removeHead(); // Remove an element to cause wrap-around
            dequeue.addLast(5);
            ListIterator<Integer> it = dequeue.listIterator();
            assertTrue(it.hasNext());
            assertEquals(Integer.valueOf(1), it.next());
            assertTrue(it.hasNext());
            assertEquals(Integer.valueOf(2), it.next());
            assertTrue(it.hasNext());
            assertEquals(Integer.valueOf(3), it.next());
            assertTrue(it.hasNext());
            assertEquals(Integer.valueOf(4), it.next());
            assertTrue(it.hasNext());
            assertEquals(Integer.valueOf(5), it.next());
            assertFalse(it.hasNext());

            while (!dequeue.isEmpty()) {
                dequeue.removeHead();
            }
        }
    }

}