package org.opencb.opencga.lib.tools.accession;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Based on code from Ryan Stansifer (http://cs.fit.edu/~ryan/java/programs/combinations/Permute-java.html)
 * and from HPG Variant (https://github.com/cyenyxe/hpg-variant/blob/next/src/gwas/epistasis/dataset.c)
 * 
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class CombinationIterator<T> implements Iterator {

    private final int size;
    private final T[] elements;  // copy of original 0 .. size-1
    private final T[] output;        // array for output,  0 .. size-1
    private final int[] permutationIdxs;  // perm of nums 1..size, perm[0]=0

    // int[], double[] array won't work :-(
    public CombinationIterator(int combinationSize, T[] elements) {
        this.elements = elements;
        size = elements.length;
        output = (T[]) Array.newInstance(elements.getClass().getComponentType(), combinationSize);
        permutationIdxs = new int[combinationSize];
        for (int i = 0; i < combinationSize-1; i++) {
            permutationIdxs[i] = 0;
        }
        permutationIdxs[combinationSize-1] = -1;
    }

    @Override
    public boolean hasNext() {
        return permutationIdxs[0] < size - 1;
    }

    @Override
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object next() throws NoSuchElementException {
        for (int i = permutationIdxs.length - 1; i >= 0; i--) {
            if (permutationIdxs[i] + 1 < size) {
                // Increment coordinate, for example: (0,1,2) -> (0,1,3)
                permutationIdxs[i] += 1;

                // The following coordinates must be, at least, the same as the current one
                // Let num_blocks=4, (0,1,3) --increment--> (0,2,3) --correct--> (0,2,2)
                if (i < permutationIdxs.length - 1) {
                    for (int j = i + 1; j < permutationIdxs.length; j++) {
                        permutationIdxs[j] = 0;
                    }
                }
                
                // Fill output array
                for (int j = 0; j < permutationIdxs.length; j++) {
                    output[j] = elements[permutationIdxs[j]];
                }
                
                return output;
            }
        }

        throw new NoSuchElementException();
    }

    @Override
    public String toString() {
        final int n = Array.getLength(output);
        final StringBuffer sb = new StringBuffer("[");
        for (int j = 0; j < n; j++) {
            sb.append(Array.get(output, j).toString());
            if (j < n - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        return new String(sb);
    }

    public static void main(String[] args) {
        for (Iterator i = new CombinationIterator(Integer.parseInt(args[0]), Arrays.copyOfRange(args, 1, args.length-1)); i.hasNext();) {
            final String[] a = (String[]) i.next();
            System.out.println(i);
        }
    }
}
