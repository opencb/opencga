package org.opencb.opencga.storage.mongodb.alignment;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class AlignmentIndexNotExistsException extends Exception {

    /**
     * Creates a new instance of
     * <code>AlignmentIndexNotExistsException</code> without detail message.
     */
    public AlignmentIndexNotExistsException() {
    }

    /**
     * Constructs an instance of
     * <code>AlignmentIndexNotExistsException</code> with the specified detail
     * message.
     *
     * @param msg the detail message.
     */
    public AlignmentIndexNotExistsException(String msg) {
        super(msg);
    }
}
