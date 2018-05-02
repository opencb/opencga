package org.opencb.opencga.storage.core.metadata;

import org.opencb.commons.datastore.core.ObjectMap;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProjectMetadata {

    private String species;
    private String assembly;

    private int release;

    private ObjectMap attributes;

    public ProjectMetadata() {
        release = 1;
        attributes = new ObjectMap();
    }

    public ProjectMetadata(String species, String assembly, int release) {
        this(species, assembly, release, null);
    }

    public ProjectMetadata(String species, String assembly, int release, ObjectMap attributes) {
        this.species = species;
        this.assembly = assembly;
        this.release = release;
        this.attributes = attributes != null ? attributes : new ObjectMap();
    }

    public ProjectMetadata copy() {
        return new ProjectMetadata(species, assembly, release, new ObjectMap(attributes));
    }

    public String getSpecies() {
        return species;
    }

    public ProjectMetadata setSpecies(String species) {
        this.species = species;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public ProjectMetadata setAssembly(String assembly) {
        this.assembly = assembly;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public ProjectMetadata setRelease(int release) {
        this.release = release;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public ProjectMetadata setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }

}
