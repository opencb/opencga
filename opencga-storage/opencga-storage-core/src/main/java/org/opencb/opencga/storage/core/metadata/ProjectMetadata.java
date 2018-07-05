package org.opencb.opencga.storage.core.metadata;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;

import java.util.*;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProjectMetadata {

    private String species;
    private String assembly;

    private int release;

    private VariantAnnotationSets annotation;

    private Map<String, Integer> counters;

    private ObjectMap attributes;

    public static class VariantAnnotationSets {
        private VariantAnnotationMetadata current;
        private List<VariantAnnotationMetadata> saved;

        public VariantAnnotationSets() {
            saved = new ArrayList<>();
        }

        public VariantAnnotationSets(VariantAnnotationMetadata current, List<VariantAnnotationMetadata> saved) {
            this.current = current;
            this.saved = saved;
        }

        public VariantAnnotationMetadata getCurrent() {
            return current;
        }

        public VariantAnnotationSets setCurrent(VariantAnnotationMetadata current) {
            this.current = current;
            return this;
        }

        public List<VariantAnnotationMetadata> getSaved() {
            return saved;
        }

        public VariantAnnotationMetadata getSaved(String name) {
            ProjectMetadata.VariantAnnotationMetadata saved = null;
            for (ProjectMetadata.VariantAnnotationMetadata annotation : getSaved()) {
                if (annotation.getName().equals(name)) {
                    saved = annotation;
                }
            }
            if (saved == null) {
                throw new VariantQueryException("Variant Annotation snapshot \"" + name + "\" not found!");
            }
            return saved;
        }

        public VariantAnnotationSets setSaved(List<VariantAnnotationMetadata> saved) {
            this.saved = saved;
            return this;
        }
    }

    public static class VariantAnnotationMetadata {
        private int id;
        private String name;
        private Date creationDate;
        private VariantAnnotatorProgram annotator;
        private List<ObjectMap> sourceVersion;

        public VariantAnnotationMetadata() {
            sourceVersion = new ArrayList<>();
        }

        public VariantAnnotationMetadata(int id, String name, Date creationDate, VariantAnnotatorProgram annotator,
                                         List<ObjectMap> sourceVersion) {
            this.id = id;
            this.name = name;
            this.creationDate = creationDate;
            this.annotator = annotator;
            this.sourceVersion = sourceVersion != null ? sourceVersion : new ArrayList<>();
        }

        public int getId() {
            return id;
        }

        public VariantAnnotationMetadata setId(int id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public VariantAnnotationMetadata setName(String name) {
            this.name = name;
            return this;
        }

        public Date getCreationDate() {
            return creationDate;
        }

        public VariantAnnotationMetadata setCreationDate(Date creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        public VariantAnnotatorProgram getAnnotator() {
            return annotator;
        }

        public VariantAnnotationMetadata setAnnotator(VariantAnnotatorProgram annotator) {
            this.annotator = annotator;
            return this;
        }

        public List<ObjectMap> getSourceVersion() {
            return sourceVersion;
        }

        public VariantAnnotationMetadata setSourceVersion(List<ObjectMap> sourceVersion) {
            this.sourceVersion = sourceVersion;
            return this;
        }
    }

    public static class VariantAnnotatorProgram {
        private String name;
        private String version;
        private String commit;

        public VariantAnnotatorProgram() {
        }

        public VariantAnnotatorProgram(String name, String version, String commit) {
            this.name = name;
            this.commit = commit;
            this.version = version;
        }

        public String getName() {
            return name;
        }

        public VariantAnnotatorProgram setName(String name) {
            this.name = name;
            return this;
        }

        public String getVersion() {
            return version;
        }

        public VariantAnnotatorProgram setVersion(String version) {
            this.version = version;
            return this;
        }

        public String getCommit() {
            return commit;
        }

        public VariantAnnotatorProgram setCommit(String commit) {
            this.commit = commit;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof VariantAnnotatorProgram)) {
                return false;
            }
            VariantAnnotatorProgram that = (VariantAnnotatorProgram) o;
            return Objects.equals(name, that.name)
                    && Objects.equals(version, that.version)
                    && Objects.equals(commit, that.commit);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, version, commit);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .append("name", name)
                    .append("version", version)
                    .append("commit", commit)
                    .toString();
        }


    }

    public ProjectMetadata() {
        release = 1;
        annotation = new VariantAnnotationSets();
        counters = new HashMap<>();
        attributes = new ObjectMap();
    }

    public ProjectMetadata(String species, String assembly, int release) {
        this(species, assembly, release, null, null, null);
    }

    public ProjectMetadata(String species, String assembly, int release, ObjectMap attributes, Map<String, Integer> counters,
                           VariantAnnotationSets annotation) {
        this.species = species;
        this.assembly = assembly;
        this.release = release;
        this.attributes = attributes != null ? attributes : new ObjectMap();
        this.annotation = annotation != null ? annotation : new VariantAnnotationSets();
        this.counters = counters != null ? counters : new HashMap<>();
    }

    public ProjectMetadata copy() {
        return new ProjectMetadata(species, assembly, release, new ObjectMap(attributes), new HashMap<>(counters), annotation);
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

    public VariantAnnotationSets getAnnotation() {
        return annotation;
    }

    public ProjectMetadata setAnnotation(VariantAnnotationSets annotation) {
        this.annotation = annotation;
        return this;
    }

    public Map<String, Integer> getCounters() {
        return counters;
    }

    public ProjectMetadata setCounters(Map<String, Integer> counters) {
        this.counters = counters;
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
