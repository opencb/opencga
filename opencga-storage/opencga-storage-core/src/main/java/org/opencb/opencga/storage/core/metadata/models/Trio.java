package org.opencb.opencga.storage.core.metadata.models;

import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class represents a family trio.
 *
 * - Father and mother can be null
 * - Child cannot be null
 * - All samples must be unique
 * - All samples must be different to NA (dash)
 *
 */
public class Trio {
    public static final String NA = "-";
    private final String id;
    private final String father;
    private final String mother;
    private final String child;

    public Trio(String trio) {
        String[] split = trio.split(",");
        if (split.length != 3) {
            throw new IllegalArgumentException("Expected three samples in trio '" + trio + "'");
        }
        this.id = null;
        this.child = split[0];
        this.father = NA.equals(split[1]) ? null : split[1];
        this.mother = NA.equals(split[2]) ? null : split[2];
    }

    public Trio(List<String> trio) {
        this(null, trio);
    }

    public Trio(String id, List<String> trio) {
        this.id = id;
        this.child = trio.get(0);
        this.father = NA.equals(trio.get(1)) ? null : trio.get(1);
        this.mother = NA.equals(trio.get(2)) ? null : trio.get(2);
    }

    public Trio(String father, String mother, String child) {
        this(null, father, mother, child);
    }

    public Trio(String id, String father, String mother, String child) {
        this.id = id;
        this.father = NA.equals(father) ? null : father;
        this.mother = NA.equals(mother) ? null : mother;
        this.child = child;
    }

    public String getId() {
        return id;
    }

    public String getFather() {
        return father;
    }

    public String getMother() {
        return mother;
    }

    public String getChild() {
        return child;
    }

    /**
     * Returns a list with the non-null samples in the trio.
     *
     * @return List of samples
     */
    public List<String> toList() {
        ArrayList<String> list = new ArrayList<>(3);
        list.add(getChild());
        if (getFather() != null) {
            list.add(getFather());
        }
        if (getMother() != null) {
            list.add(getMother());
        }
        return list;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Trio trio = (Trio) o;
        return Objects.equals(id, trio.id)
                && Objects.equals(father, trio.father)
                && Objects.equals(mother, trio.mother)
                && Objects.equals(child, trio.child);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, father, mother, child);
    }

    /**
     * Serialize the trio into a string contain the three samples separated by commas.
     * order: child, father, mother.
     * If the father or mother are null, they will be replaced by {@link #NA}.
     *
     * Can be deserialized using {@link #Trio(String)}.
     *
     * @return String
     */
    public String serialize() {
        ArrayList<String> list = new ArrayList<>(3);
        list.add(getChild());
        if (getFather() == null) {
            list.add(NA);
        } else {
            list.add(getFather());
        }
        if (getMother() == null) {
            list.add(NA);
        } else {
            list.add(getMother());
        }
        return Strings.join(list, ',');
    }

    @Override
    public String toString() {
        return Strings.join(toList(), ',');
    }
}
