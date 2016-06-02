package org.opencb.opencga.catalog.audit;


import org.opencb.commons.datastore.core.ObjectMap;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AuditRecord {

    public enum Resource {user, project, study, file, sample, job, individual, cohort, dataset, panel, tool, variableSet}
    public enum Action {create, update, view, delete, index, login, logout, share}

    private Object id;
    private Resource resource;
    private Action action;
    private ObjectMap before;
    private ObjectMap after;
    /*
     * Time in milliseconds
     */
    private long timeStamp;
    private String userId;
    private String description;
    private ObjectMap attributes;

    public AuditRecord() {
    }

    public AuditRecord(Object id, Resource resource, Action action, ObjectMap before, ObjectMap after, long timeStamp, String userId,
                       String description, ObjectMap attributes) {
        this.id = id;
        this.resource = resource;
        this.action = action;
        this.before = before;
        this.after = after;
        this.timeStamp = timeStamp;
        this.userId = userId;
        this.description = description;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "AuditRecord{"
                + "id=" + id
                + ", resource=" + resource
                + ", action=" + action
                + ", before=" + (before == null ? "null" : before)
                + ", after=" + (after == null ? "null" : after)
                + ", timeStamp=" + timeStamp
                + ", userId='" + userId + '\''
                + ", description='" + description + '\''
                + ", attributes=" + attributes
                + '}';
    }

    public Object getId() {
        return id;
    }

    public AuditRecord setId(Object id) {
        this.id = id;
        return this;
    }

    public Resource getResource() {
        return resource;
    }

    public AuditRecord setResource(Resource resource) {
        this.resource = resource;
        return this;
    }

    public Action getAction() {
        return action;
    }

    public AuditRecord setAction(Action action) {
        this.action = action;
        return this;
    }

    public ObjectMap getBefore() {
        return before;
    }

    public AuditRecord setBefore(ObjectMap before) {
        this.before = before;
        return this;
    }

    public ObjectMap getAfter() {
        return after;
    }

    public AuditRecord setAfter(ObjectMap after) {
        this.after = after;
        return this;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public AuditRecord setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public AuditRecord setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public AuditRecord setDescription(String description) {
        this.description = description;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public AuditRecord setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }

}
