package org.opencb.opencga.core.tools.annotations;

@Deprecated
public enum FieldScope {

    MANAGED, //Only opencga can modify
    USER, //Public for user modifications
    CREATE // User mode in create but managed in update
}
