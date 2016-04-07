package org.opencb.opencga.catalog.config;

/**
 * Created by pfurio on 07/04/16.
 */
public class Policies {

    private UserCreation userCreation;

    public Policies() {
        this.userCreation = UserCreation.ALWAYS;
    }

    public UserCreation getUserCreation() {
        return userCreation;
    }

    public void setUserCreation(UserCreation userCreation) {
        this.userCreation = userCreation;
    }

    public static enum UserCreation {
        ONLY_ADMIN, ANY_LOGGED_USER, ALWAYS
    }

}
