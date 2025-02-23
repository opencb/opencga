/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.exceptions;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthorizationException extends CatalogException {

    public enum ErrorCode {
        KILL_JOB("kill", "Only the job owner or the study administrator can kill a job.");

        private final String permission;
        private final String message;

        ErrorCode(String permission, String message) {
            this.permission = permission;
            this.message = message;
        }

        public String getPermission() {
            return permission;
        }

        public String getMessage() {
            return message;
        }
    }


    public CatalogAuthorizationException(String message) {
        super(message);
    }

    public CatalogAuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogAuthorizationException(Throwable cause) {
        super(cause);
    }

    public static CatalogAuthorizationException cantRead(String userId, String resource, String id, String name) {
        return deny(userId, "read", resource, id, name);
    }

    public static CatalogAuthorizationException cantWrite(String userId, String resource, String id, String name) {
        return deny(userId, "write", resource, id, name);
    }

    public static CatalogAuthorizationException cantModify(String userId, String resource, String id, String name) {
        return deny(userId, "modify", resource, id, name);
    }

    public static CatalogAuthorizationException cantExecute(String userId, String resource, String id, String name) {
        return deny(userId, "execute", resource, id, name);
    }

    @Deprecated
    public static CatalogAuthorizationException deny(String userId, String permission, String resource, long id, String name) {
        return new CatalogAuthorizationException("Permission denied. "
                + (userId == null || userId.isEmpty() ? "" : "User '" + userId + "'")
                + " cannot " + permission + " "
                + resource + " { uid: " + id + (name == null || name.isEmpty() ? "" : ", name: \"" + name + "\"") + " }");
    }

    public static CatalogAuthorizationException deny(String userId, String permission, String resource, String id, String name) {
        return new CatalogAuthorizationException("Permission denied. "
                + (userId == null || userId.isEmpty() ? "" : "User '" + userId + "'")
                + " cannot " + permission + " "
                + resource + " { id: " + id + (name == null || name.isEmpty() ? "" : ", name: \"" + name + "\"") + " }");
    }

    public static CatalogAuthorizationException deny(String userId, String resource, String id, ErrorCode errorCode) {
        return deny(userId, resource, id, errorCode, null);
    }

    public static CatalogAuthorizationException deny(String userId, String resource, String id, ErrorCode errorCode, Throwable cause) {
        return new CatalogAuthorizationException("Permission denied. "
                + (userId == null || userId.isEmpty() ? "" : "User '" + userId + "'")
                + " cannot " + errorCode.getPermission() + " " + resource + " { id: " + id + " }. " + errorCode.getMessage(), cause);
    }

    public static CatalogAuthorizationException deny(String userId, String description) {
        return new CatalogAuthorizationException("Permission denied. " + (userId == null || userId.isEmpty() ? "" : "User '" + userId + "'")
                + " cannot " + description);
    }

    public static CatalogAuthorizationException deny(Throwable cause) {
        return new CatalogAuthorizationException("Permission denied.", cause);
    }

    public static CatalogAuthorizationException denyAny(String userId, String permission, String resource) {
        return new CatalogAuthorizationException("Permission denied. "
                + (userId == null || userId.isEmpty() ? "" : "User '" + userId + "'")
                + " cannot " + permission + " any " + resource + ".");
    }

    public static CatalogAuthorizationException userIsBanned(String userId) {
        return new CatalogAuthorizationException("Too many login attempts. The account for user '" + userId + "' is banned."
                + " Please, talk to your organization owner/administrator.");
    }

    public static CatalogAuthorizationException userIsSuspended(String userId) {
        return new CatalogAuthorizationException("The account for user '" + userId + "' is suspended. Please, talk to your organization"
                + " owner/administrator.");
    }

    public static CatalogAuthorizationException accountIsExpired(String userId, String expirationDate) {
        return new CatalogAuthorizationException("The account for user '" + userId + "' expired on " + expirationDate + ". Please,"
                + " talk to your organization owner/administrator.");
    }

    public static CatalogAuthorizationException passwordExpired(String userId, String expirationDate) {
        return new CatalogAuthorizationException("The password for the user account '" + userId + "' expired on " + expirationDate
                + ". Please, reset your password or talk to your organization owner/administrator.");
    }

    public static CatalogAuthorizationException notOrganizationOwner() {
        return notOrganizationOwner("perform this action");
    }

    public static CatalogAuthorizationException notOrganizationOwner(String action) {
        return new CatalogAuthorizationException("Permission denied: Only the organization owner can " + action);
    }

    public static CatalogAuthorizationException notOrganizationOwnerOrAdmin() {
        return notOrganizationOwnerOrAdmin("perform this action");
    }

    public static CatalogAuthorizationException notOrganizationOwnerOrAdmin(String action) {
        return new CatalogAuthorizationException("Permission denied: Only the owner or administrators of the organization can " + action);
    }

    public static CatalogAuthorizationException notStudyAdmin(String action) {
        return new CatalogAuthorizationException("Permission denied: Only the study administrators can " + action);
    }

    public static CatalogAuthorizationException notStudyMember(String action) {
        return new CatalogAuthorizationException("Permission denied: Only the members of the study can " + action);
    }

    public static CatalogAuthorizationException opencgaAdminOnlySupportedOperation() {
        return opencgaAdminOnlySupportedOperation("perform this action");
    }

    public static CatalogAuthorizationException opencgaAdminOnlySupportedOperation(String action) {
        if (action == null || action.isEmpty()) {
            action = "perform this action";
        }
        return new CatalogAuthorizationException("Permission denied: Only the OPENCGA ADMINISTRATOR users can " + action);
    }

}
