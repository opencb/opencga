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

package org.opencb.opencga.analysis.clinical.meta.tree.node;

public enum OperatorTreeType {
    AND,
    OR,
    NOT_IN;

    /**
     * Parse a string into an Operator enum value.
     * Handles both underscore and space formats (e.g., "NOT_IN" and "NOT IN").
     *
     * @param value the string representation of the operator
     * @return the corresponding Operator enum value
     * @throws IllegalArgumentException if the value is not a valid operator
     */
    public static OperatorTreeType fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Operator cannot be null");
        }

        // Replace spaces with underscores to normalize input
        String normalized = value.trim().toUpperCase().replace(" ", "_");

        try {
            return OperatorTreeType.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid operator: " + value +
                    ". Valid operators are: AND, OR, NOT IN");
        }
    }

    @Override
    public String toString() {
        // Return "NOT IN" instead of "NOT_IN" for better readability
        return this == NOT_IN ? "NOT IN" : name();
    }
}

