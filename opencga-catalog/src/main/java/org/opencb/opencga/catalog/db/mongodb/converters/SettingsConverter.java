package org.opencb.opencga.catalog.db.mongodb.converters;

import org.opencb.opencga.core.models.settings.Settings;

public class SettingsConverter extends OpenCgaMongoConverter<Settings> {

    public SettingsConverter() {
        super(Settings.class);
    }

}
