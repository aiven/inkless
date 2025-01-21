// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres.converters;

import org.jooq.generated.enums.FileReasonT;
import org.jooq.impl.AbstractConverter;

import io.aiven.inkless.control_plane.FileReason;

public class FileReasonTToFileReasonConverter extends AbstractConverter<FileReasonT, FileReason> {
    public FileReasonTToFileReasonConverter() {
        super(FileReasonT.class, FileReason.class);
    }

    @Override
    public FileReason from(final FileReasonT databaseObject) {
        if (databaseObject == null) {
            return null;
        }
        return FileReason.fromName(databaseObject.getLiteral());
    }

    @Override
    public FileReasonT to(final FileReason userObject) {
        if (userObject == null) {
            return null;
        }
        return FileReasonT.lookupLiteral(userObject.name);
    }
}
