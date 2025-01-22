// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import java.util.Set;

public record DeleteFilesRequest(Set<String> objectKeyPaths) {
}
