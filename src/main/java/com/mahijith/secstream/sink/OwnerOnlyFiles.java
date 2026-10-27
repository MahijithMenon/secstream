package com.mahijith.secstream.sink;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;

/**
 * Opens append-only output files readable by the owning user only. Event and dead-letter files
 * contain security data (user ids, IPs), so they must not be world-readable by default.
 */
public final class OwnerOnlyFiles {
  private OwnerOnlyFiles() {}

  public static FileChannel openForAppend(Path path) throws IOException {
    Path parent = path.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    OpenOption[] options = {
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
    };
    if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
      FileAttribute<?> ownerOnly =
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"));
      return FileChannel.open(path, java.util.Set.of(options), ownerOnly);
    }
    return FileChannel.open(path, options);
  }
}
