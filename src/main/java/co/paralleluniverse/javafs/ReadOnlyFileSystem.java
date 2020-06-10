package co.paralleluniverse.javafs;

import java.nio.file.FileSystem;
import java.nio.file.spi.FileSystemProvider;

import co.paralleluniverse.filesystem.FileSystemAdapter;

/**
 *
 * @author pron
 */
class ReadOnlyFileSystem extends FileSystemAdapter {
    public ReadOnlyFileSystem(FileSystem fs) {
        super(fs, new ReadOnlyFileSystemProvider(fs.provider()));
    }

    ReadOnlyFileSystem(FileSystem fs, FileSystemProvider fsp) {
        super(fs, fsp);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }
}
