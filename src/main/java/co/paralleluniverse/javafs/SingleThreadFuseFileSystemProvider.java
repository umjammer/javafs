package co.paralleluniverse.javafs;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.spi.FileSystemProvider;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import co.paralleluniverse.fuse.DirectoryFiller;
import co.paralleluniverse.fuse.StructFuseFileInfo;
import co.paralleluniverse.fuse.StructStat;
import co.paralleluniverse.fuse.StructStatvfs;
import co.paralleluniverse.fuse.StructTimeBuffer;

class SingleThreadFuseFileSystemProvider extends FuseFileSystemProvider {

    /** */
    private final ExecutorService singleService = Executors.newSingleThreadExecutor();

    /** */
    private final ExecutorService multiService = Executors.newCachedThreadPool();

    public SingleThreadFuseFileSystemProvider(FileSystemProvider fsp, URI uri, boolean debug) {
        super(fsp, uri, debug);
    }

    public SingleThreadFuseFileSystemProvider(FileSystem fs, boolean debug) {
        super(fs, debug);
    }

    @Override
    protected int getattr(String path, StructStat stat) {
        try {
            return multiService.submit(() -> super.getattr(path, stat)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int readlink(String path, ByteBuffer buffer, long size) {
        try {
            return singleService.submit(() -> super.readlink(path, buffer, size)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int mknod(String path, long mode, long dev) {
        try {
            return singleService.submit(() -> super.mknod(path, mode, dev)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int mkdir(String path, long mode) {
        try {
            return singleService.submit(() -> super.mkdir(path, mode)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int unlink(String path) {
        try {
            return singleService.submit(() -> super.unlink(path)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int rmdir(String path) {
        try {
            return singleService.submit(() -> super.rmdir(path)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int symlink(String path, String target) {
        try {
            return singleService.submit(() -> super.symlink(path, target)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int rename(String path, String newName) {
        try {
            return singleService.submit(() -> super.rename(path, newName)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int link(String path, String target) {
        try {
            return singleService.submit(() -> super.link(path, target)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int chmod(String path, long mode) {
        try {
            return singleService.submit(() -> super.chmod(path, mode)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int chown(String path, long uid, long gid) {
        try {
            return singleService.submit(() -> super.chown(path, uid, gid)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int truncate(String path, long offset) {
        try {
            return singleService.submit(() -> super.truncate(path, offset)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int open(String path, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.open(path, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int read(String path, ByteBuffer buffer, long size, long offset, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.read(path, buffer, size, offset, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int write(String path, ByteBuffer buffer, long size, long offset, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.write(path, buffer, size, offset, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int statfs(String path, StructStatvfs statvfs) {
        try {
            return singleService.submit(() -> super.statfs(path, statvfs)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int flush(String path, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.flush(path, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public int release(String path, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.release(path, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int fsync(String path, int datasync, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.fsync(path, datasync, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int opendir(String path, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.opendir(path, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int readdir(String path, StructFuseFileInfo info, DirectoryFiller filler) {
        try {
            return singleService.submit(() -> super.readdir(path, info, filler)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int releasedir(String path, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.releasedir(path, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int fsyncdir(String path, int dataSync, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.fsyncdir(path, dataSync, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int access(String path, int access) {
        try {
            return multiService.submit(() -> super.access(path, access)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int create(String path, long mode, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.create(path, mode, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int ftruncate(String path, long offset, StructFuseFileInfo info) {
        try {
            return singleService.submit(() -> super.ftruncate(path, offset, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int fgetattr(String path, StructStat stat, StructFuseFileInfo info) {
        try {
            return multiService.submit(() -> super.fgetattr(path, stat, info)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int utimens(String path, StructTimeBuffer timeBuffer) {
        try {
            return singleService.submit(() -> super.utimens(path, timeBuffer)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }
}
