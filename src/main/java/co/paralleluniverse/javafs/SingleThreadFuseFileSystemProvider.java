package co.paralleluniverse.javafs;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.spi.FileSystemProvider;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import co.paralleluniverse.fuse.DirectoryFiller;
import co.paralleluniverse.fuse.StructFuseFileInfo;
import co.paralleluniverse.fuse.StructStat;
import co.paralleluniverse.fuse.StructStatvfs;
import co.paralleluniverse.fuse.StructTimeBuffer;

class SingleThreadFuseFileSystemProvider extends FuseFileSystemProvider {

    /** */
    private ExecutorService singleService = Executors.newSingleThreadExecutor();

    /** */
    private ExecutorService multiService = Executors.newCachedThreadPool();

    public SingleThreadFuseFileSystemProvider(FileSystemProvider fsp, URI uri, boolean debug) {
        super(fsp, uri, debug);
    }

    public SingleThreadFuseFileSystemProvider(FileSystem fs, boolean debug) {
        super(fs, debug);
    }

    @Override
    protected int getattr(String path, StructStat stat) {
        Future<Integer> f = multiService.submit(() -> {
            return super.getattr(path, stat);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int readlink(String path, ByteBuffer buffer, long size) {
        Future<Integer> f = singleService.submit(() -> {
            return super.readlink(path, buffer, size);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int mknod(String path, long mode, long dev) {
        Future<Integer> f = singleService.submit(() -> {
            return super.mknod(path, mode, dev);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int mkdir(String path, long mode) {
        Future<Integer> f = singleService.submit(() -> {
            return super.mkdir(path, mode);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int unlink(String path) {
        Future<Integer> f = singleService.submit(() -> {
            return super.unlink(path);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int rmdir(String path) {
        Future<Integer> f = singleService.submit(() -> {
            return super.rmdir(path);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int symlink(String path, String target) {
        Future<Integer> f = singleService.submit(() -> {
            return super.symlink(path, target);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int rename(String path, String newName) {
        Future<Integer> f = singleService.submit(() -> {
            return super.rename(path, newName);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int link(String path, String target) {
        Future<Integer> f = singleService.submit(() -> {
            return super.link(path, target);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int chmod(String path, long mode) {
        Future<Integer> f = singleService.submit(() -> {
            return super.chmod(path, mode);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int chown(String path, long uid, long gid) {
        Future<Integer> f = singleService.submit(() -> {
            return super.chown(path, uid, gid);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int truncate(String path, long offset) {
        Future<Integer> f = singleService.submit(() -> {
            return super.truncate(path, offset);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int open(String path, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.open(path, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int read(String path, ByteBuffer buffer, long size, long offset, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.read(path, buffer, size, offset, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int write(String path, ByteBuffer buffer, long size, long offset, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.write(path, buffer, size, offset, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int statfs(String path, StructStatvfs statvfs) {
        Future<Integer> f = singleService.submit(() -> {
            return super.statfs(path, statvfs);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int flush(String path, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.flush(path, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public int release(String path, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.release(path, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int fsync(String path, int datasync, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.fsync(path, datasync, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int opendir(String path, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.opendir(path, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int readdir(String path, StructFuseFileInfo info, DirectoryFiller filler) {
        Future<Integer> f = singleService.submit(() -> {
            return super.readdir(path, info, filler);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int releasedir(String path, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.releasedir(path, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int fsyncdir(String path, int datasync, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.fsyncdir(path, datasync, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int access(String path, int access) {
        Future<Integer> f = multiService.submit(() -> {
            return super.access(path, access);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int create(String path, long mode, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.create(path, mode, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int ftruncate(String path, long offset, StructFuseFileInfo info) {
        Future<Integer> f = singleService.submit(() -> {
            return super.ftruncate(path, offset, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int fgetattr(String path, StructStat stat, StructFuseFileInfo info) {
        Future<Integer> f = multiService.submit(() -> {
            return super.fgetattr(path, stat, info);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected int utimens(String path, StructTimeBuffer timeBuffer) {
        Future<Integer> f = singleService.submit(() -> {
            return super.utimens(path, timeBuffer);
        });
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }
}
