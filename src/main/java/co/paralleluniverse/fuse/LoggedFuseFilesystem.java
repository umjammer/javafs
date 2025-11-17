package co.paralleluniverse.fuse;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;

import jnr.ffi.Pointer;


final class LoggedFuseFilesystem extends FuseFilesystem {
    private interface LoggedMethod<T> {
        T invoke();
    }

    private interface LoggedVoidMethod {
        void invoke();
    }

    private static final String methodSuccess = "Method succeeded.";
    private static final String methodFailure = "Exception thrown: ";
    private static final String methodResult = " Result: ";
    private final String className;
    private final Logger logger;
    private final FuseFilesystem filesystem;

    LoggedFuseFilesystem(FuseFilesystem filesystem, Logger logger) {
        this.filesystem = filesystem;
        this.logger = logger;
        className = filesystem.getClass().getName();
    }

    private void log(String methodName, LoggedVoidMethod method) {
        log(methodName, method, null, (Object[]) null);
    }

    private void log(String methodName, LoggedVoidMethod method, String path, Object... args) {
        try {
            logger.log(Level.TRACE, "entering %s, %s, %s".formatted(className, methodName, Arrays.toString(args)));
            method.invoke();
            logger.log(Level.INFO, "entering %s, %s, %s, %s".formatted(className, methodName, (path == null ? "" : "[" + path + "] ") + methodSuccess, args));
            logger.log(Level.TRACE, "exiting %s, %s, %s".formatted(className, methodName, Arrays.toString(args)));
        } catch (Throwable e) {
            logException(e, methodName, null, args);
        }
    }

    private <T> T log(String methodName, T defaultValue, LoggedMethod<T> method) {
        return log(methodName, defaultValue, method, null, (Object[]) null);
    }

    private <T> T log(String methodName, T defaultValue, LoggedMethod<T> method, String path, Object... args) {
        try {
            logger.log(Level.TRACE, "entering %s, %s, %s".formatted(className, methodName, Arrays.toString(args)));
            T result = method.invoke();
            logger.log(Level.INFO, "entering %s, %s, %s, %s".formatted(className, methodName, (path == null ? "" : "[" + path + "] ") + methodSuccess + methodResult + result, args));
            logger.log(Level.TRACE, "exiting %s, %s, %s".formatted(className, methodName, Arrays.toString(args)));
            return result;
        } catch (Throwable e) {
            return logException(e, methodName, defaultValue, args);
        }
    }

    private <T> T logException(Throwable e, String methodName, T defaultValue, Object... args) {
        StackTraceElement[] stack = e.getStackTrace();
        StringBuilder builder = new StringBuilder();
        for (StackTraceElement element : stack)
            builder.append('\n').append(element);

        logger.log(Level.ERROR, "entering %s, %s, %s, %s".formatted(className, methodName, methodFailure + e + builder, args));
        return defaultValue;
    }

    @Override
    public void _destroy() {
        destroy();
        _destroy(this, filesystem);
    }

    @Override
    public int access(String path, int access) {
        return log("access", 0, () -> filesystem.access(path, access), path, access);
    }

    @Override
    public void afterUnmount(Path mountPoint) {
        log("afterUnmount", () -> filesystem.afterUnmount(mountPoint), mountPoint.toString());
    }

    @Override
    public void beforeMount(Path mountPoint) {
        log("beforeMount", () -> filesystem.beforeMount(mountPoint), mountPoint.toString());
    }

    @Override
    public int bmap(String path, StructFuseFileInfo info) {
        return log("bmap", 0, () -> filesystem.bmap(path, info), path, info);
    }

    @Override
    public int chmod(String path, long mode) {
        return log("chmod", 0, () -> filesystem.chmod(path, mode), path, mode);
    }

    @Override
    public int chown(String path, long uid, long gid) {
        return log("chown", 0, () -> filesystem.chown(path, uid, gid), path, uid, gid);
    }

    @Override
    public int create(String path, long mode, StructFuseFileInfo info) {
        return log("create", 0, () -> filesystem.create(path, mode, info), path, mode, info);
    }

    @Override
    public void destroy() {
        log("destroy", () -> filesystem.destroy());
    }

    @Override
    public int fgetattr(String path, StructStat stat, StructFuseFileInfo info) {
        return log("fgetattr", 0, () -> filesystem.fgetattr(path, stat, info), path, stat);
    }

    @Override
    public int flush(String path, StructFuseFileInfo info) {
        return log("flush", 0, () -> filesystem.flush(path, info), path, info);
    }

    @Override
    public int fsync(String path, int datasync, StructFuseFileInfo info) {
        return log("fsync", 0, () -> filesystem.fsync(path, datasync, info), path, info);
    }

    @Override
    public int fsyncdir(String path, int datasync, StructFuseFileInfo info) {
        return log("fsyncdir", 0, () -> filesystem.fsyncdir(path, datasync, info), path, info);
    }

    @Override
    public int ftruncate(String path, long offset, StructFuseFileInfo info) {
        return log("ftruncate", 0, () -> filesystem.ftruncate(path, offset, info), path, offset, info);
    }

    @Override
    public int getattr(String path, StructStat stat) {
        return log("getattr", 0, () -> filesystem.getattr(path, stat), path, stat);
    }

    @Override
    protected String getName() {
        return log("getName", null, () -> filesystem.getName());
    }

    @Override
    protected String[] getOptions() {
        return log("getOptions", null, () -> filesystem.getOptions());
    }

    @Override
    public int getxattr(String path, String xattr, XattrFiller filler, long size, long position) {
        return log("getxattr", 0, () -> filesystem.getxattr(path, xattr, filler, size, position), path, xattr, filler, size, position);
    }

    @Override
    public void init() {
        log("init", () -> filesystem.init());
    }

    @Override
    public int link(String path, String target) {
        return log("link", 0, () -> filesystem.link(path, target), path, target);
    }

    @Override
    public int listxattr(String path, XattrListFiller filler) {
        return log("listxattr", 0, () -> filesystem.listxattr(path, filler), path, filler);
    }

    @Override
    public int lock(String path, StructFuseFileInfo info, int command, StructFlock flock) {
        return log("lock", 0, () -> filesystem.lock(path, info, command, flock), path, info, command, flock);
    }

    @Override
    public int mkdir(String path, long mode) {
        return log("mkdir", 0, () -> filesystem.mkdir(path, mode), path, mode);
    }

    @Override
    public int mknod(String path, long mode, long dev) {
        return log("mknod", 0, () -> filesystem.mknod(path, mode, dev), path, mode, dev);
    }

    @Override
    public int open(String path, StructFuseFileInfo info) {
        return log("open", 0, () -> filesystem.open(path, info), path, info);
    }

    @Override
    public int opendir(String path, StructFuseFileInfo info) {
        return log("opendir", 0, () -> filesystem.opendir(path, info), path, info);
    }

    @Override
    public int read(String path, ByteBuffer buffer, long size, long offset, StructFuseFileInfo info) {
        return log("read", 0, () -> filesystem.read(path, buffer, size, offset, info), path, buffer, size, offset, info);
    }

    @Override
    public int readdir(String path, StructFuseFileInfo info, DirectoryFiller filler) {
        return log("readdir", 0, () -> filesystem.readdir(path, info, filler), path, filler);
    }

    @Override
    public int readlink(String path, ByteBuffer buffer, long size) {
        return log("readlink", 0, () -> filesystem.readlink(path, buffer, size), path, buffer, size);
    }

    @Override
    public int release(String path, StructFuseFileInfo info) {
        return log("release", 0, () -> filesystem.release(path, info), path, info);
    }

    @Override
    public int releasedir(String path, StructFuseFileInfo info) {
        return log("releasedir", 0, () -> filesystem.releasedir(path, info), path, info);
    }

    @Override
    public int removexattr(String path, String xattr) {
        return log("removexattr", 0, () -> filesystem.removexattr(path, xattr), path, xattr);
    }

    @Override
    public int rename(String path, String newName) {
        return log("rename", 0, () -> filesystem.rename(path, newName));
    }

    @Override
    public int rmdir(String path) {
        return log("rmdir", 0, () -> filesystem.rmdir(path), path);
    }

    @Override
    public int setxattr(String path, String name, ByteBuffer buf, long size, int flags, int position) {
        return log("setxattr", 0, () -> filesystem.setxattr(path, name, buf, size, flags, position), path, name, buf, size, flags, position);
    }

    @Override
    public int statfs(String path, StructStatvfs statvfs) {
        return log("statfs", 0, () -> filesystem.statfs(path, statvfs), path, statvfs);
    }

    @Override
    public int symlink(String path, String target) {
        return log("symlink", 0, () -> filesystem.symlink(path, target), path, target);
    }

    @Override
    public int truncate(String path, long offset) {
        return log("truncate", 0, () -> filesystem.truncate(path, offset), path, offset);
    }

    @Override
    public int unlink(String path) {
        return log("unlink", 0, () -> filesystem.unlink(path), path);
    }

    @Override
    public int utimens(String path, StructTimeBuffer timeBuffer) {
        return log("utimens", 0, () -> filesystem.utimens(path, timeBuffer), path, timeBuffer);
    }

    @Override
    public int write(String path, ByteBuffer buf, long bufSize, long writeOffset, StructFuseFileInfo wrapper) {
        return log("write", 0, () -> filesystem.write(path, buf, bufSize, writeOffset, wrapper), path, buf, bufSize, writeOffset, wrapper);
    }

    @Override
    protected int ioctl(String path, int cmd, Pointer arg, StructFuseFileInfo fi, long flags, Pointer data) {
        return log("ioctl", 0, () -> filesystem.ioctl(path, cmd, arg, fi, flags, data), path, cmd, arg, fi, flags, data);
    }

    @Override
    protected int poll(String path, StructFuseFileInfo fi, StructFusePollHandle ph, Pointer reventsp) {
        return log("poll", 0, () -> filesystem.poll(path, fi, ph, reventsp), path, fi, ph, reventsp);
    }

    @Override
    protected int write_buf(String path, StructFuseBufvec buf, long off, StructFuseFileInfo fi) {
        return log("write_buf", 0, () -> filesystem.write_buf(path, buf, off, fi), path, buf, off, fi);
    }

    @Override
    protected int read_buf(String path, Pointer bufp, long size, long off, StructFuseFileInfo fi) {
        return log("read_buf", 0, () -> filesystem.read_buf(path, bufp, size, off, fi), path, bufp, size, off, fi);
    }

    @Override
    protected int flock(String path, StructFuseFileInfo fi, int op) {
        return log("flock", 0, () -> filesystem.flock(path, fi, op), path, fi, op);
    }

    @Override
    protected int fallocate(String path, int mode, long off, long length, StructFuseFileInfo fi) {
        return log("fallocate", 0, () -> filesystem.fallocate(path, mode, off, length, fi), path, mode, off, length, fi);
    }

}
