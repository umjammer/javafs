package co.paralleluniverse.fuse;

import jnr.ffi.Pointer;
import jnr.ffi.annotations.Delegate;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.types.dev_t;
import jnr.ffi.types.gid_t;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.ffi.types.u_int32_t;
import jnr.ffi.types.uid_t;

final class StructFuseOperationsIfaces {
    public interface _init       { @Delegate void _init(Pointer conn); }
    public interface _destroy    { @Delegate void _destroy(); }
    public interface _readlink   { @Delegate int _readlink(String path, Pointer buffer, @size_t long size); }
    public interface _mknod      { @Delegate int _mknod(String path, @mode_t long mode, @dev_t long dev); }
    public interface _mkdir      { @Delegate int _mkdir(String path, @mode_t long mode); }
    public interface _unlink     { @Delegate int _unlink(String path); }
    public interface _rmdir      { @Delegate int _rmdir(String path); }
    public interface _symlink    { @Delegate int _symlink(String path, String target); }
    public interface _rename     { @Delegate int _rename(String path, String newName); }
    public interface _link       { @Delegate int _link(String path, String target); }
    public interface _chmod      { @Delegate int _chmod(String path, @mode_t long mode); }
    public interface _chown      { @Delegate int _chown(String path, @uid_t long uid, @gid_t long gid); }
    public interface _truncate   { @Delegate int _truncate(String path, @off_t long offset); }
    public interface _open       { @Delegate int _open(String path, Pointer info); }
    public interface _read       { @Delegate int _read(String path, @Out Pointer buffer, @size_t long size, @off_t long offset, Pointer info); }
    public interface _write      { @Delegate int _write(String path, @In Pointer buffer, @size_t long size, @off_t long offset, Pointer info); }
    public interface _flush      { @Delegate int _flush(String path, Pointer info); }
    public interface _fsync      { @Delegate int _fsync(String path, int datasync, @In Pointer info); }
    public interface _release    { @Delegate int _release(String path, Pointer info); }
    public interface _opendir    { @Delegate int _opendir(String path, Pointer info); }
    public interface _readdir    { @Delegate int _readdir(String path, Pointer buf, Pointer fillFunction, @off_t long offset, @In Pointer info); }
    public interface _releasedir { @Delegate int _releasedir(String path, Pointer info); }
    public interface _fsyncdir   { @Delegate int _fsyncdir(String path, int datasync, @In Pointer info); }
    public interface _access     { @Delegate int _access(String path, int access); }
    public interface _create     { @Delegate int _create(String path, @mode_t long mode, Pointer info); }
    public interface _ftruncate  { @Delegate int _ftruncate(String path, @off_t long offset, @In Pointer info); }
    public interface _utimens    { @Delegate int _utimens(String path, Pointer timebuffer); }
    public interface _bmap       { @Delegate int _bmap(String path, Pointer info); }
    public interface _statfs     { @Delegate int _statfs(String path, @Out Pointer statsvfs); }
    public interface _lock       { @Delegate int _lock(String path, Pointer info, int cmd, Pointer flock); }
    public interface _getattr    { @Delegate int _getattr(String path, Pointer stat); }
    public interface _fgetattr   { @Delegate int _fgetattr(String path, Pointer stat, Pointer info); }

    public interface _listxattr   { @Delegate int _listxattr(String path, Pointer buffer, @size_t long size); }
    public interface _removexattr { @Delegate int _removexattr(String path, String xattr); }

    public interface _getxattr_MAC     { @Delegate int _getxattr(String path, String xattr, Pointer buffer, @size_t long size, @u_int32_t long position); }
    public interface _setxattr_MAC     { @Delegate int _setxattr(String path, String xattr, Pointer value, @size_t long size, int flags, int position); }

    public interface _getxattr_NOT_MAC { @Delegate int _getxattr(String path, String xattr, Pointer buffer, @size_t long size); }
    public interface _setxattr_NOT_MAC { @Delegate int _setxattr(String path, String xattr, Pointer value, @size_t long size, int flags); }

    public interface _ioctl     { @Delegate void _ioctl(String path, int cmd, Pointer arg, Pointer fi, @u_int32_t long flags, Pointer data); }
    public interface _poll      { @Delegate void _poll(String path, Pointer fi, Pointer ph, Pointer reventsp); }
    public interface _write_buf { @Delegate void _write_buf(String path, Pointer buf, @off_t long off, Pointer fi); }
    public interface _read_buf  { @Delegate void _read_buf(String path, Pointer bufp, @size_t long size, @off_t long off, Pointer fi); }
    public interface _flock     { @Delegate void _flock(String path, Pointer fi, int op); }
    public interface _fallocate { @Delegate void _fallocate(String path, int mode, @off_t long off, @off_t long length, Pointer fi); }

    private StructFuseOperationsIfaces() {
    }
}
