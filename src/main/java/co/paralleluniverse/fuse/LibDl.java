package co.paralleluniverse.fuse;

public interface LibDl {
    int RTLD_LAZY = 0x1;
    int RTLD_NOW = 0x2;
    int RTLD_GLOBAL = 0x100;

    void dlopen(String file, int mode);
}
