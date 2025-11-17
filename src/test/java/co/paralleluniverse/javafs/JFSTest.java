/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package co.paralleluniverse.javafs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.common.jimfs.Jimfs;

import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assert_;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 *
 * @author pron
 */
public class JFSTest {

    @Test
    public void test() throws Exception {
        FileSystem fs = Jimfs.newFileSystem();
        try (DataOutputStream os = new DataOutputStream(Files.newOutputStream(fs.getPath("/jimfs.txt")))) {
            os.writeUTF("JIMFS");
        }

        Path mnt = Files.createTempDirectory("jfsmnt");
        try {
            JavaFS.mount(fs, mnt, false, false);

            // From this point on we use the old file IO
            File root = mnt.toFile();

            // verify that we are, in fact, in Jimfs
            try (DataInputStream is = new DataInputStream(Files.newInputStream(new File(root, "jimfs.txt").toPath()))) {
                assertEquals("JIMFS", is.readUTF());
            }

            try (DataOutputStream os = new DataOutputStream(Files.newOutputStream(new File(root, "a.txt").toPath()))) {
                os.writeUTF("hello!");
            }
            try (DataOutputStream os = new DataOutputStream(Files.newOutputStream(new File(root, "b.txt").toPath()))) {
                os.writeUTF("wha?");
            }
            try (DataOutputStream os = new DataOutputStream(Files.newOutputStream(new File(root, "c.txt").toPath()))) {
                os.writeUTF("goodbye!");
            }

            assert_().that(root.list()).asList().containsAtLeast("a.txt", "b.txt", "c.txt", "jimfs.txt");

            try (DataInputStream is = new DataInputStream(Files.newInputStream(new File(root, "a.txt").toPath()))) {
                assertEquals("hello!", is.readUTF());
            }
            try (DataInputStream is = new DataInputStream(Files.newInputStream(new File(root, "b.txt").toPath()))) {
                assertEquals("wha?", is.readUTF());
            }
            try (DataInputStream is = new DataInputStream(Files.newInputStream(new File(root, "c.txt").toPath()))) {
                assertEquals("goodbye!", is.readUTF());
            }
        } finally {
            JavaFS.unmount(mnt);
            Files.delete(mnt);
        }
    }
}
