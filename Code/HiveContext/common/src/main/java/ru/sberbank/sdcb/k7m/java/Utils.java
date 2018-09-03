package ru.sberbank.sdcb.k7m.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.util.Arrays;
import java.io.*;

public class Utils {

    private static final int BUFFER = 2 * 1024;

    /** Copy all files in a directory to one output file (merge). */
    public static boolean copyMerge(FileSystem srcFS, Path srcDir,
                                    FileSystem dstFS, Path dstFile,
                                    boolean deleteSource,
                                    Configuration conf,
                                    String addString,
                                    String header) throws IOException {
        dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);

        if (!srcFS.getFileStatus(srcDir).isDirectory())
            return false;

        OutputStream out = dstFS.create(dstFile);

        try {
            if (header != null)
                out.write(header.concat(System.lineSeparator()).getBytes("UTF-8"));

            FileStatus contents[] = srcFS.listStatus(srcDir);
            Arrays.sort(contents);
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    InputStream in = srcFS.open(contents[i].getPath());
                    try {
                        IOUtils.copyBytes(in, out, conf, false);
                        if (addString!=null)
                            out.write(addString.getBytes("UTF-8"));

                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            out.close();
        }


        if (deleteSource) {
            return srcFS.delete(srcDir, true);
        } else {
            return true;
        }
    }

    private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
                                  boolean overwrite) throws IOException {
        if (dstFS.exists(dst)) {
            FileStatus sdst = dstFS.getFileStatus(dst);
            if (sdst.isDirectory()) {
                if (null == srcName) {
                    throw new IOException("Target " + dst + " is a directory");
                }
                return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
            } else if (!overwrite) {
                throw new IOException("Target " + dst + " already exists");
            }
        }
        return dst;
    }

    public static void encodeFile(FileSystem fs, Path file,
                                String srcCharset, String tgtCharset) throws IOException {

        if (fs.getFileStatus(file).isDirectory()) {
            throw new IOException(file + " is a directory");
        }

        try (InputStream in = fs.open(file);
             Reader reader = new InputStreamReader(in, srcCharset);
             OutputStream out = fs.create(file.suffix(".tmp"));
             Writer writer = new OutputStreamWriter(out, tgtCharset)) {
                char[] buffer = new char[BUFFER];
                int read;
                while ((read = reader.read(buffer)) != -1) {
                    writer.write(buffer, 0, read);
                }

                fs.delete(file, false);
                fs.rename(file.suffix(".tmp"), file);
        }
    }

    public static void encodeFile(FileSystem fs, Path file, String tgtCharset) throws IOException {
        encodeFile(fs, file, "UTF-8", tgtCharset);
    }

}
