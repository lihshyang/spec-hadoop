package org.apache.hadoop.hdfs.server.namenode.spec;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * Created by aolx on 5/30/17.
 */
public class SpecBackedFileSystem extends FileSystem {

  public SpecBackedFileSystem() {

  }

  @Override
  public URI getUri() {
    return null;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return null;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return false;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return false;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    return new FileStatus[0];
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {

  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return false;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return null;
  }
}
