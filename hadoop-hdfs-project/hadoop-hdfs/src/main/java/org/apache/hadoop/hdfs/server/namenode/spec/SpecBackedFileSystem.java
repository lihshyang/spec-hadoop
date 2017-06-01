package org.apache.hadoop.hdfs.server.namenode.spec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  static final Log LOG = LogFactory.getLog(SpecBackedFileSystem.class);

  private final SpecClient client;

  public SpecBackedFileSystem() {
    this.client = new SpecClient();
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

//    return client.getListing(f.toString());
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
    LOG.debug("mkdir: "+f.toString());
    return client.mkdirs(f.toString(), permission, true);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return null;
  }
}
