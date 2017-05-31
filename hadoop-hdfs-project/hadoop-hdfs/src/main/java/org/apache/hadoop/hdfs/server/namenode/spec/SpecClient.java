package org.apache.hadoop.hdfs.server.namenode.spec;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;

import javax.sql.rowset.serial.SerialStruct;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

/**
 * Created by aolx on 2017/5/25.
 */
public class SpecClient implements ClientProtocol {
  final SpecServerCLib specServer;
  final String confPath;
  public SpecClient() {
    specServer = (SpecServerCLib) Native.loadLibrary("specServer", SpecServerCLib.class);
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL resource = cl.getResource("quorum.config");
    confPath = resource.getPath();
  }

  @Override
  public LocatedBlocks getBlockLocations(String src, long offset, long length) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return null;
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return null;
  }

  @Override
  public HdfsFileStatus create(String src, FsPermission masked, String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent, short replication, long blockSize) throws AccessControlException, AlreadyBeingCreatedException, DSQuotaExceededException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
    return null;
  }

  @Override
  public LocatedBlock append(String src, String clientName) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
    return null;
  }

  @Override
  public boolean setReplication(String src, short replication) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
    return false;
  }

  @Override
  public void setPermission(String src, FsPermission permission) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {

  }

  @Override
  public void setOwner(String src, String username, String groupname) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {

  }

  @Override
  public void abandonBlock(ExtendedBlock b, String src, String holder) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {

  }

  @Override
  public LocatedBlock addBlock(String src, String clientName, ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes) throws AccessControlException, FileNotFoundException, NotReplicatedYetException, SafeModeException, UnresolvedLinkException, IOException {
    return null;
  }

  @Override
  public LocatedBlock getAdditionalDatanode(String src, ExtendedBlock blk, DatanodeInfo[] existings, DatanodeInfo[] excludes, int numAdditionalNodes, String clientName) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
    return null;
  }

  @Override
  public boolean complete(String src, String clientName, ExtendedBlock last, long fileId) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
    return false;
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {

  }

  @Override
  public boolean rename(String src, String dst) throws UnresolvedLinkException, SnapshotAccessControlException, IOException {
    return false;
  }

  @Override
  public void concat(String trg, String[] srcs) throws IOException, UnresolvedLinkException, SnapshotAccessControlException {

  }

  @Override
  public void rename2(String src, String dst, Options.Rename... options) throws AccessControlException, DSQuotaExceededException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {

  }

  private String callClientClib(String request) {
    PointerByReference ptrRep = new PointerByReference();
    specServer.runClient(confPath, request, ptrRep);
    final Pointer reply = ptrRep.getValue();
    return reply.getString(0);
  }

  @Override
  public boolean delete(String src, boolean recursive) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
    ReplicaUpcall.Request.Builder req = ReplicaUpcall.Request.newBuilder().setSrc(src).
        setRecursive(recursive);
    String result = callClientClib(TextFormat.printToString(req));
    ReplicaUpcall.Reply.Builder repBuilder = ReplicaUpcall.Reply.newBuilder();
    TextFormat.merge(result, repBuilder);
    ReplicaUpcall.Reply reply = repBuilder.build();
    if (reply.hasException()) {
      throw new IOException(reply.getException());
    }
    return reply.getSuccess();
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
    ReplicaUpcall.Request.Builder req = ReplicaUpcall.Request.newBuilder().setSrc(src).
        setMasked(masked.toShort()).setCreateParent(createParent);
    String result = callClientClib(TextFormat.printToString(req));
    TextFormat.printToString(req);
    ReplicaUpcall.Reply.Builder repBuilder = ReplicaUpcall.Reply.newBuilder();
    TextFormat.merge(result, repBuilder);
    ReplicaUpcall.Reply reply = repBuilder.build();
    if (reply.hasException()) {
      throw new IOException(reply.getException());
    }
    return reply.getSuccess();
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter, boolean needLocation) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    ReplicaUpcall.Request.Builder req = ReplicaUpcall.Request.newBuilder().setSrc(src).
        setStartAfter(ByteString.copyFrom(startAfter)).setNeedLocation(needLocation);
    String result = callClientClib(TextFormat.printToString(req));
    TextFormat.printToString(req);
    ReplicaUpcall.Reply.Builder repBuilder = ReplicaUpcall.Reply.newBuilder();
    TextFormat.merge(result, repBuilder);
    ReplicaUpcall.Reply reply = repBuilder.build();
    if (reply.hasException()) {
      throw new IOException(reply.getException());
    }
    DirectoryListing dl = (DirectoryListing) SerializationUtils.deserialize(reply.getDirectoryListing().toByteArray());
    for (HdfsFileStatus status: dl.getPartialListing()) {
      status.setPermission(new FsPermission(status.permissionInShort));
    }
    return dl;
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing() throws IOException {
    return new SnapshottableDirectoryStatus[0];
  }

  @Override
  public void renewLease(String clientName) throws AccessControlException, IOException {

  }

  @Override
  public boolean recoverLease(String src, String clientName) throws IOException {
    return false;
  }

  @Override
  public long[] getStats() throws IOException {
    return new long[0];
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type) throws IOException {
    return new DatanodeInfo[0];
  }

  @Override
  public long getPreferredBlockSize(String filename) throws IOException, UnresolvedLinkException {
    return 0;
  }

  @Override
  public boolean setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked) throws IOException {
    return false;
  }

  @Override
  public void saveNamespace() throws AccessControlException, IOException {

  }

  @Override
  public long rollEdits() throws AccessControlException, IOException {
    return 0;
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws AccessControlException, IOException {
    return false;
  }

  @Override
  public void refreshNodes() throws IOException {

  }

  @Override
  public void finalizeUpgrade() throws IOException {

  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie) throws IOException {
    return null;
  }

  @Override
  public void metaSave(String filename) throws IOException {

  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {

  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return null;
  }

  @Override
  public boolean isFileClosed(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return false;
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src) throws AccessControlException, UnresolvedLinkException, IOException {
    return null;
  }

  @Override
  public ContentSummary getContentSummary(String path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return null;
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, SnapshotAccessControlException, IOException {

  }

  @Override
  public void fsync(String src, String client, long lastBlockLength) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {

  }

  @Override
  public void setTimes(String src, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, SnapshotAccessControlException, IOException {

  }

  @Override
  public void createSymlink(String target, String link, FsPermission dirPerm, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {

  }

  @Override
  public String getLinkTarget(String path) throws AccessControlException, FileNotFoundException, IOException {
    return null;
  }

  @Override
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName) throws IOException {
    return null;
  }

  @Override
  public void updatePipeline(String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock, DatanodeID[] newNodes) throws IOException {

  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
    return null;
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    return 0;
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {

  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    return null;
  }

  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName) throws IOException {
    return null;
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName) throws IOException {

  }

  @Override
  public void renameSnapshot(String snapshotRoot, String snapshotOldName, String snapshotNewName) throws IOException {

  }

  @Override
  public void allowSnapshot(String snapshotRoot) throws IOException {

  }

  @Override
  public void disallowSnapshot(String snapshotRoot) throws IOException {

  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot, String fromSnapshot, String toSnapshot) throws IOException {
    return null;
  }
}
