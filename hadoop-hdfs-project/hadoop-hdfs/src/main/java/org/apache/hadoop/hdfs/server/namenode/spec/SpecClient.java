package org.apache.hadoop.hdfs.server.namenode.spec;

import com.google.protobuf.ByteString;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
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
import org.apache.commons.codec.binary.Base64;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import static org.apache.hadoop.hdfs.server.namenode.spec.ReplicaUpcall.Request.Operation.LS;
import static org.apache.hadoop.hdfs.server.namenode.spec.ReplicaUpcall.Request.Operation.MKDIR;
import static org.apache.hadoop.hdfs.server.namenode.spec.ReplicaUpcall.Request.Operation.RM;

/**
 * Created by aolx on 2017/5/25.
 */
public class SpecClient implements ClientProtocol {
  final SpecServerCLib specServer;
  final String confPath;
  final Pointer clientPtr;
  public SpecClient() {
    specServer = (SpecServerCLib) Native.loadLibrary("specServer", SpecServerCLib.class);
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL resource = cl.getResource("quorum.config");
    confPath = resource.getPath();

    PointerByReference ppClient = new PointerByReference();
    specServer.newClientPtr(confPath, ppClient);
    clientPtr = ppClient.getValue();
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

  private String callClientClib(byte[] request) {
    PointerByReference ptrRep = new PointerByReference();
    specServer.runClient(clientPtr, Base64.encodeBase64String(request), ptrRep);
    final Pointer reply = ptrRep.getValue();
    return reply.getString(0);
  }

  @Override
  public boolean delete(String src, boolean recursive) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
    ReplicaUpcall.Request.Builder req = ReplicaUpcall.Request.newBuilder().setOp(RM).setSrc(src).
        setRecursive(recursive);
    String result = callClientClib(req.build().toByteArray());
    ReplicaUpcall.Reply.Builder repBuilder = ReplicaUpcall.Reply.newBuilder();
    byte[] bytes = Base64.decodeBase64(result);
    ReplicaUpcall.Reply reply = repBuilder.mergeFrom(bytes).build();
    if (reply.hasException()) {
      throw new IOException(reply.getException());
    }
    return reply.getSuccess();
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
    ReplicaUpcall.Request.Builder req = ReplicaUpcall.Request.newBuilder().setOp(MKDIR).setSrc(src).
        setMasked(masked.toShort()).setCreateParent(createParent);
    String result = callClientClib(req.build().toByteArray());
    ReplicaUpcall.Reply.Builder repBuilder = ReplicaUpcall.Reply.newBuilder();
    byte[] bytes = Base64.decodeBase64(result);
    ReplicaUpcall.Reply reply = repBuilder.mergeFrom(bytes).build();
    if (reply.hasException()) {
      throw new IOException(reply.getException());
    }
    return reply.getSuccess();
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter, boolean needLocation) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    ReplicaUpcall.Request.Builder req = ReplicaUpcall.Request.newBuilder().setOp(LS).setSrc(src).
        setStartAfter(ByteString.copyFrom(startAfter)).setNeedLocation(needLocation);
    String result = callClientClib(req.build().toByteArray());
    ReplicaUpcall.Reply.Builder repBuilder = ReplicaUpcall.Reply.newBuilder();
    byte[] bytes = Base64.decodeBase64(result);
    ReplicaUpcall.Reply reply = repBuilder.mergeFrom(bytes).build();
    if (reply.hasException()) {
      throw new IOException(reply.getException());
    }
    ReplicaUpcall.DirectoryListing listing = reply.getDirectoryListing();
    HdfsFileStatus[] allStates = new HdfsFileStatus[listing.getPartialListingCount()];
    for (int i = 0; i < allStates.length; i++) {
      ReplicaUpcall.HdfsFileStatus l = listing.getPartialListing(i);
      allStates[i] = new HdfsFileStatus(l.getLength(),l.getFileType()==ReplicaUpcall.HdfsFileStatus.FileType.IS_DIR,
          l.getBlockReplication(),l.getBlocksize(),l.getModificationTime(),l.getAccessTime(),new FsPermission((short) l.getPermission()),
          l.getOwner(),l.getGroup(),l.getSymlink().toByteArray(),l.getPath().toByteArray(),l.getFileId(),l.getChildrenNum());
    }
    DirectoryListing dl = new DirectoryListing(allStates, listing.getRemainingEntries());
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

  public static void main(String[] args) throws IOException, InterruptedException {
    SpecClient client = new SpecClient();
    System.out.println("starting mkdir ");
    System.out.println(client.mkdirs("/mkdirtest", FsPermission.getDefault(), true));
    System.out.println("starting complete ");
    Thread.sleep(2000);
    DirectoryListing result = client.getListing("/", new byte[0], false);
    /*
    SpecClient client = new SpecClient();
    int mkdirN = Integer.parseInt(args[1]);
    String clientIndex = args[2];
    System.out.println("starting mkdir " + clientIndex);
    long start = System.nanoTime();
    for(int i = 0; i < mkdirN; i++) {
      System.out.println(client.mkdirs("/mkdirtest" + clientIndex + "_" + i, FsPermission.getDefault(), true));
    }
    long end = System.nanoTime();
    System.out.println("mkdir complete " + clientIndex);
    System.out.println("mkdir time elapse: " + Long.toString(end - start));

    start = System.nanoTime();
    DirectoryListing result = client.getListing("/", new byte[0], false);
    end = System.nanoTime();
    System.out.println("LS time elapse: " + Long.toString(end - start));
    for (HdfsFileStatus s: result.getPartialListing()) {
      System.out.println(s.getLocalName() + " " + s.getOwner() + " " + s.getModificationTime());
    }*/
  }
}
