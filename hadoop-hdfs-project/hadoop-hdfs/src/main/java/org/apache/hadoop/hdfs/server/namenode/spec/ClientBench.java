package org.apache.hadoop.hdfs.server.namenode.spec;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

import java.io.IOException;

/**
 * Created by aolx on 6/14/17.
 */
public class ClientBench {
  static ClientProtocol service;

  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.err.println("Usage: " + args[0] + " ha|spec ls|mkdir|rmdir [prefix]");
      return;
    }

    if (args[1].equals("ha")) {
      service = new HAClient(new HdfsConfiguration());
    } else if (args[1].equals("spec")) {
      service = new SpecClient();
    } else {
      System.err.println("Usage: " + args[0] + " ha|spec ls|mkdir|rmdir [prefix]");
      return;
    }

    if (args[2].equals("ls")) {
      testLs();
    } else if (args[2].equals("mkdir") && args.length == 4) {
      testMkdir(args[3]);
    } else if (args[2].equals("rmdir") && args.length == 4) {
      testRmdir(args[3]);
    } else {
      System.err.println("Usage: " + args[0] + " ha|spec ls|mkdir|rmdir [prefix]");
    }
  }

  public static void testLs() throws IOException {
    long last = System.nanoTime();
    for (int i = 0; i < 10000; i++) {
      service.getListing("/", new byte[0], false);
      System.out.println((System.nanoTime() - last) / 1000);
      last = System.nanoTime();
    }
  }

  public static void testMkdir(String prefix) throws IOException {
    service.mkdirs("/" + prefix, FsPermission.getDefault(), false);

    long last = System.nanoTime();
    for (int i = 10000; i < 20000; i++) {
      service.mkdirs("/" + prefix + "/" + i, FsPermission.getDefault(), false);
      System.out.println((System.nanoTime() - last) / 1000);
      last = System.nanoTime();
    }
  }

  public static void testRmdir(String prefix) throws IOException {
    long last = System.nanoTime();
    for (int i = 10000; i < 20000; i++) {
      service.delete("/" + prefix + "/" + i, false);
      System.out.println((System.nanoTime() - last) / 1000);
      last = System.nanoTime();
    }
  }
}
