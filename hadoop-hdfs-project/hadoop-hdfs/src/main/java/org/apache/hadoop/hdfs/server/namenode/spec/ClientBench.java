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
      System.err.println("Usage: ClientBench ha|spec ls|mkdir|rmdir NOPS [prefix]");
      return;
    }

    if (args[0].equals("ha")) {
      service = new HAClient(new HdfsConfiguration());
    } else if (args[0].equals("spec")) {
      service = new SpecClient();
    } else {
      System.err.println("Usage: ClientBench ha|spec ls|mkdir|rmdir NOPS [prefix]");
      return;
    }

    if (args[1].equals("ls") && args.length == 3) {
      testLs(args[2]);
    } else if (args[1].equals("mkdir") && args.length == 4) {
      testMkdir(args[2], args[3]);
    } else if (args[1].equals("rmdir") && args.length == 4) {
      testRmdir(args[2], args[3]);
    } else {
      System.err.println("Usage: ClientBench ha|spec ls|mkdir|rmdir NOPS [prefix]");
    }
  }

  public static void testLs(String nops) throws IOException {
    long last = System.nanoTime();
    for (int i = 0; i < Integer.parseInt(nops); i++) {
      service.getListing("/", new byte[0], false);
      long current = System.nanoTime();
      System.out.println((current - last) / 1000);
      last = current;
    }
  }

  public static void testMkdir(String nops, String prefix) throws IOException {
    service.mkdirs("/" + prefix, FsPermission.getDefault(), false);

    long last = System.nanoTime();
    for (int i = 10000; i < 10000 + Integer.parseInt(nops); i++) {
      service.mkdirs("/" + prefix + "/" + i, FsPermission.getDefault(), false);
      long current = System.nanoTime();
      System.out.println((current - last) / 1000);
      last = current;
    }
  }

  public static void testRmdir(String nops, String prefix) throws IOException {
    long last = System.nanoTime();
    for (int i = 10000; i < 10000 + Integer.parseInt(nops); i++) {
      service.delete("/" + prefix + "/" + i, false);
      long current = System.nanoTime();
      System.out.println((current - last) / 1000);
      last = current;
    }
  }
}
