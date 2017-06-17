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
    if (args.length < 4) {
      System.err.println("Usage: ClientBench ha|spec ls|mkdir|rmdir NOPS SLEEPTIME [prefix]");
      return;
    }

    if (args[0].equals("ha")) {
      service = new HAClient(new HdfsConfiguration());
    } else if (args[0].equals("spec")) {
      service = new SpecClient();
    } else {
      System.err.println("Usage: ClientBench ha|spec ls|mkdir|rmdir NOPS SLEEPTIME [prefix]");
      return;
    }

    if (args[1].equals("ls") && args.length == 4) {
      testLs(args[2], args[3]);
    } else if (args[1].equals("mkdir") && args.length == 5) {
      testMkdir(args[2], args[3], args[4]);
    } else if (args[1].equals("rmdir") && args.length == 5) {
      testRmdir(args[2], args[3], args[4]);
    } else {
      System.err.println("Usage: ClientBench ha|spec ls|mkdir|rmdir NOPS SLEEPTIME [prefix]");
    }
  }

  public static void testLs(String nops, String sleepTime) throws IOException {
    for (int i = 0; i < Integer.parseInt(nops); i++) {
      long before = System.nanoTime();
      service.getListing("/", new byte[0], false);
      long after = System.nanoTime();
      System.out.println((after - before) / 1000);
      try {
        long t = Long.parseLong(sleepTime);
        if (t > 0)
          Thread.sleep(t);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void testMkdir(String nops, String sleepTime, String prefix) throws IOException {
    service.mkdirs("/" + prefix, FsPermission.getDefault(), false);
    for (int i = 10000; i < 10000 + Integer.parseInt(nops); i++) {
      long before = System.nanoTime();
      service.mkdirs("/" + prefix + "/" + i, FsPermission.getDefault(), false);
      long after = System.nanoTime();
      System.out.println((after - before) / 1000);
      try {
        long t = Long.parseLong(sleepTime);
        if (t > 0)
          Thread.sleep(t);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void testRmdir(String nops, String sleepTime, String prefix) throws IOException {
    for (int i = 10000; i < 10000 + Integer.parseInt(nops); i++) {
      long before = System.nanoTime();
      service.delete("/" + prefix + "/" + i, false);
      long after = System.nanoTime();
      System.out.println((after - before) / 1000);
      try {
        long t = Long.parseLong(sleepTime);
        if (t > 0)
          Thread.sleep(t);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
