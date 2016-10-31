package com.mapr.db.samples.basic;

import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Ex00Maprfs {


	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "maprfs://bbs-ocosrv-q001.bbs.aphp.fr:7222");
			FileSystem fs = FileSystem.get(conf);
			Path file2Read = new Path("/user/mapr/test.csv");
			FSDataInputStream istr = fs.open(file2Read);
			Scanner scan = new Scanner(istr);
			while (scan.hasNext()) {
				String line = scan.nextLine();
				System.out.println(line);
			}
			scan.close();
			istr.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
