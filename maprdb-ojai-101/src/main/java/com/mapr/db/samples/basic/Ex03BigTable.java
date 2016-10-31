/*
 *  Copyright 2009-2016 MapR Technologies
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mapr.db.samples.basic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;



/**
 * This class shows the basic operations of MapR DB
 */
public class Ex03BigTable {

  public static final String TABLE_PATH = "/user/mapr/mapr_ojai_toto";

  private Table table;


  public Ex03BigTable() {
  }

  public static void main(String[] args) throws Exception {

    Ex03BigTable app = new Ex03BigTable();
    app.run();

  }

  private void run() throws Exception {

//    this.deleteTable(TABLE_PATH);
    this.table = this.getTable(TABLE_PATH);
    this.printTableInformation(TABLE_PATH);
//
//    System.out.println("\n\n========== INSERT NEW RECORDS ==========");
//    System.out.println(new Date());
//    this.createDocuments();
//    System.out.println("\n\n========== DONE ==========");
//    System.out.println(new Date());

    System.out.println("\n\n========== QUERIES ==========");
    this.queryDocuments();

    System.out.println(new Date());
    this.table.close();

  }

  /**
   * Get the table, create it if not present
   *
   * @throws IOException
   */
  private Table getTable(String tableName) throws IOException {
    Table table;

    if (!MapRDB.tableExists(tableName)) {
      table = MapRDB.createTable(tableName); // Create the table if not already present
    } else {
      table = MapRDB.getTable(tableName); // get the table
    }
    return table;
  }

  private void deleteTable(String tableName) throws IOException {
    if (MapRDB.tableExists(tableName)) {
      MapRDB.deleteTable(tableName);
    }

  }

  /**
   *
   */
  private void createDocuments() throws IOException {

	try {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "maprfs://bbs-ocosrv-q001.bbs.aphp.fr:7222");
		FileSystem fs = FileSystem.get(conf);
		Path file2Read = new Path("/user/hive/warehouse/orc_toto/toto.csv");
		FSDataInputStream istr = fs.open(file2Read);
		Scanner scan = new Scanner(istr);
		int i = 0;
		while (scan.hasNext()) {
			String line = scan.nextLine();
			String[] fields = line.split(",");
		    // Create a new document (simple format)
		    Document document = MapRDB.newDocument()
		      .set("_id", fields[0])
		      .set("value", fields[1]);

		    // save document into the table
		    table.insertOrReplace(document);
		    i++;
		    i = i % 10000;
		    if (i == 0) {
		        table.flush(); // flush to the server
		    }
		}
		scan.close();
		istr.close();
	}
	catch (Exception e) {
		e.printStackTrace();
	}
  }


  /**
   * Query the record
   */
  private void queryDocuments() throws Exception {

//    {
//      // get a single document
//      Document record = table.findById("mdupont");
//      System.out.print("Single record\n\t");
//      System.out.println(record);
//
//      //print individual fields
//      System.out.println("Id : " + record.getIdString() + " - first name : " + record.getString("first_name"));
//    }
//
//    {
//      // get a single document
//      Document record = table.findById("mdupont", "last_name");
//      System.out.print("Single record with projection\n\t");
//      System.out.println(record);
//
//      //print individual fields
//      System.out.println("Id : " + record.getIdString() + " - first name : " + record.getString("first_name"));
//
//    }
//
//    {
//      // get single document and map it to the bean
//      User user = table.findById("alehmann").toJavaBean( User.class );
//      System.out.println("User Pojo from document : "+ user.toString());
//    }
//
//    {
//      // all recordss in the table
//      System.out.println("\n\nAll records");
//      DocumentStream rs = table.find();
//      Iterator<Document> itrs = rs.iterator();
//      Document readRecord;
//      while (itrs.hasNext()) {
//        readRecord = itrs.next();
//        System.out.println("\t" + readRecord);
//      }
//      rs.close();
//    }
//

//    {
//      // all records in the table with projection
//      System.out.println("\n\nAll records with projection");
//
//      try(DocumentStream documentStream = table.find("first_name", "last_name")) {
//        for (Document doc : documentStream ) {
//          System.out.println("\t" + doc);
//        }
//      }
//    }


//    {
//      // all records and use a POJO
//      // it is interesting to see how you can ignore unknown attributes with the JSON Annotations
//      System.out.println("\n\nAll records with a POJO");
//
//      try(DocumentStream documentStream = table.find()) {
//        for (Document doc : documentStream ) {
//          System.out.println("\t" + doc.toJavaBean(User.class));
//        }
//      }
//
//    }


    {
      // find with key condition
      System.out.println("\n\nFind with key condition");
      System.out.println(new Date());
      System.out.println("\n\n");

      // Condition equals a string
      QueryCondition condition = MapRDB.newCondition()
    	.and()
        .is("_id", QueryCondition.Op.GREATER_OR_EQUAL, "AE")
        .is("_id", QueryCondition.Op.LESS, "AF")
        .close()
        .build();
      System.out.println("\n\nCondition: " + condition);
      try(DocumentStream documentStream = table.find(condition)) {
        for (Document doc : documentStream ) {
          System.out.println("\t" + doc);
        }
      }
      System.out.println("\n\nFind with key condition done");
      System.out.println(new Date());
      System.out.println("\n\n");
    }

    {
        // find count with key condition
        System.out.println("\n\nFind count with key condition");
        System.out.println(new Date());
        System.out.println("\n\n");

        int counter = 0;
        // Condition equals a string
        QueryCondition condition = MapRDB.newCondition()
      	.and()
          .is("_id", QueryCondition.Op.GREATER_OR_EQUAL, "AE")
          .is("_id", QueryCondition.Op.LESS, "AF")
          .close()
          .build();
        System.out.println("\n\nCondition: " + condition);
        try(DocumentStream documentStream = table.find(condition)) {
          for (Document doc : documentStream ) {
        	  counter++;
          }
        }
        System.out.println("result: "+counter+" row(s)");
        System.out.println("\n\nFind count with key condition done");
        System.out.println(new Date());
        System.out.println("\n\n");
      }

    {
        // find with value condition
        System.out.println("\n\nFind with value condition");
        System.out.println(new Date());
        System.out.println("\n\n");

        // Condition equals a string
        QueryCondition condition = MapRDB.newCondition()
      	.and()
          .is("value", QueryCondition.Op.GREATER_OR_EQUAL, "AE")
          .is("value", QueryCondition.Op.LESS, "AF")
          .close()
          .build();
        System.out.println("\n\nCondition: " + condition);
        try(DocumentStream documentStream = table.find(condition)) {
          for (Document doc : documentStream ) {
            System.out.println("\t" + doc);
          }
        }
        System.out.println("\n\nFind with value condition done");
        System.out.println(new Date());
        System.out.println("\n\n");
      }

//    {
//      // Condition as date range
//      QueryCondition condition = MapRDB.newCondition()
//        .and()
//        .is("dob", GREATER_OR_EQUAL, ODate.parse("1980-01-01"))
//        .is("dob", LESS, ODate.parse("1981-01-01"))
//        .close()
//        .build();
//      System.out.println("\n\nCondition: " + condition);
//      try(DocumentStream documentStream = table.find(condition)) {
//        for (Document doc : documentStream ) {
//          System.out.println("\t" + doc);
//        }
//      }
//    }


//    {
//      // Condition in sub document
//      QueryCondition condition = MapRDB.newCondition()
//        .is("address.zip", EQUAL, 95109)
//        .build();
//      System.out.println("\n\nCondition: " + condition);
//      try(DocumentStream documentStream = table.find(condition)) {
//        for (Document doc : documentStream ) {
//          System.out.println("\t" + doc);
//        }
//      }
//    }


//    {
//      // Contains a specific value in an array
//      QueryCondition condition = MapRDB.newCondition()
//        .is("interests[]", EQUAL, "sports");
//
//      System.out.println("\n\nCondition: " + condition);
//      try(DocumentStream documentStream = table.find(condition)) {
//        for (Document doc : documentStream ) {
//          System.out.println("\t" + doc);
//        }
//      }
//    }

//    {
//      // Contains a value at a specific index
//      QueryCondition condition = MapRDB.newCondition()
//        .is("interests[0]", EQUAL, "sports")
//        .build();
//      System.out.println("\n\nCondition: " + condition);
//      try(DocumentStream documentStream = table.find(condition, "first_name", "last_name", "interests")) {
//        for (Document doc : documentStream ) {
//          System.out.println("\t" + doc);
//        }
//      }
//    }


  }


  /**
   * Print table information such as Name, Path and Tablets information (sharding)
   *
   * @param tableName    The table to describe
   * @throws IOException If anything goes wrong accessing the table
   */
  private void printTableInformation(String tableName) throws IOException {
    Table table = MapRDB.getTable(tableName);
    System.out.println("\n=============== TABLE INFO ===============");
    System.out.println(" Table Name : " + table.getName());
    System.out.println(" Table Path : " + table.getPath());
    System.out.println(" Table Infos : " + Arrays.toString(table.getTabletInfos()));
    System.out.println("==========================================\n");
  }


}
