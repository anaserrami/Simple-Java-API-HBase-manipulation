package com.errami;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class App {
    public static final String TABLE_NAME = "Students";
    public static final String CF_INFO = "info";
    public static final String CF_GRADES = "grades";

    public static void display(Result result) {
        for (Cell cell : result.rawCells()) {
            byte[] cf = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);

            String cfString = Bytes.toString(cf);
            String qualifierString = Bytes.toString(qualifier);
            String valueString = Bytes.toString(value);

            System.out.println("Column Family: " + cfString +
                    ", Column: " + qualifierString +
                    ", Value: " + valueString);
        }
    }

    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "zookeeper");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "hbase-master:16000");

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);

            Admin admin = connection.getAdmin();

            TableName tableName = TableName.valueOf(TABLE_NAME);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_INFO));
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_GRADES));

            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

            if (!admin.tableExists(tableName)) {
                admin.createTable(tableDescriptor);
                System.out.println("Table created !");
            } else {
                System.err.println("Table already exists !");
            }

            Table table = connection.getTable(tableName);

            // Add student1
            Put putStudent1 = new Put(Bytes.toBytes("student1"));
            putStudent1.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
            putStudent1.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("20"));
            putStudent1.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("B"));
            putStudent1.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"), Bytes.toBytes("A"));
            table.put(putStudent1);
            System.out.println("Student1 added !");

            // Add student2
            Put putStudent2 = new Put(Bytes.toBytes("student2"));
            putStudent2.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"), Bytes.toBytes("Jane Smith"));
            putStudent2.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("22"));
            putStudent2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("A"));
            putStudent2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"), Bytes.toBytes("A"));
            table.put(putStudent2);
            System.out.println("Student2 added !");

            // Retrieve and display information for student1
            Get getStudent1 = new Get(Bytes.toBytes("student1"));
            Result resultStudent1 = table.get(getStudent1);
            display(resultStudent1);

            // Update age of Jane Smith to 23 and math grade to A+
            Put putUpdateJane = new Put(Bytes.toBytes("student2"));
            putUpdateJane.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("23"));
            putUpdateJane.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("A+"));
            table.put(putUpdateJane);
            System.out.println("Jane's age and math grade updated !");

            // Delete student1
            Delete deleteStudent1 = new Delete(Bytes.toBytes("student1"));
            table.delete(deleteStudent1);
            System.out.println("Student1 deleted !");

            // Display information for all students
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                display(result);
            }
            scanner.close();

            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table deleted !");
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}