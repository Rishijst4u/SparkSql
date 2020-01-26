package com;

import com.model.TestPojo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

public class SparkSqlMainApplication {
    public static void main(String arg[]){

        String wareHouseDir="C:///tmp/hive";
        SparkSession sparkSession =SparkSession.builder().master("local[*]").appName("Spark Sql Application")
                .config("spark.sql.warehouse.dir",wareHouseDir)
                .config("hive.exec.dynamic.partition.mode","nonstrict")
                .enableHiveSupport()
                .getOrCreate();

        JavaSparkContext jsc= JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        //Creating a Rdd of int from list of int
        JavaRDD listData=jsc.parallelize(getList());

        //Creating TestPojo rdd from generic rdd
        JavaRDD<TestPojo> testPojoRdd=listData.map( x -> createTestPojo(x));

        //Converting java rdd of testpojo to dataset
        Dataset<TestPojo> testPojoDataset=sparkSession.createDataset(testPojoRdd.rdd(), Encoders.bean(TestPojo.class));

        //displaying top 20 results
        testPojoDataset.show(20,false);

        //DAG of DataFrame
        testPojoDataset.explain();

        //Storing testpojo data into hive table
        storeDataInHive(testPojoDataset);

        //retriving stored data from table again
        sparkSession.sql("select * from test.test_data limit 20").show(20,false);



    }

    public static List<Integer> getList(){
        List <Integer> data=new ArrayList<Integer>();
        int i=0;
        for(;i<100000;i++)
            data.add(i);
        return data;
    }

  public static TestPojo createTestPojo(Object data){
        int id=Integer.parseInt(data+"");
        int partitioncol=id%3;
        TestPojo testPojo=new TestPojo(id,partitioncol);
        return testPojo;
    }

    public static boolean storeDataInHive(Dataset<TestPojo> testPojoDataset){
        try{
            testPojoDataset.repartition(3).write().mode(SaveMode.Append).partitionBy("partitioncol").saveAsTable("test.test_data");
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
}
