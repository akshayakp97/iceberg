/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark;

import java.util.Map;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

public class TestAwsGlueTable {
    Logger LOG = LoggerFactory.getLogger(TestAwsGlueTable.class);
//  static final AwsClientFactory clientFactory = AwsClientFactories.defaultFactory();
//  static final GlueCatalog glueCatalog = new GlueCatalog();
//  static final GlueClient glue = clientFactory.glue();

//  @Test
//  public void testGlueTableScanFiles() {
//    SparkSession sparkSession =
//            SparkSession.builder()
//                    .master("local[2]")
//                    .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
//                    .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
//                    .config("spark.driver.bindAddress", "127.0.0.1")
//                    .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
//                    .config("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
//                    .config("spark.sql.catalog.spark_catalog.warehouse",  "s3://apt-736810617217-us-east-1/iceberg/glue_warehouse/")
//                    .config("spark_catalog", SparkSessionCatalog.class.getName())
//                    .config("spark.jars", "/Volumes/workplace/KeplerSparkApplication/src/KeplerSparkApplicationDependencies/lib/aws-glue-datacatalog-spark-client-3.6.0.jar")
//                    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
//                    .enableHiveSupport()
//                    .getOrCreate();
//
//    sparkSession.sql("use spark_catalog.tpcds_3000_iceberg_parq");
//    String str = sparkSession.conf().get("hive.metastore.client.factory.class");
//    sparkSession.sql("select * from spark_catalog.tpcds_3000_iceberg_parq.iceberg_parq_200mb_file_size_64_mb_row_group");
//  }

    @Test
    public void test() {
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                .config("spark.sql.defaultUrlStreamHandlerFactory.enabled", "false")
                .getOrCreate();
        spark.sql("select * from tpcds_3000_iceberg_parq.iceberg_parquet_large_file_size_small_row_group").show(1179898);
    }

}