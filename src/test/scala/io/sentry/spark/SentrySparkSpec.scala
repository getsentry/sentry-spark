// package io.sentry.spark;

// import org.scalatest.FlatSpec

// import org.apache.spark.{SparkContext, SparkConf};

// import io.sentry.Sentry;
// import io.sentry.spark._;

// class SentrySparkSpec extends FlatSpec {
//   it should "set tags" in {
//     val config = new SparkConf().setAppName("ExampleApp").setMaster("local")
//     val sparkContext = new SparkContext(config);
//     SentrySpark.applyContext(sparkContext);

//     println(Sentry.getContext().getTags())
//   }
// }
