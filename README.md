# Sentry for Apache Spark

A Sentry Integration for Scala Spark.

This integration is in alpha and has an unstable API.

Supports Spark `2.x.x` and above.

Interested in PySpark? Check out our [PySpark integration](https://docs.sentry.io/platforms/python/pyspark/).

## Installation

Add the package as a library dependecy.

```scala
libraryDependencies += "io.sentry" %% "sentry-spark" % "0.0.1-alpha05"
```

Make sure to [configure the Sentry SDK](https://docs.sentry.io/clients/java/config/#id2).

We recommend using a `sentry.properties` file and place it in `<SPARK_HOME>/conf`, or anywhere in the Spark Driver's classpath.

When using cluster mode, we recommend the `--files` spark-submit option.

In order to use the integration, you will need to make the jar accesible to your Spark Driver.

```bash
SCALA_VERSION="2.11" # or "2.12"
./bin/spark-submit \
    --jars "sentry-spark_$SCALA_VERSION-0.0.1-alpha05.jar" \
    --files "sentry.properties" \
    example-spark-job.jar
```

## Usage

### Basic Usage

The `sentry-spark` integration will automatically add tags and other metadata to your Sentry events. You can set it up like this:

```scala
import io.sentry.Sentry
import io.sentry.spark.SentrySpark;

...

Sentry.init();

val spark = SparkSession
    .builder
    .appName("Simple Application")
    .getOrCreate();

SentrySpark.applyContext(spark);
```

`SentrySpark.applyContext` can take a `SparkSession`, `SparkContext` or `StreamingContext`.

### Listeners

The `sentry-spark` integration exposes custom listeners that allow you to report events and errors to Sentry.

Supply the listeners as configuration properties so that they get instantiated as soon as possible.

#### SentrySparkListener [Spark Core]

The `SentrySparkListener` hooks onto the spark scheduler and adds breadcrumbs, tags and reports errors accordingly.

```scala
// Using SparkSession
val spark = SparkSession
    .builder
    .appName("Simple Application")
    .config("spark.extraListeners", "io.sentry.spark.listener.SentrySparkListener")
    .getOrCreate()

// Using SparkContext
val conf = new SparkConf()
    .setAppName("Simple Application")
    .setMaster("local[2]")
    .config("spark.extraListeners", "io.sentry.spark.listener.SentrySparkListener")
val sc  = new SparkContext(conf)
```

#### SentryQueryExecutionListener [Spark SQL]

The `SentryQueryExecutionListener` listens for query events and reports failures as Sentry errors. 

The configuration option `spark.sql.queryExecutionListeners` is only supported for Spark 2.3 and above.

```scala
val spark = SparkSession
    .builder
    .appName("Simple Spark SQL application")
    .config("spark.sql.queryExecutionListeners", "io.sentry.spark.listener.SentryQueryExecutionListener")
    .getOrCreate()
```

#### SentryStreamingQueryListener [Spark SQL]

The `SentryStreamingQueryListener` listens for streaming queries and reports failures as Sentry errors. 

```scala
val spark = SparkSession
    .builder
    .appName("Simple SQL Streaming Application")
    .config("spark.sql.streaming.streamingQueryListeners", "io.sentry.spark.listener.SentryStreamingQueryListener")
    .getOrCreate();
```

#### SentryStreamingListener [Spark Streaming]

The `SentryStreamingListener` listens for ongoing streaming computations and adds breadcrumbs, tags and reports errors accordingly.

```scala
import io.sentry.spark.listener.SentryStreamingListener;

val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
val ssc = new StreamingContext(conf, Seconds(1))

ssc.addStreamingListener(new SentryStreamingListener);
```

## Development

Package the assets with

```bash
sbt package
```

To run tests

```bash
sbt test
```

To publish to bintray, make sure you have a valid GPG key and supply your bintray credentials.

For more info see [sbt-bintray](https://github.com/sbt/sbt-bintray) and [sbt-pgp](https://github.com/sbt/sbt-pgp)

```bash
sbt publishSigned
```

## Contributing

As this integration is under active work, reach out to us on our [Discord](https://discord.gg/ez5KZN7) if you are looking to get involved.
