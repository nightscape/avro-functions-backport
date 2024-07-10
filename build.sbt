val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.3.2")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.apache.spark" %% "spark-avro" % sparkVersion,
    "org.scalameta" %% "munit" % "1.0.0" % Test,
    "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.4" % Test,
)
