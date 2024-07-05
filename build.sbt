libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.3.2",
    "org.apache.spark" %% "spark-hive" % "3.3.2",
    "org.apache.spark" %% "spark-avro" % "3.3.2",
    "org.scalameta" %% "munit" % "1.0.0" % Test,
    "com.sksamuel.avro4s" % "avro4s-core_2.12" % "2.0.4" % Test,
)
