scalaVersion := "2.12.8"

libraryDependencies += "com.chuusai" %% "shapeless" % "2.3.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0-RC3" % "test"

/*
Test / testOptions += Tests.Cleanup { loader => 
                        val sparkSessionClass = loader.loadClass("org.apache.spark.sql.SparkSession")
                        val sparkSession = sparkSessionClass.getMethod("active").invoke(null)
                        sparkSessionClass.getMethod("stop").invoke(sparkSession)
                      }
*/
