package sparkdatabase

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait SparkDatabase[A] {
  def save(writeDataset: (Dataset[_], String) => Unit)(a: A): Unit
  def load(readDataset: String => DataFrame): A
}

class SparkTables[A](implicit sdb: SparkDatabase[A]) {
  def save(a: A): Unit = sdb.save(_.write.saveAsTable(_))(a)
  def load(spark: SparkSession): A = sdb.load(spark.read.table)
}

object SparkDatabase {
  def apply[A](implicit sdb: SparkDatabase[A]): SparkDatabase[A] = sdb
  def withDefaultTables[A: SparkDatabase] = new SparkTables[A]
}

