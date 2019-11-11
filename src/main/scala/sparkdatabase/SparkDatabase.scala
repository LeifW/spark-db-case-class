package sparkdatabase

import org.apache.spark.sql.{DataFrame, Dataset}

trait SparkDatabase[A] {
  def save(writeDataset: (String, Dataset[_]) => Unit)(a: A): Unit
  def load(readDataset: String => DataFrame): A

}
