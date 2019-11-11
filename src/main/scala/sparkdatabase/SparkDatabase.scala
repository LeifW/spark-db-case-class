package sparkdatabase

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait SparkDatabase[A] {
  def save(writeDataset: (String, Dataset[_]) => Unit)(a: A): Unit
  def load(readDataset: (String, SparkSession) => DataFrame)(spark: SparkSession):

}
