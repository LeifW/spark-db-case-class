package sparkdatabase.generic

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import shapeless.labelled.{FieldType, field}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}
import sparkdatabase.SparkDatabase

trait SparkDatabaseGenericDerivation {
  implicit val hNilTable: SparkDatabase[HNil] = new SparkDatabase[HNil] {
    override def save(writeDataset: (String, Dataset[_]) => Unit)(a: HNil): Unit = ()
    override def load(readDataset: (String, SparkSession) => DataFrame)(spark: SparkSession): HNil = HNil
  }

  implicit def hConsTables[A: Encoder, K <: Symbol, T <: HList](implicit key: Witness.Aux[K], sdb: SparkDatabase[T]): SparkDatabase[FieldType[K, Dataset[A]] :: T] = new SparkDatabase[FieldType[K, Dataset[A]] :: T] {
    override def save(writeDataset: (String, Dataset[_]) => Unit)(a: FieldType[K, Dataset[A]] :: T): Unit = {
      writeDataset(key.value.name, a.head)
      sdb.save(writeDataset)(a.tail)
    }
    override def load(readDataset: (String, SparkSession) => DataFrame)(spark: SparkSession): FieldType[K, Dataset[A]] :: T =
     field[K](readDataset(key.value.name, spark).as[A]) :: sdb.load(readDataset)(spark)
  }

  implicit def genericTables[A, Repr](implicit gen: LabelledGeneric.Aux[A, Repr], dbRepr: Lazy[SparkDatabase[Repr]]): SparkDatabase[A] = new SparkDatabase[A] {
    override def save(writeDataset: (String, Dataset[_]) => Unit)(a: A): Unit = dbRepr.value.save(writeDataset)(gen.to(a))

    override def load(readDataset: (String, SparkSession) => DataFrame)(spark: SparkSession): A = gen.from(dbRepr.value.load(readDataset)(spark))
  }
}
