package sparkdatabase.generic

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}
import shapeless.labelled.{FieldType, field}
import sparkdatabase.SparkDatabase

trait SparkDatabaseGenericDerivation {
  implicit val hNilTable: SparkDatabase[HNil] = new SparkDatabase[HNil] {
    override def save(writeDataset: (Dataset[_], String) => Unit)(a: HNil): Unit = ()
    override def load(readDataset: String => DataFrame): HNil                    = HNil
  }

  implicit def hConsTables[A: Encoder, K <: Symbol, T <: HList](implicit key: Witness.Aux[K], sdb: SparkDatabase[T]): SparkDatabase[FieldType[K, Dataset[A]] :: T] = new SparkDatabase[FieldType[K, Dataset[A]] :: T] {
    override def save(writeDataset: (Dataset[_], String) => Unit)(a: FieldType[K, Dataset[A]] :: T): Unit = {
      writeDataset(a.head, key.value.name)
      sdb.save(writeDataset)(a.tail)
    }
    override def load(readDataset: String => DataFrame): FieldType[K, Dataset[A]] :: T =
     field[K](readDataset(key.value.name).as[A]) :: sdb.load(readDataset)
  }

  implicit def genericTables[A, Repr](implicit gen: LabelledGeneric.Aux[A, Repr], dbRepr: Lazy[SparkDatabase[Repr]]): SparkDatabase[A] = new SparkDatabase[A] {
    override def save(writeDataset: (Dataset[_], String) => Unit)(a: A): Unit = dbRepr.value.save(writeDataset)(gen.to(a))
    override def load(readDataset: String => DataFrame): A                    = gen.from(dbRepr.value.load(readDataset))
  }
}
