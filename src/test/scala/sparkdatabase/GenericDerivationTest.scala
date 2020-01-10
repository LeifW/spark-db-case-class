package sparkdatabase

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Account(accountId: Long, name: String)
case class Item(sku: String, price: BigDecimal)
case class Purchase(accountId: Long, items: Seq[Item])

case class CommerceDB(accounts: Dataset[Account], purchases: Dataset[Purchase])

class GenericDerivationTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  //override def afterAll()= spark.stop()

  import spark.implicits._
  import sparkdatabase.generic._

  //val sdb = SparkDatabase[CommerceDB]
  //val sdb = SparkDatabase.withDefaultTables[CommerceDB]

  val accounts = Seq(Account(123, "Bob"))
  val purchases = Seq(Purchase(123, Seq(Item("xod9", BigDecimal(16.95)))))

  val datasets = CommerceDB(
    accounts = accounts.toDS,
    purchases = purchases.toDS
  )

  //spark.catalog.
  // createTempView so we don't get "table already exists" errors on successive test runs
  // save table defaults to writing parquet files to spark-warehouse directory
  def saveTables[A](a: A)(implicit sdb: SparkDatabase[A]): Unit = sdb.save(_ createTempView _)(a)
  def loadTables[A](implicit sdb: SparkDatabase[A]): A = sdb.load(spark.read.table)


  "The generic derivation machinery" should "save to tables" in {
    saveTables(datasets)
  }
  it should "read from tables" in {
    val loaded = loadTables[CommerceDB]
    loaded.accounts.collect should contain theSameElementsAs accounts
    loaded.purchases.collect should contain theSameElementsAs purchases
  }

}
