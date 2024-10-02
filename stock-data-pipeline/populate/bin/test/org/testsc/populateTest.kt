package org.testsc
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.TestResult
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.populate.*
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ClientTest {


    companion object {
        // Spark session is initialized once per class
        var spark: SparkSession = SparkSession.builder()
            .appName("Test Spark App")
            .master("local[*]")  // Use local mode for testing
            .getOrCreate()

        fun tearDown() {
            // If necessary, you can stop the Spark session after all tests manually.
            // However, you might prefer not to stop it here if you want to reuse it across multiple tests.
            spark.stop()
        }
    }



    @Test
    fun fetchOptionAggs() {
        val client = OptionsClient(polygonKey)
        assertNotNull(client.getContractAggs("O:SPY251219C00650000",
            1, "day",
            from = "2023-01-09", to="2023-02-10"))
    }

@Test
fun `test basic Spark functionality`() {
        val contracts = listOf("O:AAPL240920C00005000", "O:AAPL240920C00010000", "O:AAPL240920C00025000")
        for (cohtract in contracts){
            val result = fetchOptionContracts(spark, "AAPL")
            assertNotNull(result)
        }
    }
    @Test
    fun `test asynchronous retrieval`(): TestResult {
        // initialize some test state
        val contracts = listOf("O:AAPL240920C00005000", "O:AAPL240920C00010000", "O:AAPL240920C00025000")

        return runTest {
            val res = withContext(Dispatchers.IO) {
                val client = OptionsClient(polygonKey)

                // Flow to emit the results of getting contract aggregates
                fun simple(): Flow<List<OptionAggregateFull>> = flow {
                    for (contract in contracts) {
                        val aggs = client.asyncGetContractAggs(contract, 1, "day", "2024-08-19","2024-09-19") // Fetch aggregates for each contract
                        emit(aggs) // Emit the result
                    }
                }

                // Collect the flow and do something with the results, e.g., print or assert
                simple().collect { aggs ->
                    println(aggs) // Replace with assertions or checks for the test
                }
            }
        }

}
 @Test
 fun `test async get optionContracts`():TestResult = runBlocking {

     val contractFlow = contractFlow(tickers.limit(100).collectAsList().map{
         it -> it.getAs(0)
     })

     contractFlow.collect { contracts ->
         println("Fetched ${contracts.size} contracts:")
         if(contracts.size > 0) {
             val res = spark.createDataFrame(contracts, OptionTicker::class.java)
             res.write().mode("append").parquet("results/contracts/${contracts.last().underlyingTicker}")
         }
     }
     assertTrue(true)
 }
@Test
//fun `test optionContract broadcasting`(){
//
//    // Read the tickers from a CSV file
//    val tickers = spark.read()
//        .option("header", "true")
//        .option("inferSchema", "true")
//        .csv("sp500_companies.csv")
//        .select("Symbol")
//
//    // Use flatMap to fetch option contracts and create a combined Dataset of Maps
//    val contractsDf: Dataset<Row> = tickers.repartition(col("Symbol")).flatMap(
//        FlatMapFunction<Row, OptionTicker> { row ->
//            // Extract the ticker symbol from the row
//            val ticker = row.getAs<String>("Symbol")
//
//            // Fetch the option contracts for the ticker
//            val result = fetchOptionContracts(spark, ticker)
//
//            // Convert result to an iterator of maps (handling null case with emptyList)
//            result?.iterator() ?: emptyList<OptionTicker>().iterator()
//        },
//        Encoders.bean(OptionTicker::class.java)// Using Java serialization for Maps
//    ).toDF()
//
//    // Assert that the combined DataFrame is not null
//    assertNotNull(contractsDf)
//
//    // Show the contents of the DataFrame
//    contractsDf.show(false)
//    contractsDf.printSchema()
//}
//    @Test
//    fun `test single optionContract aggregate broadcasting`(){
//
//        // Read the tickers from a CSV file
//        val tickers = spark.read()
//            .option("header", "true")
//            .option("inferSchema", "true")
//            .csv("sp500_companies.csv")
//            .select("Symbol")
//
//        // Use flatMap to fetch option contracts and create a combined Dataset of Maps
//        val contractsDf: Dataset<Row> = tickers.repartition(col("Symbol")).flatMap(
//            FlatMapFunction<Row, OptionTicker> { row ->
//                // Extract the ticker symbol from the row
//                val ticker = row.getAs<String>("Symbol")
//
//                // Fetch the option contracts for the ticker
//                val result = fetchOptionContracts(spark, ticker)
//
//                // Convert result to an iterator of maps (handling null case with emptyList)
//                result?.iterator() ?: emptyList<OptionTicker>().iterator()
//            },
//            Encoders.bean(OptionTicker::class.java)// Using Java serialization for Maps
//        ).toDF()
//
//        // Assert that the combined DataFrame is not null
//        assertNotNull(contractsDf)
//
//        val aggsDf: Dataset<Row> = contractsDf.flatMap(
//            FlatMapFunction<Row, OptionAggregateFull>{
//                row ->  val ticker = row.getAs<String>("ticker")
//                val oclient = OptionsClient(polygonKey)
//
//                val result = oclient.getContractAggs(ticker)
//                result.iterator()
//            },
//            Encoders.bean(OptionAggregateFull::class.java)
//        ).toDF()
//            .withColumn("hash_id", expr("hash(*)"))
//
//
//
//        // Show the contents of the DataFrame
//        aggsDf.show()
//        aggsDf.printSchema()
//    }
fun `test async optionContract aggregate broadcasting`():TestResult = runBlocking{

        // Read the tickers from a CSV file
        val tickers = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("sp500_companies.csv")
            .select("Symbol")

        // Use flatMap to fetch option contracts and create a combined Dataset of Maps
        val contractsDf: Dataset<Row> = tickers.repartition(col("Symbol")).flatMap(
            FlatMapFunction<Row, OptionTicker> { row ->
                // Extract the ticker symbol from the row
                val ticker = row.getAs<String>("Symbol")

                // Fetch the option contracts for the ticker
                val result = fetchOptionContracts(spark, ticker)

                // Convert result to an iterator of maps (handling null case with emptyList)
                result.iterator() ?: emptyList<OptionTicker>().iterator()
            },
            Encoders.bean(OptionTicker::class.java)// Using Java serialization for Maps
        ).toDF()

        // Assert that the combined DataFrame is not null
        assertNotNull(contractsDf)

        val contracts = contractsDf.limit(20) .select("ticker").collectAsList()

        fun fetchAggregates():Flow<Dataset<Row>> = contracts
            .asFlow()
        .flatMapConcat { row ->

            flow{
                val client = OptionsClient(polygonKey)
                val aggs = withContext(Dispatchers.IO) {
                    val ticker = row.getAs<String>("ticker")
                    spark.createDataFrame( client.asyncGetContractAggs(ticker, 1, "day", "2024-08-19","2024-09-19") , OptionAggregateFull::class.java)

                }
             emit(aggs)
            }

        }
        var aggsDf = spark.createDataFrame(emptyList<OptionAggregateFull>(), OptionAggregateFull::class.java)

        fetchAggregates().collect { aggregates ->
            aggsDf = aggsDf.union(aggregates) // Replace with assertions or other processing logic
        }
        aggsDf = aggsDf.withColumn("hash_id", expr("hex(hash(*))"))

        // Show the contents of the DataFrame
        aggsDf.show()
        aggsDf.printSchema()
    }


}
