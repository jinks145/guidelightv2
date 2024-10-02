package org.populate

import io.github.cdimascio.dotenv.dotenv
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.system.measureTimeMillis

// DB_URL was database-1.ctq6oauek6z5.us-east-2.rds.amazonaws.com
//DB_PW was SBu14zqI95fPvfHQUkF5
// Main function to run the Spark job
fun main(args: Array<String>) = runBlocking(Dispatchers.IO) {

    Files.createDirectories(Paths.get("results"))
    val dbname = "stockoptionsdb"
    val dotenv = dotenv()
    val url = dotenv["DB_URL"]
    val port = 5432
    val props = Properties().apply {
        put("user", dotenv["RDS_USER_ID"])
        put("password", dotenv["RDS_PW"])
        put("driver", "org.postgresql.Driver")
        put("connectTimeout", "60000")  // 60 seconds
        put("socketTimeout", "300000")  // 5 minutes
        put("maximumPoolSize", "80")    // Pool size limit
        put("tcpKeepAlive", "true")
    }

    // Initialize Spark session
    //    spark.sparkContext().setCheckpointDir("checkpoint")
    println("=== Spark session initialized ====\n")
    println("=== Reading company tickers ====\n")
// Read tickers from CSV

    println("=== Complete. ====\n")

//    spark.catalog().clearCache()
    println("\n=== Gathering Contracts ====\n")
    if(!File("results/contracts").exists()) {
        Files.createDirectories(Paths.get("results/contracts"))
        contractFlow.collect { contracts ->
            println("Fetched ${contracts.size} contracts:")
            if (contracts.isNotEmpty()) {

                val res = spark.createDataFrame(contracts, OptionTicker::class.java)
                res.write().mode("append").parquet("results/contracts/${contracts.last().underlyingTicker}")
            }
        }
    }
//    contractsDf.write().mode("overwrite").parquet("results/contracts")
//    contractsDf.write().mode("overwrite").parquet("data/contracts")
    println("\n ==== dataframe stucture for contracts ===")
//    val fetchedContracts = spark.read().schema(contractSchema).option("recursiveFileLookup", "true") // Assume the first row is a header
//        .parquet("results/contracts")
    contractsDf.printSchema()

    contractsDf.repartition(512).write().mode("overwrite")
        .option("batchsize", "5000")
        .jdbc("jdbc:postgresql://${url}:${port}/${dbname}", "contracts", props)

    println("\n=== Gathering Contracts Complete. Persisted the records ====\n")
    val contracts = contractsDf.select("ticker").collectAsList()


val aggsFlow = fun(args: Array<String>): Flow<List<OptionAggregateFull>> {
    return if (args.size < 2) {
        getAggregatesFlow(contracts, polygonKey, "2024-06-18", "2024-09-19")
    } else {
        getAggregatesFlow(contracts, polygonKey, args[0], args[1])
    }
}


    var runTime = 0L
if(!File("results/aggs").exists()) {
    Files.createDirectories(Paths.get("results/aggs"))
    runTime = measureTimeMillis {
// Producer coroutine: collects and batches the data
        val aggFlow = aggsFlow(args)
        val channel = Channel<List<OptionAggregateFull>>(capacity = Channel.UNLIMITED)
        val batchSize = 2500

        val scope = CoroutineScope(currentCoroutineContext())
        val producerJob = producerJob(scope, aggFlow, channel, batchSize)
        val consumerJob = consumerJob(scope, channel)
        producerJob.join()
        consumerJob.join()

    }
}


    println("=== Collecting aggregate df is complete ===\n")

    println("\n=== Gathering option aggregates complete ===\n")
    // Show the contents of the DataFrame

    println("\n ==== dataframe stucture for option aggregates ===\n")

    contractsDf.printSchema()
    val aggsDf = spark.read().option("recursiveFileLookup", "true").parquet("results/aggs").cache()
    val count =  aggsDf.count()
    println("\n=== Persisting aggregates to database with $count rows. ====\n")


    aggsDf.repartition(512).write().
    mode("overwrite")
        .option("batchsize", "5200")
        .jdbc("jdbc:postgresql://${url}:${port}/${dbname}", "option_aggs", props)

    println("=== Spark session stopping ===\n")
    println("\n=== Gathering option aggregates completed with $runTime ms===\n")
    spark.stop()
}


