package org.populate

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import java.security.MessageDigest

val contractSchema = Encoders.bean(OptionTicker::class.java).schema()
val spark = SparkSession.builder()
    .appName("Kotlin Spark MongoDB Integration")
    .master("local[*]")
    .config("spark.executor.cores", "4")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/mydatabase.mycollection")
    .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/test.myCollection")
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/test.myCollection")
    .config("spark.task.maxFailures", "5")
    .orCreate

val tickers = spark.read()
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("sp500_companies.csv")
    .select("Symbol").cache()


// Flow function to get aggregates for each contract
fun getAggregatesFlow(contracts: List<Row>, polygonKey: String, fromDate: String, toDate: String): Flow<List<OptionAggregateFull>> = flow {
    val oclient = OptionsClient(polygonKey)  // Create OptionsClient once, not in loop
    for (contract in contracts) {
        val ticker = contract.getAs<String>("ticker")  // Get ticker from contract
        println("Getting aggregates for $ticker")

        // Suspend while fetching the aggregates


        for(i in 1 .. 3){
            try {
                val result = oclient.asyncGetContractAggs(ticker, from = fromDate, to = toDate)
                // Emit the result into the flow
                emit(result)
                break
            } catch (e: java.net.SocketTimeoutException){
                if (i == 3) {
                    println("Failed to fetch aggregates after 3 attempts for $ticker")
                    throw e
                }

                println("Request failed for $ticker. Retrying $i")
                delay(1000)
            }

        }
    }
}

fun contractFlow(tickers: List<String>): Flow<List<OptionTicker>> = flow {
    val oclient = OptionsClient(polygonKey)

    tickers.forEach { ticker ->
        // Fetch contracts for each ticker asynchronously and emit the result as a Flow
        val contracts = oclient.asyncGetContracts(ticker)
        emit(contracts)  // Emit the fetched contracts for this ticker
    }
}.flowOn(Dispatchers.IO)

val contractFlow = contractFlow(tickers.collectAsList().map{
        it -> it.getAs(0)
})




val contractsDf = spark.read().schema(contractSchema).option("recursiveFileLookup", "true") // Assume the first row is a header
    .parquet("results/contracts")



val channel = Channel<List<OptionAggregateFull>>(capacity = Channel.UNLIMITED)


val aggList = mutableListOf<OptionAggregateFull>()  // Temporary list for batching

val digest = MessageDigest.getInstance("SHA-256")
val producerJob = fun (scope: CoroutineScope, aggsFlow:Flow<List<OptionAggregateFull>>, channel: Channel<List<OptionAggregateFull>>, batchSize: Int):Job {
    return scope.launch{
        aggsFlow.filter { agg -> agg.isNotEmpty() }
            .collect { agg ->
                aggList.addAll(agg.toList())
                println("\n=== current size of the list: ${aggList.size}===\n")
                // When the batch size reaches 100, send it through the channel
                if (aggList.size >= batchSize) {
                    println("=== Sent through channel ===")
                    channel.send(aggList.toList())  // Send a copy of the current batch
                    aggList.clear()  // Clear the list after sending
                }
            }
        // Send any remaining aggregates after collection is done
        if (aggList.isNotEmpty()) {
            println("=== Sent through channel ===")
            channel.send(aggList)
            aggList.clear()
        }
        channel.close()
    }  // Close the channel when done
}

val consumerJob = fun(scope: CoroutineScope, channel: Channel<List<OptionAggregateFull>>):Job{
    return scope.launch(Dispatchers.IO) {
        for (batch in channel) {
            println("=== Received through channel ===")
            if (batch.isNotEmpty()) {
                val hash = digest.digest(batch.last().ticker.toByteArray())

                println("\n=== persisting aggregates for batch ===\n")
                spark.createDataFrame(batch, OptionAggregateFull::class.java)
                    .withColumn("hash_id", expr("hex(hash(*))"))
                    .write().mode("append")
                    .parquet("results/aggs/${hash.joinToString("") { "%02x".format(it) }}-${today}")

            }
        }
    }
}