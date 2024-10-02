package org.populate
//import org.jetbrains.kotlinx.spark.api.SparkSession
import io.github.cdimascio.dotenv.dotenv
import io.polygon.kotlin.sdk.rest.ComparisonQueryFilterParameters
import io.polygon.kotlin.sdk.rest.PolygonRestClient
import io.polygon.kotlin.sdk.rest.reference.OptionsContractsParameters
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.Json
import okhttp3.*
import org.apache.spark.sql.SparkSession
import java.io.IOException
import java.io.Serializable
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException


//import org.apache.spark.sql.types.StructType

//TODO: Need to Figure out all the base urls

@kotlinx.serialization.Serializable
data class AggregatesResponse(
    val adjusted: Boolean,
    val count: Int?=null,
    val queryCount: Int,
    val request_id: String,
     val results: List<OptionAggregate>?=null,
    val resultsCount: Int,
    val status: String,
    val ticker: String
)
@kotlinx.serialization.Serializable
data class OptionAggregate(
    val c: Double, // Close price
    val h: Double, // High price
    val l: Double, // Low price
    val n: Int,    // Number of trades
    val o: Double, // Open price
    val t: Long,   // Timestamp
    val v: Int,    // Volume
    val vw: Double // Volume-weighted average price
):Serializable
@kotlinx.serialization.Serializable
data class OptionAggregateFull(
    val ticker: String,
    val c: Double, // Close price
    val h: Double, // High price
    val l: Double, // Low price
    val n: Int,    // Number of trades
    val o: Double, // Open price
    val t: Long,   // Timestamp
    val v: Int,    // Volume
    val vw: Double // Volume-weighted average price
):Serializable
@kotlinx.serialization.Serializable
data class OptionTicker(
    @SerialName("cfi") val cfi: String? = null,
    @SerialName("contract_type") val contractType: String? = null,
    @SerialName("correction") val correction: Int? = null,
    @SerialName("exercise_style") val exerciseStyle: String? = null,
    @SerialName("expiration_date") val expirationDate: String? = null,
    @SerialName("primary_exchange") val primaryExchange: String? = null,
    @SerialName("shares_per_contract") val sharesPerContract: Double? = null,
    @SerialName("strike_price") val strikePrice: Double? = null,
    @SerialName("ticker") val ticker: String? = null,
    @SerialName("underlying_ticker") val underlyingTicker: String? = null
):Serializable
@kotlinx.serialization.Serializable
data class OptionTickerResponse(
    val request_id:String,
    val next_url: String?=null,
    val results:List<OptionTicker>?= emptyList()


):Serializable



val nyZoneId = ZoneId.of("America/New_York")
val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
var today = Instant.now().atZone(nyZoneId).toLocalDate().format(formatter)
val polygonKey = dotenv()["POLYGON_API_KEY"]
val v = listOf("v1", "v2", "v3")
val baseUrl = "https://api.polygon.io/v3/reference/options/"
val extensions = listOf("stocks" to 25,
    "options" to "$baseUrl/${v[2]}/options/",
    "reference" to "$baseUrl/${v[2]}/reference/").toMap()
//For unit testing in spark


 fun fetchOptionContracts(
    spark: SparkSession,
    underlying_ticker: String,
    d: Int = 30,
    expired: Boolean = false
): List<OptionTicker> {
    return try {
        // Perform the API call asynchronously on the IO dispatcher
            val oclient = PolygonRestClient(polygonKey).referenceClient
            val sp = ComparisonQueryFilterParameters(underlying_ticker)
            val params = OptionsContractsParameters(sp, limit = d, expired = expired)

            // Initialize the iterator to paginate through results
            val iterator = oclient.listOptionsContracts(params).iterator()

            // List to accumulate all OptionTickers
            val optionTickers = mutableListOf<OptionTicker>()

            // Iterate through the paginated results
            while (iterator.hasNext()) {
                val contract = iterator.next()
                val ticker = OptionTicker(
                    contract.cfi,
                    contract.contractType,
                    contract.correction,
                    contract.exerciseStyle,
                    contract.expirationDate,
                    contract.primaryExchange,
                    contract.sharesPerContract,
                    contract.strikePrice,
                    contract.ticker,
                    contract.underlyingTicker
                )
                optionTickers.add(ticker)  // Add the contract to the list
            }
            println("\n=== Completed Gathering for $underlying_ticker ====\n")
            optionTickers  // Return the list of OptionTickers

    } catch (e: Exception) {
        println("Failed to get contract for $underlying_ticker due to $e")
        emptyList() // Return an empty list in case of exception
    }
}

class OptionsClient(api_key: String) {
    private val apiKey: String = api_key
    private val client: OkHttpClient = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(5, 5, TimeUnit.MINUTES))
        .connectTimeout(60, TimeUnit.SECONDS)   // Increase connection timeout
        .readTimeout(60, TimeUnit.SECONDS)      // Increase read timeout
        .writeTimeout(60, TimeUnit.SECONDS)     // Increase write timeout
        .build()

    suspend fun asyncGetContracts(
        underlying_ticker: String,
        d: Int = 1000,
        expired: Boolean = false
    ): List<OptionTicker> {
        val url =
            "https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=$underlying_ticker&expired=$expired&limit=$d&apiKey=$apiKey"
        println("\n====\nFetching from: \n$url\n===\n")

        val request = Request.Builder()
            .url(url)

            .build()

        return withContext(Dispatchers.IO) {
            val response = client.newCall(request).execute()  // Synchronous execution
            if (!response.isSuccessful) {
                throw IOException("Unexpected code $response")
            }

            val responseBody = response.body?.string()

            responseBody?.let {
                // Parse the JSON response using kotlinx.serialization
                val json = Json { ignoreUnknownKeys = true }

                val responseData = json.decodeFromString<OptionTickerResponse>(it)
                return@withContext responseData.results?.map { optionResponse ->
                        OptionTicker(
                            cfi = optionResponse.cfi,
                            contractType = optionResponse.contractType,
                            correction = optionResponse.correction,
                            exerciseStyle = optionResponse.exerciseStyle,
                            sharesPerContract = optionResponse.sharesPerContract,
                            expirationDate = optionResponse.expirationDate,
                            primaryExchange =optionResponse.primaryExchange,
                            strikePrice = optionResponse.strikePrice,
                            ticker = optionResponse.ticker,
                            underlyingTicker = optionResponse.underlyingTicker
                        )
                    }
                }  // Return the list of OptionTickers
            } ?: emptyList()

    }



    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun asyncGetContractAggs(
        optionsTicker: String,
        multiplier: Int = 1,
        timespan: String = "day",
        from: String = today,
        to: String = today,
        adjusted: Boolean = true,
        sort: String = "asc",
    ): List<OptionAggregateFull> = suspendCancellableCoroutine { continuation ->

        val client = OkHttpClient.Builder()
            .connectionPool(ConnectionPool(5, 5, TimeUnit.MINUTES)) // Adjust these values
            .build()
        val url =
            "https://api.polygon.io/v2/aggs/ticker/$optionsTicker/range/$multiplier/$timespan/$from/$to?adjusted=$adjusted&sort=$sort&apiKey=$apiKey"
        println("\n=== fetching from===\n$url")
        val request = Request.Builder()
            .url(url)
            .build()

        val call = client.newCall(request)

        // Handle coroutine cancellation
        continuation.invokeOnCancellation {
            call.cancel()
        }

        call.enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                // Resume the coroutine with an exception
                if (continuation.isActive) {
                    continuation.resumeWithException(e)
                }
            }

            override fun onResponse(call: Call, response: Response) {
                try {
                    val bodyString = response.body?.string()
                    val result = when {
                        response.isSuccessful && bodyString != null -> {
                            val body = Json.decodeFromString<AggregatesResponse>(bodyString)
                            if (body.results != null)
                                println("\n=== Found and added aggregates for ${body.ticker} with ${body.results.size} results ===\n")

                            else
                                println("\n=== No aggregates for ${body.ticker}===\n")
                            body.results?.map { aggResponse ->
                                OptionAggregateFull(
                                    ticker = body.ticker,
                                    c = aggResponse.c,
                                    h = aggResponse.h,
                                    l = aggResponse.l,
                                    n = aggResponse.n,
                                    o = aggResponse.o,
                                    t = aggResponse.t,
                                    v = aggResponse.v,
                                    vw = aggResponse.vw
                                )
                            } ?: emptyList()
                        }
                        else -> {
                            // Handle errors or unexpected responses
                            emptyList()
                        }
                    }
                    // Resume the coroutine with the result
                    continuation.resume(result)
                } catch (e: Exception) {
                    // Resume the coroutine with an exception
                    if (continuation.isActive) {
                        continuation.resumeWithException(e)
                    }
                }
            }
        })
    }

    fun getContractAggs(
        optionsTicker: String,
        multiplier: Int = 1,
        timespan: String = "day",
        from: String = today,
        to: String = today,
        adjusted: Boolean = true,
        sort: String = "asc",
    ): List<OptionAggregateFull> {
        val client = OkHttpClient()
        val url =
            "https://api.polygon.io/v2/aggs/ticker/$optionsTicker/range/$multiplier/$timespan/$from/$to?adjusted=$adjusted&sort=$sort&apiKey=$apiKey"

        return try {
            val request = Request.Builder()
                .url(url)
                .build()
            val response = client.newCall(request).execute()

            val bodyString = response.body?.string()

            when {
                response.isSuccessful -> {
                    // Success (2xx status code)

                    if (bodyString != null) {
                        val body = Json.decodeFromString<AggregatesResponse>(bodyString)



                        return body.results?.map {
                                aggResponse -> OptionAggregateFull(
                            body.ticker, aggResponse.c,
                            aggResponse.h, aggResponse.l,
                            aggResponse.n, aggResponse.o,
                            aggResponse.t, aggResponse.v,
                            aggResponse.vw

                        ) } ?: emptyList()
                    }

                }

                response.code in 400..499 -> {
                    // Client-side error (4xx status code)
                    println("Client error: ${response.code} @ \n $url")

                }

                response.code in 500..599 -> {
                    // Server-side error (5xx status code)
                    println("Server error: ${response.code} @ \n $url")
                }

                else -> {
                    // Other HTTP response status codes
                    println("Unexpected error: ${response.code} @ \n $url")
                }
            }

            emptyList()
        } catch (e: IOException) {
            println("Network error: ${e.message}")
            emptyList()
        }
    }

    }

