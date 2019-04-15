spark-shell --master yarn --conf spark.ui.port=12654

//check size of data in hdfs 
hdfs dfs -du -s -h /data/

spark-shell --master yarn --conf spark.ui.port=12654 --num-executors 1 --executor-memory 512M

//Initialize spark Context programatically 1.6
import org.apache.spark.{SparkConf, SparkContext}
val conf = new SparkConf().setAppName("Daily revenue").setMaster("yarn-client")
val sc = new SparkContext(conf)

//create RDD from local System
val productsRaw = scala.io.Source.fromFile("/root/data/retail_db/products/part-00000").getLines.toList
val products = sc.parallelize(productsRaw)

//create RDD From List
val l = (1 to 100).toList
val RDDFromList = sc.parallelize(l)

// reading json in dataframe 
val ordersFromJson = sqlContext.read.json("/user/test_user/retail_db_json/orders/")

// reading Textfile 
val orders = sc.textFile("/user/test_user/retail_db/orders/")

//select in dataframe 
ordersFromJson.select("order_id","order_date")

// print schema of dataframe 
ordersFromJson.printSchema

//another way of reading different files 
val readData = sqlContext.load("path","fileformate")

val str = orders.first
str.split(",")(1).substring(0,10).replace("-","").toInt

//map
val orderDate = orders.map(str => { str.split(",")(1).substring(0,10).replace("-","").toInt} ) 

val ordersPairedRDD = orders.map(order => {
	val o = order.split(",")
	(o(0).toInt, o(1).substring(0,10).replace("-","").toInt)
})

//tuples
val ordersPairedRDDTuples = orders.map(order => {
	(orders.split(",")(1).toInt, order)
})

val l = List("hello", "how are you","Let's perform word count")
val l_rdd = sc.parallelize(l)
val l_flatmap = l_rdd.flatMap(wordcount => wordcount.split(" "))

//filtering of data 
orders.filter(order => order.split(",")(3) == "COMPLETE").foreach(println)


//joins 

val orderItem = sc.textFile("/user/test_user/retail_db/order_items/")
val ordersMap = orders.map(order => (order.split(",")(0).toInt, order.split(",")(1).substring(0,10)))
val orderItemsMap = orderItem.map(orderItem => {
	val o = orderItem.split(",")
	(o(1).toInt, o(4).toFloat)
})
val ordersJoin = ordersMap.join(orderIltemsMap)

// Get all the orders which do not have corresponding enteries in order items 

val ordersMap = orders.map(order => {(order.split(",")(0).toInt, order)})
val orderIltemsMap = orderItem.map(orderItem => {
	val o = orderItem.split(",")
	(o(1).toInt, orderItem)
})
val ordersLeftOuterJoin = ordersMap.leftOuterJoin(orderIltemsMap)
val ordersLeftOuterJoinJoinFilter = ordersLeftOuterJoin.filter(order => order._2._2 == None)
val ordersWithNoOrderItem = ordersLeftOuterJoinJoinFilter.map(order => order._2._1)

//aggregations using actions
orders.map(order => (order.split(",")(3),"")).countBykey.foreach(println)

val orderItemsRevenue = orderItem.map(o => o.split(",")(4).toFloat)
orderItemsRevenue.reduce((total,revenue) => total + revenue).take(10).foreach(println)
val orderItemsMaxRevenue = orderItemsRevenue.reduce((max,revenue) => { 
	if(max < revenue ) revenue else max
})


val orderItemsMap = orderItem.map(orderItem => {
	val o = orderItem.split(",")
	(o(1).toInt, o(4).toFloat)
})


//GroupByKey
val orderItemsGBK = orderItemsMap.groupByKey  
//Get revenue per order_id using GBK
  orderItemsGBK.map(rec => (rec._1, rec._2.toList.sum)).take(10).foreach(println)

//Get data in descending order by order_item_subtotal for order_id using GBK
orderItemsGBK.flatMap(rec => { rec._2.toList.sortBy(o => -o).map(k=> (rec._1,k))}).take(10).foreach(println)

//Aggregations - reduceByKey
val revenuePerOrderId = orderItemsMap.reduceByKey((total,revenue) => total + revenue)

//min revenue
val minRevenuePerOrderID = orderItemsMap.reduceByKey((min, revenue)=> if(min > revenue ) revenue else min)

//aggregations - aggregateByKey

 val revenueAndMaxPerProductId = orderItemsMap.aggregateByKey((0.0f,0.0f))(
 (inter, subtotal) => (inter._1 + subtotal, if(subtotal > inter._2) subtotal else  inter._2),
 (total, inter) => (total._1 +  inter._1, if(total._2 > inter._2) total._2  else  inter._2))

 //sorting sortByKey
val products = sc.textFile("/user/test_user/retail_db/products")
 val productsMap = products.map(product => (product.split(",")(1).toInt,product))
 val productsSortById = productsMap.sortByKey()
 val productsSortByPrice = products.filter(product => product.split(",")(4) != "").
 map(product => ((product.split(",")(1).toInt,-product.split(",")(4).toFloat),product))
val productsSorted = productsSortByPrice.sortByKey().map(rec => rec._2)
productsSorted.take(10).foreach(println)


//Ranking -- Global Ranking (details of top 5 products using take)
val products = sc.textFile("/user/test_user/retail_db/products")
val productsMap = products.
	filter(product => product.split(",")(4) != "").
	map(product => (product.split(",")(4).toFloat,product))
val productsSortedByPrice = productsMap.sortByKey(false)
productsSortedByPrice.take(10).foreach(println)

//Ranking -- Global Ranking (details of top 5 products using takeOrder)
val products = sc.textFile("/user/test_user/retail_db/products")
val productsMap = products.
	filter(product => product.split(",")(4) != "").
	takeOrdered(10)(Ordering[Float].reverse.on(product => product.split(",")(4).toFloat)).foreach(println)


//Ranking Get top N priced products with in each product category 
val products = sc.textFile("/user/test_user/retail_db/products")
val productsMap = products.filter(product => product.split(",")(4) != "").
map(product => (product.split(",")(1).toInt, product))
val productsGroupByCategory = productsMap.groupByKey



def getTopNproducts(productsIterable: Iterable[String], topN : Int):Iterable[String] = {
	val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
	val topNPrices=productPrices.toList.sortBy(p => -p).take(topN)
	val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
	val minOfTopNPrice  = topNPrices.min
	val topNPricedProducts = productsSorted.takeWhile(p => p.split(",")(4).toFloat >= minOfTopNPrice)
	topNPricedProducts
}

val productsIterable = productsGroupByCategory.first._2
val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
val topNPrices = productPrices.toList.sortBy(p => -p).take(5)

//Get All the products in decesending order by price
val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
val minOfTopNPrice  = topNPrices.min
val topNPricedProducts = productsSorted.takeWhile(p => p.split(",")(4).toFloat >= minOfTopNPrice)

val top3PricedProductsPerCategory = productsGroupByCategory.flatMap(rec => getTopNproducts(rec._2, 3)) 


 //Set Operations

 // reading Textfile 
val orders = sc.textFile("/user/test_user/retail_db/orders/")
val customers_201308 = orders.filter(order =>order.split(",")(1).contains("2013-08")).
map(order => order.split(",")(2).toInt)
val customers_201309 = orders.filter(order =>order.split(",")(1).contains("2013-09")).
map(order => order.split(",")(2).toInt)
// Get all the customers who placed the orders in 2013 August and 2013 September
val customers_201308_and_201309 = customers_201308.intersection(customers_201309)

// Get all the unique customers who placed the orders in 2013 August and 2013 September
 val customers_201308_union_201309 = customers_201308.union(customers_201309).distinct

 // Get all the unique customers who placed the orders in 2013 August but not in 2013 September
 val customers_201308_minus_201309 = customers_201308.map(c => (c,1)).
 leftOuterJoin(customers_201309.map(c=> (c,1))).
filter(rec =>  rec._2._2 == None).map(rec => rec._1).distinct


