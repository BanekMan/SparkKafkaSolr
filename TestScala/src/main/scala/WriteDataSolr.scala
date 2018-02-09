package main.scala

import com.github.takezoe.solr.scala._

object WriteDataSolr {
  
  val client = new SolrClient("http://localhost:8983/solr/name")
  
  client
  .add(Map("id"->"007", "manu" -> "Spark", "name" -> "Kafka"))
  .add(Map("id"->"008", "manu" -> "Spark", "name" -> "Scala"))
  .add(Map("id"->"009", "manu" -> "Spark", "name" -> "Solr"))
  .commit
  
  // query
val result = client.query("name: %name%")
  .fields("id", "manu", "name")
  .sortBy("id", Order.asc)
  .getResultAsMap(Map("name" -> "ThinkPad"))

result.documents.foreach { doc: Map[String, Any] =>
  println("id: " + doc("id"))
  println("  manu: " + doc("manu"))
  println("  name: " + doc("name"))
}
}
