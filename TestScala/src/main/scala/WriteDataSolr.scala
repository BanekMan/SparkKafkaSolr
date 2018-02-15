package main.scala
import org.apache.solr.client.solrj.impl._

object WriteDataSolr {
  val client = new HttpSolrClient("http://localhost:8983/solr/name")
}