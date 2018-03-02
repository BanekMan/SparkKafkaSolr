package main.scala
import org.apache.solr.client.solrj.impl._
/**
 * Object which connect to Solr Client using class HttpSolrClient
 */
object WriteDataSolr {
  val client = new HttpSolrClient("http://localhost:8983/solr/name")
}