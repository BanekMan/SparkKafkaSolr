package main.scala
import org.apache.solr.client.solrj.impl._
import org.apache.solr.common._

object WriteDataSolr {
   def main(args: Array[String]): Unit = {
     
     val client = new HttpSolrClient("http://localhost:8983/solr/name")
     
     val doc = new SolrInputDocument()
     
    def getSolrDocument(): SolrInputDocument = {
      val document = new SolrInputDocument()
      document.addField("id", "5")
      document.addField("name", "imie5")
      document.addField("age", "35")
      document.addField("addr", "Adres5")
      document
    }
    client.add(getSolrDocument)
    client.commit
  }
}