// this is a comment
// Assignment 5
// Greg Wills
// Big Data Analytics 
// Professor Grace Yang

import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec

object PageRank {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("Page Rank").setMaster("local")
        val sc = new SparkContext(sparkConf)

        val input = sc.textFile("../LinkGraph/links/") // your Wiki Linkgraph from Part One

        // Regexes
        val title_regex = """(?<=\()[^\,]+""".r // find first in
        //val links_regex = """(?<=\,).*$""".
        val links_regex = """(?<=\t)[^\t]+""".r

        val links = input.map {
        	case(inString) =>
        		val title = title_regex.findFirstIn(inString).mkString("")
        		val outlinks = links_regex.findAllIn(inString).toList
        	(title,outlinks)
        }

        //links.collect.foreach(println)

        var ranks = links.mapValues(v => 1.0)

        val ITERATIONS = 20
        
 		// TODO: Implement your PageRank algorithm according to the notes
 		for (i <- 0 to ITERATIONS) {
			val contribs = links.join(ranks).flatMap {
				case (title, (links, rank)) =>
					links.map(dest => (dest, rank / links.size))
			}
			ranks = contribs.reduceByKey( _+_ ).mapValues(0.15 + 0.85 * _)
		}

		// TODO: Sort pages by their PageRank scores in descending order
		val test = ranks.sortBy(_._2,false)
		//ranks.collect.foreach(println)

		val final_output = test.map(f => ("[[" + f._1 +"]]\t"+ f._2))

 		// TODO: save the page title and pagerank scores in compressed format (save your disk space). Use “\t” as the delimiter.
 		final_output.saveAsTextFile("./pageranks", classOf[GzipCodec]) 
 		// final_output.saveAsTextFile("./pageranks")

		sc.stop()
    }
}