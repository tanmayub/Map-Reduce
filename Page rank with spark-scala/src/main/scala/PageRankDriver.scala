import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

object PageRankDriver {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("pagerank2")
        val sc = new SparkContext(conf)
        val input = sc.textFile(args(0))
        def hp = new HtmlParser()
        val resWODangling = input.map(line => hp.process(line))
                .filter(field => field.get(0).indexOf("~") < 0
                        && !field.get(0).trim().isEmpty).persist()

        val keys = resWODangling.map(key => (key.get(0),"[]"))
                .distinct()
        val dangling = resWODangling.map(v => v.get(1))
                .distinct()
                .flatMap(x => x.split(","))
                .map(x => (x, "[]"))
                .distinct()
                .subtractByKey(keys)

        /*val finalRes = resWODangling.map(x => (x.get(0) + ":", "[" + x.get(1) + "]"))
                .union(dangling.map(x => (x._1 + ":", "[]")))*/

        val finalRes = resWODangling.map(x => (x.get(0), x.get(1)))
                .union(dangling.map(x => (x._1, "[]"))).persist()

        /*val finalRes = sc.makeRDD(List(("A","B,D"),("B","C,D"),
            ("C","A,D"),("D","")))*/
        val countTotalRes = finalRes.count()
        //val finalRes = intrRes.map(x => (x._1, x._2, (1.toFloat/countTotalRes)))

        var ranks = finalRes.mapValues(v => 1.0/countTotalRes)

        //write to file
        //finalRes.map(x => x._1 + x._2).saveAsTextFile(args(1))

        //run page rank 10 times
        var i = 0
        var delta = 0.0
        while (i < 10) {
            //dangling
            val intr = finalRes.join(ranks).values
                    .filter(x => x._1.split(",").length == 1 && x._1.equals(""))
                    .map(x => x._2)
                    .sum()

            //not dangling
            val contri = finalRes.join(ranks).values
                    .filter(x => x._1.split(",").length != 1 && !x._1.equals(""))
                    .flatMap {
                        //when adjacency list is not empty//update page rank
                        case (urls, rank) =>
                            val s = urls.split(",").length
                            urls.split(",").map(x => (x, rank/s))
                    }

            delta = delta + intr/countTotalRes
            ranks = contri.reduceByKey(_ + _)
                    .mapValues(0.15/countTotalRes + 0.85 * _ + delta)
            i = i+1
        }
        //after 10 page rank iterations
        //ranks.map(x => x).saveAsTextFile(args(1))

        //write top k to file
        val op = sc.makeRDD(ranks.collect().sortBy(- _._2).take(100))
        //op.saveAsTextFile(args(1))
        op.map(x => System.out.println(x))

        sc.stop()
    }
}