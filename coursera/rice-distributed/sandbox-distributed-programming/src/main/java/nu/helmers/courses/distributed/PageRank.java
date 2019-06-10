package nu.helmers.courses.distributed;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A single iteration of the iterative PageRank algorithm using Apache Spark.
 *
 * https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html
 *
 * Spark is a tool for distributed computing. Spark can submit tasks to computers in a cluster using a cluster manager
 * (like Mesos or Kubernetes). For testing purposes, you can run Spark in standalone mode. More information:
 *
 * https://spark.apache.org/docs/2.2.0/cluster-overview.html
 * https://spark.apache.org/docs/2.2.0/submitting-applications.html
 *
 * The main parts to identify in a Spark program running in cluster mode are the Driver Program (the main program
 * containing a SparkContext that controls the cluster) and the Application Code (containing the actual computation;
 * typically packaged as a JAR, that is submitted by the driver to cluster nodes).
 *
 * This class is an example of Spark Application Code. It has no awareness that it may be running in a cluster.
 */
public final class PageRank {
    private PageRank() {
    }

    /**
     * Given an RDD of websites and their ranks, compute new ranks for all websites and return a new RDD containing
     * the updated ranks.
     *
     * Given a website B with many other websites linking to it, the rank for B is the sum over all source websites
     * of the rank of the source website divided by the number of outbound links from the source website.
     * This new rank is damped by multiplying it by 0.85 and adding that to 0.15:
     *
     *   new_rank(B) = 0.15 + 0.85 * sum(rank(A) / out_count(A)) for all A linking to B
     *
     * For this assignment, you are responsible for implementing this PageRank
     * algorithm using the Spark Java APIs.
     *
     * @param pages The connectivity of the website graph, keyed on unique website IDs.
     * @param ranks The current ranks of each website, keyed on unique website IDs.
     * @return The new ranks of the websites graph, using the PageRank algorithm to update site ranks.
     */
    public static JavaPairRDD<Integer, Double> calculate(
            final JavaPairRDD<Integer, WebPage> pages,
            final JavaPairRDD<Integer, Double> ranks) {

        return calculatePageRank(pages, ranks);
    }

    private static JavaPairRDD<Integer, Double> calculatePageRank(JavaPairRDD<Integer, WebPage> pages, JavaPairRDD<Integer, Double> ranks) {
        JavaPairRDD<Integer, Double> newRanks = pages.join(ranks)
                .flatMapToPair(pair -> {
                    // For every page, create a list assigning the contribution of this page (value) to each destination
                    // page (key).
                    WebPage page = pair._2()._1();
                    Double rank = pair._2()._2();

                    List<Tuple2<Integer, Double>> contributions = new LinkedList<>();
                    Iterator<Integer> destinations = page.destinations();
                    double destinationCount = (double) page.getDestinationCount();

                    while (destinations.hasNext()) {
                        Integer destinationPageId = destinations.next();
                        double contribution = rank / destinationCount;

                        contributions.add(new Tuple2<>(destinationPageId, contribution));
                    }

                    // Because we return a list (generally a page has multiple destination pages) we flatten the result.
                    return contributions;
                });

        // Since a page can be referenced from different pages, the resulting JavaPairRDD can contain multiple entries
        // for the same pageId (one contribution for each source page):
        return newRanks.reduceByKey((contrib1, contrib2) -> contrib1 + contrib2)
                // Apply damping factor:
                .mapValues(c -> 0.15 + (0.85 * c));
    }

    private static JavaPairRDD<Integer, Double> calculatePageRankVerbose(JavaPairRDD<Integer, WebPage> sites, JavaPairRDD<Integer, Double> ranks) {
        return sites.join(ranks)
        // Result: JavaPairRDD<Integer, Tuple2<WebPage, Double>>
        // (key remains the same, values are combined into a Tuple2)

        // Step 2: For current web page, we need its rank and number of outgoing links (destinationCount).
        // This information (rank, destinationCount) is needed to determine the rank of each destination.
        .flatMapValues(webpage_rank -> {
            WebPage webpage = webpage_rank._1();
            Double rank = webpage_rank._2(); // rank of current web page

            int destinationCount = webpage.getDestinationCount(); // number of outgoing links from current web page

            List<Tuple3> destinations = new ArrayList<>();
            while (webpage.destinations().hasNext()) {
                Integer destinationPageId = webpage.destinations().next();
                destinations.add(new Tuple3<>(destinationPageId, destinationCount, rank));
            }
            return destinations;
            // Because we use flatMap, the result values will be of type Tuple3 (and not List<Tuple3>).
        })
        // result: JavaPairRDD<sourcePageId, Tuple3<destinationPageId, destinationCount, rank>>

        // Step 3: we're not interested in the sourcePageId anymore. Transform
        // from (sourcePageId -> (destinationPageId, destinationCount, rank))
        // to (destinationPageId -> (destinationCount, rank))
        // In this process, also map (destinationCount, rank) to rank/destinationCount (the contribution).
        .mapToPair(new PairFunction<Tuple2<Integer, Tuple3>, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple3> tuple2) throws Exception {
                // Ignore tuple2._1() (= sourcePageId).
                Tuple3<Integer, Integer, Double> value = tuple2._2();

                Integer destinationPageId = value._1();
                Integer destinationCount = value._2();
                Double sourcePageRank = value._3();

                Double contribution = sourcePageRank / (double) destinationCount;
                return new Tuple2<>(destinationPageId, contribution);
            }
        })
        // Result: JavaPairRDD<pageId, contribution>

        // Step 4: Since pageId may occur more than once, reduce by summing the contributions.
        .reduceByKey((contrib1, contrib2) -> contrib1 + contrib2)
        // result: JavaPairRDD with unique pageIds and (uncorrected) contributions.

        // Step 5: Apply correction factor to contribution
        .mapValues(c -> (0.15 + 0.85 * c));
    }
}
