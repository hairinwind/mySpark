package my.spark.test;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import scala.Option;
import scala.Serializable;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * 
 * http://www.jesse-anderson.com/2016/04/unit-testing-spark-with-java/
 *
 */
public class TestExampleTest extends SharedJavaSparkContext implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	@Test
	public void testRunETL() {
		// Create and run the test
		List<String> input = Arrays.asList("1\tHeart", "2\tDiamonds");
		JavaRDD<String> inputRDD = jsc().parallelize(input);
		JavaPairRDD<String, Integer> result = TestExample.runETL(inputRDD);

		// Create the expected output
		List<Tuple2<String, Integer>> expectedInput = Arrays.asList(new Tuple2<String, Integer>("Heart", 1),
				new Tuple2<String, Integer>("Diamonds", 2));
		JavaPairRDD<String, Integer> expectedRDD = jsc().parallelizePairs(expectedInput);

		ClassTag<Tuple2<String, Integer>> tag = scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);

		// Run the assertions on the result and expected
		JavaRDDComparisons.assertRDDEquals(JavaRDD.fromRDD(JavaPairRDD.toRDD(result), tag),
				JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedRDD), tag));
	}

	@Test
	public void verifyFailureTest() {
		// Create and run the test
		List<String> input = Arrays.asList("1\tHeart", "2\tDiamonds");
		JavaRDD<String> inputRDD = jsc().parallelize(input);
		JavaPairRDD<String, Integer> result = TestExample.runETL(inputRDD);

		// Create the expected output
		List<Tuple2<String, Integer>> expectedInput = Arrays.asList(new Tuple2<String, Integer>("Heart", 1),
				new Tuple2<String, Integer>("Diamonds", 12));
		JavaPairRDD<String, Integer> expectedRDD = jsc().parallelizePairs(expectedInput);

		// Create ClassTag to allow class reflection
		ClassTag<Tuple2<String, Integer>> tag = scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);

		// Get the list of Tuple2s that don't match
		Option<Tuple2<Option<Tuple2<String, Integer>>, Option<Tuple2<String, Integer>>>> compareWithOrder = JavaRDDComparisons
				.compareRDDWithOrder(JavaRDD.fromRDD(JavaPairRDD.toRDD(result), tag),
						JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedRDD), tag));

		// Create the objects that don't match
		Option<Tuple2<String, Integer>> rightTuple = Option.apply(new Tuple2<String, Integer>("Diamonds", 2));
		Option<Tuple2<String, Integer>> wrongTuple = Option.apply(new Tuple2<String, Integer>("Diamonds", 12));

		Tuple2<Option<Tuple2<String, Integer>>, Option<Tuple2<String, Integer>>> together = new Tuple2<Option<Tuple2<String, Integer>>, Option<Tuple2<String, Integer>>>(
				rightTuple, wrongTuple);

		// Assert the right and wrong values
		Option<Tuple2<Option<Tuple2<String, Integer>>, Option<Tuple2<String, Integer>>>> wrongValue = Option
				.apply(together);
		assertEquals(wrongValue, compareWithOrder);
	}

}
