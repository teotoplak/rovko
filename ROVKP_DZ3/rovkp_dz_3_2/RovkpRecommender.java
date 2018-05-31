import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.io.File;
import java.util.function.Function;

abstract class RovkpRecommender implements RecommenderBuilder {

    private static final double TRAINING_PERCENTAGE = 0.3;
    private static final double EVALUATION_PERCENTAGE = 0.3;
    private static final long TEST_USER_ID = 220;
    private static final int TEST_NUM_OF_RECOMMENDATION = 10;

    static void start(String[] args, Function<String, RovkpRecommender> create) throws Exception {
        RovkpRecommender recommender = create.apply(args[0]);
        recommender.printRecommendations(TEST_USER_ID, TEST_NUM_OF_RECOMMENDATION);
        recommender.evaluate();
    }

    DataModel dataModel;

    RovkpRecommender(String directory) {
        try {
            dataModel = new FileDataModel(new File(directory + "/jester_ratings.dat"), "\\s+");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private void printRecommendations(long id, int n) throws Exception {
        Recommender recommender = buildRecommender(dataModel);
        System.out.printf("Recommendations for user %d%n (top %d)", n, id);
        recommender.recommend(id, n)
                .forEach(r -> System.out.printf("\t item ID:%3d score:%.3f%n", r.getItemID(), r.getValue()));
    }

    private void evaluate() throws Exception {
        RecommenderEvaluator recEvaluator = new RMSRecommenderEvaluator();
        double score = recEvaluator.evaluate(
                this,
                null,
                dataModel,
                TRAINING_PERCENTAGE,
                EVALUATION_PERCENTAGE
        );
        System.out.println("Recommender score: " + score);

        RecommenderIRStatsEvaluator statsEvaluator = new GenericRecommenderIRStatsEvaluator();
        IRStatistics stats = statsEvaluator.evaluate(
                this, null, dataModel, null, 2,
                GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0
        );
        System.out.printf("Precision: %.4f%n", stats.getPrecision());
        System.out.printf("Recall: %.4f%n", stats.getRecall());
    }

}