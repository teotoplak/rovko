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
import java.io.IOException;
import java.util.function.Function;

abstract class EvaluateRecommenders implements RecommenderBuilder {

    private static final String CONTENT_BASED_SIMILARITY_FILE_LOCATION = "items_similarity.csv";
    private static final String HYBRID_SIMILARITY_FILE_LOCATION = "hybrid_items_similarity.csv";
    private static final String PATH_TO_DATA_MODEL_FILE = "jester_ratings.dat";

    private static final double TRAINING_PERCENTAGE = 0.3;
    private static final double EVALUATION_PERCENTAGE = 0.3;

    public static void main(String[] args) throws IOException, Exception {
        DataModel dataModel = new FileDataModel(new File(PATH_TO_DATA_MODEL_FILE), "\\s+");

        CollaborativeRecommender collaborativeRecommender = new CollaborativeRecommender();
        ContentBasedRecommender contentBasedRecommender = new ContentBasedRecommender(CONTENT_BASED_SIMILARITY_FILE_LOCATION);
        HybridRecommender hybridRecommender = new HybridRecommender(HYBRID_SIMILARITY_FILE_LOCATION);

        // evaluate all three recommenders
        evaluate(dataModel, collaborativeRecommender);
        evaluate(dataModel, contentBasedRecommender);
        evaluate(dataModel, hybridRecommender);
    }

    private static void evaluate(DataModel dataModel, RecommenderBuilder recommender) throws Exception {
        RecommenderEvaluator recEvaluator = new RMSRecommenderEvaluator();
        double score = recEvaluator.evaluate(
                recommender,
                null,
                dataModel,
                TRAINING_PERCENTAGE,
                EVALUATION_PERCENTAGE
        );
        System.out.println("Recommender score: " + score);

    }

}