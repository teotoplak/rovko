import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CachingUserSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class CollaborativeRecommender extends RovkpRecommender {

    private static final double MIN_SIMILARITY = 0.1;

    public static void main(String[] args) throws Exception {
        RovkpRecommender.start(args, CollaborativeRecommender::new);
    }

    private CollaborativeRecommender(String dir) {
        super(dir);
    }

    @Override
    public Recommender buildRecommender(DataModel dataModel) throws TasteException {
        // or use log-likelihood correlation here
        UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
        similarity = new CachingUserSimilarity(similarity, dataModel);
        UserNeighborhood neighborhood = new NearestNUserNeighborhood(dataModel.getNumUsers(), MIN_SIMILARITY, similarity, dataModel);
        return new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
    }
}
