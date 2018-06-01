import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.File;

public class ContentBasedRecommender implements RecommenderBuilder {

    private ItemSimilarity similarity;

    public ContentBasedRecommender(String pathToFile) {
        similarity = new FileItemSimilarity(new File(pathToFile));
    }

    @Override
    public Recommender buildRecommender(DataModel dataModel) {
        return new GenericItemBasedRecommender(dataModel, similarity);
    }
}
