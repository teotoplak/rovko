import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.File;

public class ContentBasedRecommender extends RovkpRecommender {


    public static void main(String[] args) throws Exception {
        RovkpRecommender.start(args, ContentBasedRecommender::new);
    }

    private ItemSimilarity similarity;

    private ContentBasedRecommender(String directory) {
        super(directory);
        similarity = new FileItemSimilarity(new File(directory + "/items_similarity.csv"));
    }

    @Override
    public Recommender buildRecommender(DataModel dataModel) {
        return new GenericItemBasedRecommender(dataModel, similarity);
    }
}
