import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.similarity.CachingItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.util.Arrays;
import java.util.Collection;

public class NormalizedItemSimilarity implements ItemSimilarity {

    ItemSimilarity similarity;

    public NormalizedItemSimilarity(ItemSimilarity similarity) {
        this.similarity = similarity;
    }

    @Override
    public double itemSimilarity(long id1, long id2) throws TasteException {
        double sum = Arrays.stream(similarity.itemSimilarities(id1, similarity.allSimilarItemIDs(id1))).sum();
        double similarityValue = similarity.itemSimilarity(id1, id2);
        return sum == 0 || Double.isNaN(similarityValue) ? 0 : similarityValue / sum;
    }

    @Override
    public double[] itemSimilarities(long itemID1, long[] itemID2s) throws TasteException {
        double[] result = new double[itemID2s.length];
        for (int index = 0; index < itemID2s.length; index++) {
            result[index] = itemSimilarity(itemID1, itemID2s[index]);
        }
        return result;
    }

    @Override
    public long[] allSimilarItemIDs(long l) throws TasteException {
        return similarity.allSimilarItemIDs(l);
    }

    @Override
    public void refresh(Collection<Refreshable> collection) {
        throw new UnsupportedOperationException();
    }
}
