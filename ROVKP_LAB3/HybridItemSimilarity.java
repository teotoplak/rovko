
import org.apache.commons.lang3.ArrayUtils;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class HybridItemSimilarity implements ItemSimilarity {

    private ItemSimilarity similarity1;
    private ItemSimilarity similarity2;
    private int weightFactor1;
    private int weightFactor2;

    public HybridItemSimilarity(ItemSimilarity similarity1, int weightFactor1,
                                ItemSimilarity similarity2, int weightFactor2) {
        this.similarity1 = similarity1;
        this.similarity2 = similarity2;
        this.weightFactor1 = weightFactor1;
        this.weightFactor2 = weightFactor2;
    }

    @Override
    public double itemSimilarity(long itemID1, long itemID2) throws TasteException {
        double first = similarity1.itemSimilarity(itemID1,itemID2) * weightFactor1;
        double second = similarity2.itemSimilarity(itemID1,itemID2) * weightFactor2;
        return first + second;
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
    public long[] allSimilarItemIDs(long itemID) throws TasteException {
        Set<Long> ids = new HashSet<>();
        ids.addAll(LongStream.of(similarity1.allSimilarItemIDs(itemID)).boxed().collect(Collectors.toList()));
        ids.addAll(LongStream.of(similarity2.allSimilarItemIDs(itemID)).boxed().collect(Collectors.toList()));
        return ArrayUtils.toPrimitive(ids.toArray(new Long[ids.size()]));
    }

    @Override
    public void refresh(Collection<Refreshable> collection) {
        throw new UnsupportedOperationException();
    }
}
