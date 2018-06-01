import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

public class CollaborativeItemSimilarity implements ItemSimilarity {

    private final double[][] matrix;
    private final double[] norms;
    private final Map<Integer, Long> seqIdMap;
    private final Map<Long, Integer> idSeqMap;

    public CollaborativeItemSimilarity(DataModel model) throws TasteException {
        int n = model.getNumItems();
        matrix = new double[n][n];
        norms = new double[n];
        seqIdMap = new HashMap<>();
        idSeqMap = new HashMap<>();

        calculateCollaborativeModelMatrix(model);
    }

    private void calculateCollaborativeModelMatrix(DataModel model) throws TasteException {

        int counter = 0;
        LongPrimitiveIterator iterator = model.getUserIDs();
        while (iterator.hasNext()) {
            long userId = iterator.nextLong();

            for (long ratedItemId1 : model.getItemIDsFromUser(userId)) {

                //get correct item1 seq num
                Integer seqId1 = idSeqMap.get(ratedItemId1);
                if (seqId1 == null) {
                    seqId1 = counter++;
                    seqIdMap.put(seqId1, ratedItemId1);
                    idSeqMap.put(ratedItemId1, seqId1);
                }

                norms[seqId1] += Math.pow(model.getPreferenceValue(userId, ratedItemId1), 2);

                for (long ratedItemId2 : model.getItemIDsFromUser(userId)) {

                    //get correct item2 seq num
                    Integer seqId2 = idSeqMap.get(ratedItemId2);
                    if (seqId2 == null) {
                        seqId2 = counter++;
                        seqIdMap.put(seqId2, ratedItemId2);
                        idSeqMap.put(ratedItemId2, seqId2);
                    }

                    matrix[seqId1][seqId2] += model.getPreferenceValue(userId, ratedItemId1)
                            * model.getPreferenceValue(userId, ratedItemId2);
                }
            }
        }

        //get cosine similarity from similarity sums
        for (int seqId1 = 0; seqId1 < matrix.length; seqId1++) {
            for (int seqId2 = 0; seqId2 < matrix.length; seqId2++) {
                if (matrix[seqId1][seqId2] != 0) {
                    matrix[seqId1][seqId2] /= Math.sqrt(norms[seqId1]) * Math.sqrt(norms[seqId2]);
                }
            }
        }

    }

    /**
     * Returns the degree of similarity, of two items, based on the preferences that users have expressed for the items.
     */
    @Override
    public double itemSimilarity(long itemID1, long itemID2) throws TasteException {
        if (!idSeqMap.containsKey(itemID1) || !idSeqMap.containsKey(itemID2)) {
            return 0;
        }
        return matrix[idSeqMap.get(itemID1)][idSeqMap.get(itemID2)];
    }

    /**
     * A bulk-get version of itemSimilarity(long, long).
     */
    @Override
    public double[] itemSimilarities(long itemID1, long[] itemID2s) throws TasteException {
        double[] result = new double[itemID2s.length];
        for (int index = 0; index < itemID2s.length; index++) {
            result[index] = itemSimilarity(itemID1, itemID2s[index]);
        }
        return result;
    }

    /**
     * all IDs of similar items, in no particular order
     */
    @Override
    public long[] allSimilarItemIDs(long itemID) throws TasteException {
        if (!idSeqMap.containsKey(itemID)) {
            return new long[0];
        }
        List<Long> result = new LinkedList<>();
        double[] similarities = matrix[idSeqMap.get(itemID)];
        for (int i = 0; i < similarities.length; i++) {
            if(!Double.isNaN(similarities[i])) {
                result.add(seqIdMap.get(i));
            }
        }
        return result.stream().mapToLong(l -> l).toArray();
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}