import org.apache.commons.lang.NotImplementedException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class CreateHybridItemSimilarity {

    private static final int COLLABORATIVE_WEIGHT_FACTOR = 3;
    private static final int CONTENT_WEIGHT_FACTOR = 2;
    private static final String OUTPUT_FILE = "hybrid_items_similarity.csv";

    public static void main(String[] args) throws TasteException, IOException {

        // load item similarities
        FileDataModel collaborativeDataModel = new FileDataModel(new File("jester_ratings.dat"), "\\s+");
        CollaborativeItemSimilarity collaborativeSimilarity = new CollaborativeItemSimilarity(collaborativeDataModel);
        ItemSimilarity contentSimilarity = new FileItemSimilarity(new File( "items_similarity.csv"));

        // normalized item similarities
        ItemSimilarity normalizedCollaborativeSimilarity = new NormalizedItemSimilarity(collaborativeSimilarity);
        ItemSimilarity normalizedContentSimilarity = new NormalizedItemSimilarity(contentSimilarity);

        // hybrid version of item similarities
        ItemSimilarity hybridItemSimilarity = new HybridItemSimilarity(
                normalizedCollaborativeSimilarity, COLLABORATIVE_WEIGHT_FACTOR,
                normalizedContentSimilarity, CONTENT_WEIGHT_FACTOR);

        // write hybrid similarities to file
        FileDataModel itemsDataModel = new FileDataModel(new File("items_similarity.csv"), ",\\s*");
        writeItemSimilarityToCSV(hybridItemSimilarity, itemsDataModel.getUserIDs(), OUTPUT_FILE);

    }

    private static int writeItemSimilarityToCSV(ItemSimilarity similarity, LongPrimitiveIterator allItemIds,
                                                 String fileName) throws TasteException {
        int numLines = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName))) {
            while (allItemIds.hasNext()) {
                long id = allItemIds.nextLong();
                long[] similarItemIds = similarity.allSimilarItemIDs(id);
                for (long similarItemId : similarItemIds) {
                    // if id's were not compared yet
                    if (similarItemId > id) {
                        writer.write(String.format("%d,%d,%f%n", id, similarItemId, similarity.itemSimilarity(id,similarItemId)));
                        numLines++;
                    }
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return numLines;
    }
}
