import org.apache.commons.lang3.StringEscapeUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;


public class Rovkp {

    private static final String OUTPUT_PATH = "items_similarity.csv";

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Expecting path to directory with files");
            System.exit(1);
        }

        // 1) parsing text file to map
        Map<Integer,String> jokes = parseJokesDocument(Paths.get(args[0]));

        // 2) create and index Lucene documents
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory index = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(index, config);
        for (Map.Entry<Integer, String> e : jokes.entrySet()) {
            writer.addDocument(indexDocumentLucene(e));
        }
        writer.close();

        // 3) calculate similarities between Lucene documents to matrix
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        float[][] similarityMatrix = calculateSimilarity(jokes, new QueryParser("text", analyzer), searcher);
        reader.close();

        // 4) normalize matrix
        similarityMatrix = normalizeSimilarityMatrix(similarityMatrix);

        // 5) write matrix to file
        int numLines = writeToFile(similarityMatrix, OUTPUT_PATH);


        System.out.println("Number of lines " + numLines);
        Integer mostSimilarToID1 = mostSimilarID(similarityMatrix, 1);
        System.out.println("Most similiar to ID 1 is: " + mostSimilarToID1);

    }

    private static int mostSimilarID(float[][] matrix, int ID) {
        int id = -1;
        float similar = 0;
        for (int i = 0; i < matrix[ID].length; i++) {
            if (i == ID) {
                continue;
            }
            if (matrix[ID][i] > similar) {
                similar = matrix[ID][i];
                id = i;
            }
        }
        return id;
    }

    private static Map<Integer, String> parseJokesDocument(Path path) {

        Map<Integer, String> jokes = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            StringBuilder builder = new StringBuilder();

            while (true) {
                builder.setLength(0);
                String idString = reader.readLine();
                if (idString == null) {
                    break;
                }
                int id = Integer.parseInt(idString.substring(0, idString.indexOf(":")));

                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        break;
                    }
                    builder.append(line);
                }

                String joke = StringEscapeUtils.unescapeXml(builder.toString().toLowerCase().replaceAll("<.*?>", ""));

                jokes.put(id, joke);
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        return jokes;
    }

    private static Document indexDocumentLucene(Map.Entry<Integer, String> entry) {

        FieldType idFieldType = new FieldType();
        idFieldType.setStored(true);
        idFieldType.setTokenized(false);
        idFieldType.setIndexOptions(IndexOptions.NONE);

        FieldType textFieldType = new FieldType();
        textFieldType.setStored(false);
        textFieldType.setTokenized(true);
        textFieldType.setIndexOptions(IndexOptions.DOCS);

        Document doc = new Document();
        doc.add(new Field("ID", entry.getKey().toString(), idFieldType));
        doc.add(new Field("text", entry.getValue(), textFieldType));
        return doc;
    }

    private static float[][] calculateSimilarity(Map<Integer, String> jokes, QueryParser parser, IndexSearcher searcher)
            throws IOException, ParseException {

        int n = jokes.size();
        float[][] similarity = new float[n + 1][n + 1];

        for (Map.Entry<Integer, String> e : jokes.entrySet()) {
            int i = e.getKey();
            Query query = parser.parse(QueryParser.escape(e.getValue()));
            TopDocs docs = searcher.search(query, n);
            for (ScoreDoc hit : docs.scoreDocs) {
                int j = Integer.parseInt(searcher.doc(hit.doc).get("ID"));
                similarity[i][j] = hit.score;
            }
        }

        return similarity;
    }

    private static float[][] normalizeSimilarityMatrix(float[][] matrix) {

        // transforming values to wanted interval
        for (int i = 0; i < matrix.length; i++) {
            // similarity is biggest with its own self
            float maxRowSimilarity = matrix[i][i];
            if (maxRowSimilarity > 0) {
                for (int j = 0; j < matrix[i].length; j++) {
                    matrix[i][j] /= maxRowSimilarity;
                }
            }
        }

        // similarity between two documents is their i & j cell average
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j <= i; j++) {
                float x = (matrix[i][j] + matrix[j][i]) / 2;
                matrix[i][j] = matrix[j][i] = x;
            }
        }

        return matrix;
    }

    private static int writeToFile(float[][] matrix, String filename) {
        int numLines = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filename))) {
            for (int i = 0; i < matrix.length; i++) {
                for (int j = i + 1; j < matrix[i].length; j++) {
                    if (matrix[i][j] > 0) {
                        writer.write(String.format("%d,%d,%f%n", i, j, matrix[i][j]));
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
