package HadoopCF;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.EntityEntityWritable;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.hadoop.item.AggregateAndRecommendReducer;
import org.apache.mahout.cf.taste.hadoop.item.ItemFilterAsVectorAndPrefsReducer;
import org.apache.mahout.cf.taste.hadoop.item.ItemFilterMapper;
import org.apache.mahout.cf.taste.hadoop.item.PartialMultiplyMapper;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.cf.taste.hadoop.item.SimilarityMatrixRowWrapperMapper;
import org.apache.mahout.cf.taste.hadoop.item.ToVectorAndPrefReducer;
import org.apache.mahout.cf.taste.hadoop.item.UserVectorSplitterMapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.apache.mahout.cf.taste.hadoop.similarity.item.ItemSimilarityJob;
import org.apache.mahout.cf.taste.hadoop.similarity.item.ItemSimilarityJob.MostSimilarItemPairsMapper;
import org.apache.mahout.cf.taste.hadoop.similarity.item.ItemSimilarityJob.MostSimilarItemPairsReducer;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.RowSimilarityJob;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasures;

public final class RecommenderJob extends AbstractJob {
    public static final String BOOLEAN_DATA = "booleanData";
    public static final String DEFAULT_PREPARE_PATH = "preparePreferenceMatrix";
    private static final int DEFAULT_MAX_SIMILARITIES_PER_ITEM = 100;
    private static final int DEFAULT_MAX_PREFS = 500;
    private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

    public RecommenderJob() {
    }

    public int run(String[] args) throws Exception {
        this.addInputOption();
        this.addOutputOption();
        this.addOption("numRecommendations", "n", "Number of recommendations per user", String.valueOf(10));
        this.addOption("usersFile", (String)null, "File of users to recommend for", (String)null);
        this.addOption("itemsFile", (String)null, "File of items to recommend for", (String)null);
        this.addOption("filterFile", "f", "File containing comma-separated userID,itemID pairs. Used to exclude the item from the recommendations for that user (optional)", (String)null);
        this.addOption("userItemFile", "uif", "File containing comma-separated userID,itemID pairs (optional). Used to include only these items into recommendations. Cannot be used together with usersFile or itemsFile", (String)null);
        this.addOption("booleanData", "b", "Treat input as without pref values", Boolean.FALSE.toString());
        this.addOption("maxPrefsPerUser", "mxp", "Maximum number of preferences considered per user in final recommendation phase", String.valueOf(10));
        this.addOption("minPrefsPerUser", "mp", "ignore users with less preferences than this in the similarity computation (default: 1)", String.valueOf(1));
        this.addOption("maxSimilaritiesPerItem", "m", "Maximum number of similarities considered per item ", String.valueOf(100));
        this.addOption("maxPrefsInItemSimilarity", "mpiis", "max number of preferences to consider per user or item in the item similarity computation phase, users or items with more preferences will be sampled down (default: 500)", String.valueOf(500));
        this.addOption("similarityClassname", "s", "Name of distributed similarity measures class to instantiate, alternatively use one of the predefined similarities (" + VectorSimilarityMeasures.list() + ')', true);
        this.addOption("threshold", "tr", "discard item pairs with a similarity value below this", false);
        this.addOption("outputPathForSimilarityMatrix", "opfsm", "write the item similarity matrix to this path (optional)", false);
        this.addOption("randomSeed", (String)null, "use this seed for sampling", false);
        this.addFlag("sequencefileOutput", (String)null, "write the output into a SequenceFile instead of a text file");
        Map parsedArgs = this.parseArguments(args);
        if(parsedArgs == null) {
            return -1;
        } else {
            Path outputPath = this.getOutputPath();
            int numRecommendations = Integer.parseInt(this.getOption("numRecommendations"));
            String usersFile = this.getOption("usersFile");
            String itemsFile = this.getOption("itemsFile");
            String filterFile = this.getOption("filterFile");
            String userItemFile = this.getOption("userItemFile");
            boolean booleanData = Boolean.valueOf(this.getOption("booleanData")).booleanValue();
            int maxPrefsPerUser = Integer.parseInt(this.getOption("maxPrefsPerUser"));
            int minPrefsPerUser = Integer.parseInt(this.getOption("minPrefsPerUser"));
            int maxPrefsInItemSimilarity = Integer.parseInt(this.getOption("maxPrefsInItemSimilarity"));
            int maxSimilaritiesPerItem = Integer.parseInt(this.getOption("maxSimilaritiesPerItem"));
            String similarityClassname = this.getOption("similarityClassname");
            double threshold = this.hasOption("threshold")?Double.parseDouble(this.getOption("threshold")):4.9E-324D;
            long randomSeed = this.hasOption("randomSeed")?Long.parseLong(this.getOption("randomSeed")):-9223372036854775808L;
            Path prepPath = this.getTempPath("preparePreferenceMatrix");
            Path similarityMatrixPath = this.getTempPath("similarityMatrix");
            Path explicitFilterPath = this.getTempPath("explicitFilterPath");
            Path partialMultiplyPath = this.getTempPath("partialMultiply");
            AtomicInteger currentPhase = new AtomicInteger();
            int numberOfUsers = -1;
            if(shouldRunNextPhase(parsedArgs, currentPhase)) {
                ToolRunner.run(this.getConf(), new PreparePreferenceMatrixJob(), new String[]{"--input", this.getInputPath().toString(), "--output", prepPath.toString(), "--minPrefsPerUser", String.valueOf(minPrefsPerUser), "--booleanData", String.valueOf(booleanData), "--tempDir", this.getTempPath().toString()});
                numberOfUsers = HadoopUtil.readInt(new Path(prepPath, "numUsers.bin"), this.getConf());
            }

            if(shouldRunNextPhase(parsedArgs, currentPhase)) {
                if(numberOfUsers == -1) {
                    numberOfUsers = (int)HadoopUtil.countRecords(new Path(prepPath, "userVectors"), PathType.LIST, (PathFilter)null, this.getConf());
                }

                ToolRunner.run(this.getConf(), new RowSimilarityJob(), new String[]{"--input", (new Path(prepPath, "ratingMatrix")).toString(), "--output", similarityMatrixPath.toString(), "--numberOfColumns", String.valueOf(numberOfUsers), "--similarityClassname", similarityClassname, "--maxObservationsPerRow", String.valueOf(maxPrefsInItemSimilarity), "--maxObservationsPerColumn", String.valueOf(maxPrefsInItemSimilarity), "--maxSimilaritiesPerRow", String.valueOf(maxSimilaritiesPerItem), "--excludeSelfSimilarity", String.valueOf(Boolean.TRUE), "--threshold", String.valueOf(threshold), "--randomSeed", String.valueOf(randomSeed), "--tempDir", this.getTempPath().toString()});
                if(this.hasOption("outputPathForSimilarityMatrix")) {
                    Path aggregateAndRecommendInput = new Path(this.getOption("outputPathForSimilarityMatrix"));
                    Job outputFormat = this.prepareJob(similarityMatrixPath, aggregateAndRecommendInput, SequenceFileInputFormat.class, MostSimilarItemPairsMapper.class, EntityEntityWritable.class, DoubleWritable.class, MostSimilarItemPairsReducer.class, EntityEntityWritable.class, DoubleWritable.class, TextOutputFormat.class);
                    Configuration aggregateAndRecommend = outputFormat.getConfiguration();
                    aggregateAndRecommend.set(ItemSimilarityJob.ITEM_ID_INDEX_PATH_STR, (new Path(prepPath, "itemIDIndex")).toString());
                    aggregateAndRecommend.setInt(ItemSimilarityJob.MAX_SIMILARITIES_PER_ITEM, maxSimilaritiesPerItem);
                    outputFormat.waitForCompletion(true);
                }
            }

            Job aggregateAndRecommendInput1;
            if(shouldRunNextPhase(parsedArgs, currentPhase)) {
                aggregateAndRecommendInput1 = new Job(this.getConf(), "partialMultiply");
                Configuration outputFormat1 = aggregateAndRecommendInput1.getConfiguration();
                MultipleInputs.addInputPath(aggregateAndRecommendInput1, similarityMatrixPath, SequenceFileInputFormat.class, SimilarityMatrixRowWrapperMapper.class);
                MultipleInputs.addInputPath(aggregateAndRecommendInput1, new Path(prepPath, "userVectors"), SequenceFileInputFormat.class, UserVectorSplitterMapper.class);
                aggregateAndRecommendInput1.setJarByClass(ToVectorAndPrefReducer.class);
                aggregateAndRecommendInput1.setMapOutputKeyClass(VarIntWritable.class);
                aggregateAndRecommendInput1.setMapOutputValueClass(VectorOrPrefWritable.class);
                aggregateAndRecommendInput1.setReducerClass(ToVectorAndPrefReducer.class);
                aggregateAndRecommendInput1.setOutputFormatClass(SequenceFileOutputFormat.class);
                aggregateAndRecommendInput1.setOutputKeyClass(VarIntWritable.class);
                aggregateAndRecommendInput1.setOutputValueClass(VectorAndPrefsWritable.class);
                outputFormat1.setBoolean("mapred.compress.map.output", true);
                outputFormat1.set("mapred.output.dir", partialMultiplyPath.toString());
                if(usersFile != null) {
                    outputFormat1.set("usersFile", usersFile);
                }

                if(userItemFile != null) {
                    outputFormat1.set("userItemFile", userItemFile);
                }

                outputFormat1.setInt("maxPrefsPerUserConsidered", maxPrefsPerUser);
                boolean aggregateAndRecommend1 = aggregateAndRecommendInput1.waitForCompletion(true);
                if(!aggregateAndRecommend1) {
                    return -1;
                }
            }

            if(shouldRunNextPhase(parsedArgs, currentPhase)) {
                if(filterFile != null) {
                    aggregateAndRecommendInput1 = this.prepareJob(new Path(filterFile), explicitFilterPath, TextInputFormat.class, ItemFilterMapper.class, VarLongWritable.class, VarLongWritable.class, ItemFilterAsVectorAndPrefsReducer.class, VarIntWritable.class, VectorAndPrefsWritable.class, SequenceFileOutputFormat.class);
                    boolean outputFormat2 = aggregateAndRecommendInput1.waitForCompletion(true);
                    if(!outputFormat2) {
                        return -1;
                    }
                }

                String aggregateAndRecommendInput2 = partialMultiplyPath.toString();
                if(filterFile != null) {
                    aggregateAndRecommendInput2 = aggregateAndRecommendInput2 + "," + explicitFilterPath;
                }

                Class outputFormat3 = parsedArgs.containsKey("--sequencefileOutput")?SequenceFileOutputFormat.class:TextOutputFormat.class;
                Job aggregateAndRecommend2 = this.prepareJob(new Path(aggregateAndRecommendInput2), outputPath, SequenceFileInputFormat.class, PartialMultiplyMapper.class, VarLongWritable.class, PrefAndSimilarityColumnWritable.class, AggregateAndRecommendReducer.class, VarLongWritable.class, RecommendedItemsWritable.class, outputFormat3);
                Configuration aggregateAndRecommendConf = aggregateAndRecommend2.getConfiguration();
                if(itemsFile != null) {
                    aggregateAndRecommendConf.set("itemsFile", itemsFile);
                }

                if(userItemFile != null) {
                    aggregateAndRecommendConf.set("userItemFile", userItemFile);
                }

                if(filterFile != null) {
                    setS3SafeCombinedInputPath(aggregateAndRecommend2, this.getTempPath(), partialMultiplyPath, explicitFilterPath);
                }

                setIOSort(aggregateAndRecommend2);
                aggregateAndRecommendConf.set("itemIDIndexPath", (new Path(prepPath, "itemIDIndex")).toString());
                aggregateAndRecommendConf.setInt("numRecommendations", numRecommendations);
                aggregateAndRecommendConf.setBoolean("booleanData", booleanData);
                boolean succeeded = aggregateAndRecommend2.waitForCompletion(true);
                if(!succeeded) {
                    return -1;
                }
            }

            return 0;
        }
    }

    private static void setIOSort(JobContext job) {
        Configuration conf = job.getConfiguration();
        conf.setInt("io.sort.factor", 100);
        String javaOpts = conf.get("mapred.map.child.java.opts");
        if(javaOpts == null) {
            javaOpts = conf.get("mapred.child.java.opts");
        }

        int assumedHeapSize = 512;
        if(javaOpts != null) {
            Matcher m = Pattern.compile("-Xmx([0-9]+)([mMgG])").matcher(javaOpts);
            if(m.find()) {
                assumedHeapSize = Integer.parseInt(m.group(1));
                String megabyteOrGigabyte = m.group(2);
                if("g".equalsIgnoreCase(megabyteOrGigabyte)) {
                    assumedHeapSize *= 1024;
                }
            }
        }

        conf.setInt("io.sort.mb", Math.min(assumedHeapSize / 2, 1024));
        conf.setInt("mapred.task.timeout", 3600000);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new org.apache.mahout.cf.taste.hadoop.item.RecommenderJob(), args);
    }
}