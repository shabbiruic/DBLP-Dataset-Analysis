package Utils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Reads the records that are delimited by a specific begin/end tag.
 */
public class XMLInputFormat extends TextInputFormat {

//    name of configuration key which needs to be read for fetching the start and end tags of records
//    which are to be passed to the mapper.
    public static final String START_TAG_KEY = "xmlTags.start";
    public static final String END_TAG_KEY = "xmlTags.end";

    public static Logger logger = LoggerFactory.getLogger(XMLInputFormat.class);

    @Override
    /**
     * which gives the RecordReader which is used to read the data from Input file
     */
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit inputSplit,
                                                              TaskAttemptContext context) {
            return new XmlRecordReader();
    }

    /**
     * XMLRecordReader class to read through a given xml document to output xml
     * blocks as records as specified by the start tag and end tag
     *
     */
    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private byte[][] startTags;
        private  byte[][] endTags;
        private HashMap<byte[],byte[]> startEndTagMap = new HashMap<>();
        private  long start;
        private  long end;
        private  FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();
        private byte[] matchTag;

        @Override
        /**
         * Initialize the Record Reader i.e. with input file location and what are the starting and ending of
         * a vaild records.
         */
        public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException, InterruptedException {

            FileSplit split = (FileSplit)inputsplit;
            String strStartTags[] = MyConfig.getStringList(START_TAG_KEY).stream().toArray(String[]::new);
            String strEndTags[] = MyConfig.getStringList(END_TAG_KEY).stream().toArray(String[]::new);

            startTags = new byte[strStartTags.length][];
            endTags = new byte[strEndTags.length][];

            for(int i=0;i<strStartTags.length;i++)
                startTags[i] = strStartTags[i].getBytes("utf-8");

            for(int i=0;i<strEndTags.length;i++)
            {
                endTags[i] = strEndTags[i].getBytes("utf-8");
                startEndTagMap.put(startTags[i],endTags[i]);
            }

            logger.info("start and end tags got laoded");
            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(context.getConfiguration());
            fsin = fs.open(split.getPath());
            fsin.seek(start);
            logger.info("Initizalize method executed successfully");
        }

        @Override
        /**
         * reads the input file until the required start tags is not found and
         * once it has found it then keeps on pushing the data to buffer untill the end tag.
         */
        public boolean nextKeyValue() throws IOException, InterruptedException{
            if (fsin.getPos() < end) {
                    if (readUntilMatch(startTags, false)) {
                        try {
                            buffer.write(matchTag);
                            matchTag = startEndTagMap.get(matchTag);
                            if (readUntilMatch(startTags, true)) {
                                key.set(fsin.getPos());
                                value.set(buffer.getData(), 0, buffer.getLength());
                                logger.info("pushed one record");
                                return true;
                            }
                        } finally {
                            buffer.reset();
                        }
                    }

            }

            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }


        public long getPos() throws IOException {
            return fsin.getPos();
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        @Override
        public float getProgress() throws IOException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        /**
         *
         * @param tags  denotes the valid start tags
         * @param withinBlock tells are we reading inside the valid block or we are in search of valid block
         * @return
         * @throws IOException
         */
        private boolean readUntilMatch(byte[][] tags, boolean withinBlock) throws IOException {
            int i = 0;
            int[] counter = new int[tags.length];
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) return false;
                // save to buffer:
                if (withinBlock) {
                    buffer.write(b);

                if (b == matchTag[i])
                    i++;
                else
                    i = 0;
                if(i >= matchTag.length)
                    return true;
            }



                // check if we're matching:
                else {
                    for (int pos = 0; pos < tags.length; pos++) {
                        if (b == tags[pos][counter[pos]]) {
                            counter[pos]++;
                            if (counter[pos] >= tags[pos].length) {
                                matchTag = tags[pos];
                                return true;
                            }
                        }
                        else
                        {
                            counter[pos]=0;
                        }
                    }
                }

                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && fsin.getPos() >= end)
                {
                    logger.info("come out of record read loop");
                    return false;
                }
            }
        }
    }
}