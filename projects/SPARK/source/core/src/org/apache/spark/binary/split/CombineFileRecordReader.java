package org.apache.spark.binary.split;

import java.io.*;
import java.lang.reflect.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;


public class CombineFileRecordReader<K, V> extends RecordReader<K, V> {

    public static final String MAP_INPUT_FILE = "mapreduce.map.input.file";

    public static final String MAP_INPUT_PATH = "mapreduce.map.input.length";

    public static final String MAP_INPUT_START = "mapreduce.map.input.start";


    static final Class [] constructorSignature = new Class []
            {CombineFileSplit.class,
                    TaskAttemptContext.class,
                    Integer.class};

    protected CombineFileSplit split;
    protected Class<? extends RecordReader<K,V>> rrClass;
    protected Constructor<? extends RecordReader<K,V>> rrConstructor;
    protected FileSystem fs;
    protected TaskAttemptContext context;

    protected int idx;
    protected long progress;
    protected RecordReader<K, V> curReader;

    public void initialize(InputSplit split,
                           TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (CombineFileSplit)split;
        this.context = context;
        if (null != this.curReader) {
            this.curReader.initialize(split, context);
        }
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {

        while ((curReader == null) || !curReader.nextKeyValue()) {
            if (!initNextRecordReader()) {
                return false;
            }
        }
        return true;
    }

    public K getCurrentKey() throws IOException, InterruptedException {
        return curReader.getCurrentKey();
    }

    public V getCurrentValue() throws IOException, InterruptedException {
        return curReader.getCurrentValue();
    }

    public void close() throws IOException {
        if (curReader != null) {
            curReader.close();
            curReader = null;
        }
    }

    /**
     * return progress based on the amount of data processed so far.
     */
    public float getProgress() throws IOException, InterruptedException {
        long subprogress = 0;    // bytes processed in current split
        if (null != curReader) {
            // idx is always one past the current subsplit's true index.
            subprogress = (long)(curReader.getProgress() * split.getLength(idx - 1));
        }
        return Math.min(1.0f,  (progress + subprogress)/(float)(split.getLength()));
    }

    /**
     * A generic RecordReader that can hand out different recordReaders
     * for each chunk in the CombineFileSplit.
     */
    public CombineFileRecordReader(CombineFileSplit split,
                                   TaskAttemptContext context,
                                   Class<? extends RecordReader<K,V>> rrClass)
            throws IOException {
        this.split = split;
        this.context = context;
        this.rrClass = rrClass;
        this.idx = 0;
        this.curReader = null;
        this.progress = 0;

        try {
            rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
            rrConstructor.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(rrClass.getName() +
                    " does not have valid constructor", e);
        }
        initNextRecordReader();
    }


    protected boolean initNextRecordReader() throws IOException {

        if (curReader != null) {
            curReader.close();
            curReader = null;
            if (idx > 0) {
                progress += split.getLength(idx-1);    // done processing so far
            }
        }

        // if all chunks have been processed, nothing more to do.
        if (idx == split.getNumPaths()) {
            return false;
        }


        try {
            Configuration conf = context.getConfiguration();

           /* conf.set(MRJobConfig.MAP_INPUT_FILE, split.getPath(idx).toString());
            conf.setLong(MRJobConfig.MAP_INPUT_START, split.getOffset(idx));
            conf.setLong(MRJobConfig.MAP_INPUT_PATH, split.getLength(idx));*/

            conf.set(MAP_INPUT_FILE, split.getPath(idx).toString());
            conf.setLong(MAP_INPUT_START, split.getOffset(idx));
            conf.setLong(MAP_INPUT_PATH, split.getLength(idx));


            curReader =  rrConstructor.newInstance(new Object []
                    {split, context, Integer.valueOf(idx)});

            if (idx > 0) {
                // initialize() for the first RecordReader will be called by MapTask;
                // we're responsible for initializing subsequent RecordReaders.
                curReader.initialize(split, context);
            }
        } catch (Exception e) {
            throw new RuntimeException (e);
        }
        idx++;
        return true;
    }
}
