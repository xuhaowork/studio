package org.apache.spark.binary.split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CombineFileInputFormat<K, V>
  extends FileInputFormat<K, V> {

  public static final String NUM_INPUT_FILES =
          "mapreduce.input.fileinputformat.numinputfiles";

  long  frameSize;
  long  maxSize;

  private static final double SPLIT_SLOP = 1.1;   // 10% slop
  private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

  // 需要设置最小值与最大值，最小值不得小于最大帧长。

    public void  setFrameSize(long frameSize) {
        this.frameSize=frameSize;
    }

    public  long getFrameSize() {
        return frameSize;
    }

    public void  setInitSize(long initSize) {
        this.maxSize=initSize;
    }

    public long  getInitSize() {
        return  maxSize;
    }


  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

      long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
      // 最小值不得小于最大帧长
      minSize=Math.max(minSize,frameSize);

    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          FileSystem fs = path.getFileSystem(job.getConfiguration());
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(job, path)) {
          long blockSize = file.getBlockSize();
         // long splitSize = computeSplitSize(blockSize, minSize, maxSize);

          long splitSize = maxSize;
          System.out.println("切分大小为："+splitSize);

          long bytesRemaining = length;

          long  comVal=0L;

          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {


            comVal=length-bytesRemaining;

            int blkIndex = getBlockIndex(blkLocations, comVal);


            splits.add(addCreatedSplit(path, comVal, splitSize+frameSize,
                    blkLocations[blkIndex].getHosts()));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {

            comVal=length-bytesRemaining;

            int blkIndex = getBlockIndex(blkLocations, comVal);
            splits.add(addCreatedSplit(path, comVal, bytesRemaining,
                    blkLocations[blkIndex].getHosts()));
          }
        } else { // not splitable
          splits.add(addCreatedSplit(path, 0, length, blkLocations[0].getHosts()));
        }

      } else {
        //Create empty hosts array for zero length files
        splits.add(addCreatedSplit(path, 0, length, new String[0]));
      }
    }
    // Save the number of input files for metrics/loadgen
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
    LOG.debug("Total # of splits: " + splits.size());
    return splits;
  }

    public CombineFileSplit addCreatedSplit(Path file, long start, long length, String[] hosts) {

       Path[] fileArr=new Path[1];
       fileArr[0]=file;

       long[] startArr=new long[1];
       startArr[0]=start;

       long[] lengthArr=new long[1];
       lengthArr[0]= length;

      return new CombineFileSplit(fileArr, startArr, lengthArr, hosts);

    }



  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter() {
      this.filters = new ArrayList<PathFilter>();
    }

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public void add(PathFilter one) {
      filters.add(one);
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (filter.accept(path)) {
          return true;
        }
      }
      return false;
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append("[");
      for (PathFilter f: filters) {
        buf.append(f);
        buf.append(",");
      }
      buf.append("]");
      return buf.toString();
    }
  }

}
