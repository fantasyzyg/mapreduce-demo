
- demo0 : reduce output 选择 MultipleOutput
- demo1 : 自定义序列化对象实现 Writable 接口
- demo2 : 自定义Partitioner分区对象
- demo3 : 自定义Partitioner和Reduce端的GroupingComparator对象
- demo4 : 小文件合并，自定义InputFormat对象和RecordReader对象
- demo5 : join实现


### InputFormat 详解
一般默认对于`MapReduce`任务来说，默认的`InputFormat`是`TextInputFormat`


下面是关于任务启动的读取配置的一些class,在`org.apache.hadoop.mapreduce`包内
- `TextInputFormat` -> `FileInputFormat` -> `InputFormat`
- `FileSplit` -> `InputSplit`
- `LineRecordReader` -> `RecordReader`


下面关于如何启动任务的一些class，在`org.apache.hadoop.mapred`包内
- `JvmTask`
- `MapTask`  很重要的一个类，如果想要详细理解Map过程，可以精读这部分代码。
- `ReduceTask`也是一个十分重要的类，如果想详细理解Reduce过程，可以精读这部分代码。


```java


/** 
 * <code>InputFormat</code> describes the input-specification for a 
 * Map-Reduce job. 
 * 
 * <p>The Map-Reduce framework relies on the <code>InputFormat</code> of the
 * job to:<p>
 * <ol>
 *   <li>
 *   Validate the input-specification of the job. 
 *   <li>
 *   Split-up the input file(s) into logical {@link InputSplit}s, each of 
 *   which is then assigned to an individual {@link Mapper}.
 *   </li>
 *   <li>
 *   Provide the {@link RecordReader} implementation to be used to glean
 *   input records from the logical <code>InputSplit</code> for processing by 
 *   the {@link Mapper}.
 *   </li>
 * </ol>
 * 
 * <p>The default behavior of file-based {@link InputFormat}s, typically 
 * sub-classes of {@link FileInputFormat}, is to split the 
 * input into <i>logical</i> {@link InputSplit}s based on the total size, in 
 * bytes, of the input files. However, the {@link FileSystem} blocksize of  
 * the input files is treated as an upper bound for input splits. 
 
 // 切分文件也是具有一个上下界的 
    1. 上界（HDFS BlockSize）
    2. 下届可以通过参数配置
 
 A lower bound 
 * on the split size can be set via 
 * <a href="{@docRoot}/../hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml#mapreduce.input.fileinputformat.split.minsize">
 * mapreduce.input.fileinputformat.split.minsize</a>.</p>
 * 
 * <p>Clearly, logical splits based on input-size is insufficient for many 
 * applications since record boundaries are to respected. In such cases, the
 * application has to also implement a {@link RecordReader} on whom lies the
 * responsibility to respect record-boundaries and present a record-oriented
 * view of the logical <code>InputSplit</code> to the individual task.
 
 上面注释很有意义，getInputSplit返回splits交给每一个Mapper，这都是逻辑层面上的，但是RecordReader可以读取每一个InputSplit的
 数据交给Mapper的map函数进行处理。
 
 */




// InputFormat<K,V> 是一个抽象类,只有两个方法
public abstract class InputFormat<K, V> {

  /** 
   * Logically split the set of input files for the job.  
   * 
   * <p>Each {@link InputSplit} is then assigned to an individual {@link Mapper}
   * for processing.</p>
   *
   * <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
   * input files are not physically split into chunks. For e.g. a split could
   * be <i>&lt;input-file-path, start, offset&gt;</i> tuple. The InputFormat
   * also creates the {@link RecordReader} to read the {@link InputSplit}.
   * 
   * @param context job configuration.
   * @return an array of {@link InputSplit}s for the job.
   */
  public abstract 
    List<InputSplit> getSplits(JobContext context
                               ) throws IOException, InterruptedException;
  
  /**
   * Create a record reader for a given split. The framework will call
   * {@link RecordReader#initialize(InputSplit, TaskAttemptContext)} before
   * the split is used.   首先调用initialize方法，接收 InputSplit 和 MapTask 上下文Context
   * @param split the split to be read
   * @param context the information about the task
   * @return a new record reader
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract 
    RecordReader<K,V> createRecordReader(InputSplit split,
                                         TaskAttemptContext context
                                        ) throws IOException, 
                                                 InterruptedException;

}
```

> 如何构建一个Mapper的呢?

首先来看`JvmTask`，这其实就是一个Java虚拟机上的Task，实现序列化，实现Writable接口，根据任务类型来生成一个MapTask或者ReduceTask内部对象，这些对象包含了Task该有的信息了。然后根据Job Conf构建Mapper对象，获取对应的InputFormat获取对应的RecordReader，初始化RecordReader，不停读取InputSplit对应的文件内容，然后再交给Mapper.map方法去处理，InputSplit对应的就是FileSplit，保存的是该Block的基本信息，包括以下基本属性。

```java
  private Path file;       // 文件路径
  private long start;      // offest 起始位置
  private long length;     // block 块大小长度
  private String[] hosts;     // 该block在哪些主机上
  private SplitLocationInfo[] hostInfos;     // 主机的信息
```

针对RecordReader，我们来谈谈LineRecordReader。

```java
public class LineRecordReader extends RecordReader<LongWritable, Text> {

  private long start;
  private long pos;
  private long end;
  ......
  
  
  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    ......   // 根据压缩方式去构建 decompressor
    
    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    // 很重要的代码，会直接忽略开始的那一行
    if (start != 0) {
      start += in.readLine(new Text(), 0, maxBytesToConsume(start));
    }
    this.pos = start;
  }
  
  
  // 如果最后一行是被截断的，会顺利读完剩下的内容进来。
  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    // We always read one extra line, which lies outside the upper
    // split limit i.e. (end - 1)
    while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
      if (pos == 0) {
        newSize = skipUtfByteOrderMark();
      } else {
        newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));
        pos += newSize;
      }

      if ((newSize == 0) || (newSize < maxLineLength)) {
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + 
               (pos - newSize));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }
}
```

分析下`JobSubmitter`对象

```java
class JobSubmitter {

  // submitFs 连接HDFS对象
  // ClientProtocol  连接RM对象
  JobSubmitter(FileSystem submitFs, ClientProtocol submitClient) 
  throws IOException {
    this.submitClient = submitClient;
    this.jtFs = submitFs;
  }
  
  /*
   * @param job the configuration to submit
   * @param cluster the handle to the Cluster
  */
  JobStatus submitJobInternal(Job job, Cluster cluster) 
  throws ClassNotFoundException, InterruptedException, IOException {
    ......
    // job .staging 目录
    Path jobStagingArea JobSubmissionFiles.getStagingDir(cluster, conf);
    ......
    JobID jobId = submitClient.getNewJobID();
    job.setJobID(jobId);
    Path submitJobDir = new Path(jobStagingArea, jobId.toString());
    JobStatus status = null;   // 获取 MapReduce Job信息
    
    ......
    copyAndConfigureFiles(job, submitJobDir);  // 写Job信息到HDFS里面
    // Create the splits for the job
    LOG.debug("Creating splits at " + jtFs.makeQualified(submitJobDir));
    int maps = writeSplits(job, submitJobDir);  // 根据InputFormat划分split
    conf.setInt(MRJobConfig.NUM_MAPS, maps);   // 写进conf
    LOG.info("number of splits:" + maps);
    
    ......
    // Write job file to submit dir
    writeConf(conf, submitJobFile);
    
    ......
    status = submitClient.submitJob(jobId, submitJobDir.toString(), job.getCredentials());  // 获取job状态
  }
}
```