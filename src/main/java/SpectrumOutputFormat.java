import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpectrumOutputFormat extends FileOutputFormat<Text, RawSpectrum> {

  @Override
  public RecordWriter<Text, RawSpectrum> getRecordWriter(TaskAttemptContext context) {
    return new SpectrumRecordWriter();
  }

}
