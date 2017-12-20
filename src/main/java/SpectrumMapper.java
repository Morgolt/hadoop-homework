import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SpectrumMapper extends Mapper<Text, RawSpectrum, Text, RawSpectrum> {

  private int counter = 0;

  public void map(Text key, RawSpectrum value, Context context)
      throws IOException, InterruptedException {
    // limit no. of spectra to save space
		if (counter < 5) {
			++counter;
    context.write(key, value);
		}
  }

}
