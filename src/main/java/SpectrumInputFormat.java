import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.*;

public class SpectrumInputFormat
        extends InputFormat<Text, RawSpectrum> {

    private static long[]
    getFileLengths(List<Path> files, JobContext context)
            throws IOException {
        long[] lengths = new long[files.size()];
        for (int i = 0; i < files.size(); ++i) {
            FileSystem fs = files.get(i).getFileSystem(context.getConfiguration());
            lengths[i] = fs.getFileStatus(files.get(i)).getLen();
        }
        return lengths;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        HashMap<String, ArrayList<Path>> stationFiles = new HashMap<>();

        for (Path rootPath : FileInputFormat.getInputPaths(context)) {
            FileSystem fs = rootPath.getFileSystem(context.getConfiguration());
            for (FileStatus file : fs.listStatus(rootPath)) {
                String stationName = file.getPath().getName().substring(0, 5);
                if (!stationFiles.containsKey(stationName)) {
                    ArrayList<Path> temp = new ArrayList<>();
                    temp.add(file.getPath());
                    stationFiles.put(stationName, temp);
                } else {
                    stationFiles.get(stationName).add(file.getPath());
                }
            }
        }

        ArrayList<InputSplit> resultSplits = new ArrayList<>();

        for (Map.Entry<String, ArrayList<Path>> entry : stationFiles.entrySet()) {
            ArrayList<Path> oneStationListFiles = entry.getValue();
            oneStationListFiles.sort(Comparator.naturalOrder());

            if (oneStationListFiles.size() == 5) {
                Path[] oneSplitFiles = new Path[5];
                long[] lengths = getFileLengths(oneStationListFiles, context);
                oneSplitFiles = oneStationListFiles.toArray(oneSplitFiles);

                resultSplits.add(new CombineFileSplit(oneSplitFiles, lengths));
            }
        }
        return resultSplits;
    }

    @Override
    public RecordReader<Text, RawSpectrum> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new SpectrumRecordReader();
    }

}
