public class GrepMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private String pattern = "PATTERN_TO_SEARCH";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.contains(pattern)) {
            context.write(NullWritable.get(), value);
        }
    }
}
