public class URLMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text url = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        url.set(fields[0]); // Asumiendo que la URL es el primer campo
        context.write(url, one);
    }
}
