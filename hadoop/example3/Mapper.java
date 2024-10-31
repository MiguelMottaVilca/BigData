public class LinkMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text target = new Text();
    private Text source = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        source.set(fields[0]); // URL fuente
        target.set(fields[1]); // URL destino
        context.write(target, source);
    }
}
