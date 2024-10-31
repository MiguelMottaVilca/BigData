public class TermVectorMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text hostname = new Text();
    private Text termVector = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Extracción del hostname y término desde los datos del documento
        String[] fields = value.toString().split(" ");
        hostname.set(fields[0]); // Suponiendo que el hostname es el primer campo
        termVector.set(fields[1] + ":" + fields[2]); // Ejemplo: "word:frequency"
        context.write(hostname, termVector);
    }
}
