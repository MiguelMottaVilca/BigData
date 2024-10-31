public class TermVectorReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Construye y filtra el vector de términos por cada hostname
        StringBuilder vector = new StringBuilder();
        for (Text val : values) {
            vector.append(val.toString()).append(", ");
        }
        context.write(key, new Text(vector.toString()));
    }
}
