public class LinkReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder list = new StringBuilder();
        for (Text val : values) {
            list.append(val.toString()).append(", ");
        }
        context.write(key, new Text(list.toString()));
    }
}
