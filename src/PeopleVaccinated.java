import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Scanner;

public class PeopleVaccinated {
    public static class PeopleVaccinatedMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString());
            String row = "";

            if (scanner.hasNextLine()) {
                row = scanner.nextLine();
            }

            String[] data = row.split(",");
            String date = "";
            String ageGroup = "";
            String remain = "";

            for (int i = 0; i < data.length; i++) {
                if (i == 0) {
                    date = data[i];
                }
                if (date.compareTo("2021-12-31") >= 0 && date.compareTo("2022-03-23") <= 0) {
                    if (i == 1) {
                        ageGroup = data[i];
                    } else if (i > 2) {
                        remain += "," + data[i];
                    }
                }
            }

            context.write(new Text(ageGroup), new Text(remain));
        }
    }

    public static class PeopleVaccinatedReducer extends Reducer<Text, Text, Text, Text> {
        int maxSino = 0;
        String maxSinoAG = "";
        int maxBio = 0;
        String maxBioAG = "";
        String out = "";
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ageGroup = key.toString();
            int[] doses = new int[12];
            int sinoPeople = 0;
            int bioPeople = 0;
            for (Text val : values) {
                String[] data = val.toString().split(",");
                for (int i = 1; i < data.length; i++) {
                    doses[i - 1] += Integer.parseInt(data[i].trim());
                }
            }
            for (int i = 0; i < doses.length; i++) {
                if (i < 6) {
                    sinoPeople += doses[i];
                } else {
                    bioPeople += doses[i];
                }
            }
            if (sinoPeople > maxSino) {
                maxSino = sinoPeople;
                maxSinoAG = ageGroup;
            }
            if (bioPeople > maxBio) {
                maxBio = bioPeople;
                maxBioAG = ageGroup;
            }
            out = "Age Group: " + maxSinoAG + ", Vaccines: " + maxSino + "," + "Age Group: " + maxBioAG + ", Vaccines: " + maxBio;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(out), new Text());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: PeopleVaccinated <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "PeopleVaccinated by Jinchuan");
        job.setJarByClass(PeopleVaccinated.class);

        job.setMapperClass(PeopleVaccinatedMapper.class);
        job.setReducerClass(PeopleVaccinatedReducer.class);
        job.setOutputKeyClass(Text.class); //output key
        job.setOutputValueClass(Text.class); //output value

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
