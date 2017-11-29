package ua.lviv.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
//This program count comments that contain word "iPhone"
public class CommentCounter {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("YouTubeCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> comments = sc.textFile("/Data/DataIn/YouTube/GBcomments.csv");
        JavaRDD<String> videos = sc.textFile("/Data/DataIn/YouTube/GBvideos.csv");

        JavaPairRDD<String,Integer> commentsMap = comments.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
               if(!s.contains("video_id,comment_text,likes,replies") || s.equals("")){
                   char [] sChars = s.toCharArray();
                   int firstCharOfComment = 0;
                   int lastCharOfComment =0;
                   String comment="";
                   for (int i = 0; i < sChars.length-1; i++) {
                       if(sChars[i]==',' && sChars[i+1]=='"'){
                           firstCharOfComment = i+1;

                       }
                       if(sChars[i]=='"' && sChars[i+1]==','){
                           lastCharOfComment = i+1;

                       }
                   }
                   if (firstCharOfComment<0 || lastCharOfComment<0) {
                       return new Tuple2<>("0",1);
                   }else {
                       comment = s.substring(firstCharOfComment, lastCharOfComment);
                       s = s.replace(comment, "comment");
                       String[] str = s.split(",");
                       str[1] = comment;
                       if (str[1].contains("iPhone")) {
                           return new Tuple2<>(str[0], 1);
                       } else return new Tuple2<>(str[0], 0);
                   }
               }else {
                   return new Tuple2<>("0",0);
               }
               }
        });

        JavaPairRDD<String,String> videosMap = videos.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String [] str = s.split(",");
                return new Tuple2<>(str[0],str[2]);
            }
        });

        JavaPairRDD<String,Integer> countComments = commentsMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        JavaPairRDD<String ,Tuple2<Integer,String>> join = countComments.join(videosMap);

        JavaRDD<Tuple2<Integer,String >> channelNamesAndNumOfComments = join.values();

        JavaPairRDD<String,Integer> resultMap = channelNamesAndNumOfComments.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<>(integerStringTuple2._2,integerStringTuple2._1);
            }
        });

        JavaPairRDD<String,Integer> result = resultMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        System.out.println(result.collect());
    }
}
