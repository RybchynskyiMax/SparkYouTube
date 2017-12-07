package ua.lviv.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.regex.*;

//This program count comments that contain word "iPhone"
public class CommentCounter {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("YouTubeCount").setMaster("local[2]") ;
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> comments = sc.textFile("/home/max/IdeaProjects/SparkYouTube/src/main/resources/DataIn/GBcomments.csv");
        JavaRDD<String> videos = sc.textFile("/home/max/IdeaProjects/SparkYouTube/src/main/resources/DataIn/GBvideos.csv");

        JavaPairRDD<String,Integer> commentsMap = comments.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                Pattern commentPattern = Pattern.compile(".*?\"([^\"]*)\".*?");
                StringBuilder sTemp= new StringBuilder();
                int delimeterIndex = 0;
                if(commentPattern.matcher(s).matches()){
                char [] chars = s.toCharArray();
                for (int i = 0; i < chars.length; i++) {
                    if(chars[i]==','){
                        delimeterIndex = i;
                        break;
                    }
                }
                for (int i = 0; i < delimeterIndex; i++) {
                    sTemp.append(chars[i]);
                }
                    if (s.contains("ass")) {
                        return new Tuple2<>(sTemp.toString(), 1);
                    } else return new Tuple2<>(sTemp.toString(), 0);

                } else {
                    return new Tuple2<>("0", 0);
                }

        }});

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
        for (Tuple2<String, Integer> stringIntegerTuple2 : result.collect()) {
            if(!(stringIntegerTuple2._2==0)){
                System.out.println(stringIntegerTuple2);
            }
        }


        }

    }

