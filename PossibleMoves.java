/*
 * CS61C Spring 2014 Project2
 * Reminders:
 *
 * DO NOT SHARE CODE IN ANY WAY SHAPE OR FORM, NEITHER IN PUBLIC REPOS OR FOR DEBUGGING.
 *
 * This is one of the two files that you should be modifying and submitting for this project.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;
import java.lang.StringBuilder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PossibleMoves {



    /** Mapper from Hadoop */
    public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, IntWritable> {
        int boardWidth;
        int boardHeight;
        boolean OTurn;
        int counter = 0;
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
        }
                                  
        /**
         * The map function for the first mapreduce that you should be filling out.
         */
        @Override
        public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException {
            if (val.getStatus() == 0) {
                int value = key.get();
                String state = Proj2Util.gameUnhasher(value, boardWidth, boardHeight);
                for (int i = 0; i < state.length(); i++) {
                    StringBuilder input = new StringBuilder(state);
                    if (state.charAt(i) == ' ') {
                        if (OTurn) {
                            input.setCharAt(i, 'O');
                        } else {
                            input.setCharAt(i, 'X');
                        }
                        value = Proj2Util.gameHasher(input.toString(), boardWidth, boardHeight);
                        IntWritable child = new IntWritable(value);
                        context.write(child, key);
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, MovesWritable> {
        int boardWidth;
        int boardHeight;
        int connectWin;
        boolean OTurn;
        boolean lastRound;
        /**
         * Configuration and setup that occurs before reduce gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            connectWin = context.getConfiguration().getInt("connectWin", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
            lastRound = context.getConfiguration().getBoolean("lastRound", true);
        }

        /**
         * The reduce function for the first mapreduce that you should be filling out.
         */
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /** Setting up the two parts of the MovesWritable. */

            Stack<Integer> parents = new Stack<Integer>();
            int valueofkey = key.get();
            int winning;
            int numberofmoves = 0;
            String boardrep = Proj2Util.gameUnhasher(valueofkey, boardWidth, boardHeight);
            if (Proj2Util.gameFinished(boardrep, boardWidth, boardHeight, connectWin)) {
                if (OTurn) {
                    winning = 1;
                } else {
                    winning = 2;
                }
            } else {
                if (lastRound) {
                    winning = 3;
                } else {
                    winning = 0;
                }
            }
            for (IntWritable item : values) {
                int convert = item.get();
                if (convert != valueofkey) {
                    parents.push(convert);
                }
            }
            int total = parents.size();
            if (total != 0) {
                int[] valueofparents = new int[total];
                for (int i = 0; i < total; i++) {
                    valueofparents[i] = parents.pop();
                }
                MovesWritable everything = new MovesWritable(winning, numberofmoves, valueofparents);
                context.write(key, everything);
            }
        }
    }
}
