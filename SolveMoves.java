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

public class SolveMoves {
    public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, ByteWritable> {
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
        }

        /**
         * The map function for the second mapreduce that you should be filling out.
         */
        @Override
        public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException {
            int value = key.get();
            int[] parents = val.getMoves();
            for (int item : parents) {
                IntWritable parenthash = new IntWritable(item);
                ByteWritable valuehash = new ByteWritable(val.getValue());
                context.write(parenthash, valuehash);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, ByteWritable, IntWritable, MovesWritable> {

        int boardWidth;
        int boardHeight;
        int connectWin;
        boolean OTurn;
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            connectWin = context.getConfiguration().getInt("connectWin", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
        }

        /**
         * The reduce function for the first mapreduce that you should be filling out.
         */
        @Override
        public void reduce(IntWritable key, Iterable<ByteWritable> values, Context context) throws IOException, InterruptedException {
            Stack<Integer> possible_parents = new Stack<Integer>();
            ArrayList<Integer> current_int = new ArrayList<Integer>();
            ArrayList<MovesWritable> current_total = new ArrayList<MovesWritable>();
            ArrayList<MovesWritable> win = new ArrayList<MovesWritable>();
            ArrayList<MovesWritable> draw = new ArrayList<MovesWritable>();
            ArrayList<MovesWritable> lose = new ArrayList<MovesWritable>();
            byte final_input = 0;


            int value = key.get();
            int edit = 0;
            for (ByteWritable item : values) {
                current_total.add(new MovesWritable(item.get(), new int[0]));
                edit = item.get();
                current_int.add(edit >> 2);
            }

            if (current_int.contains(0)) {
                for (MovesWritable item : current_total) {
                    int decide = item.getStatus();
                    if (decide == 2) {
                        if (OTurn) {
                            lose.add(item);
                        } else {
                            win.add(item);
                        }
                    }
                    if (decide == 3) {
                        draw.add(item);
                    }
                    if (decide == 1) {
                        if (OTurn) {
                            win.add(item);
                        } else {
                            lose.add(item);
                        }
                    }
                }

                if (win.size() > 0) {
                    int increment = Integer.MAX_VALUE;
                    MovesWritable target = new MovesWritable();
                    for (MovesWritable item : win) {
                        int decide = item.getMovesToEnd();
                        if (decide <= increment) {
                            increment = decide;
                            target = item;
                        }
                    }
                    target.setMovesToEnd(target.getMovesToEnd() + 1);
                    final_input = target.getValue();
                } else if (draw.size() > 0) {
                    int increment = Integer.MIN_VALUE;
                    MovesWritable target = new MovesWritable();
                    for (MovesWritable item : draw) {
                        int decide = item.getMovesToEnd();
                        if (decide > increment) {
                            increment = decide;
                            target = item;
                        }
                    }
                    target.setMovesToEnd(target.getMovesToEnd() + 1);
                    final_input = target.getValue();
                } else if (lose.size() > 0) {
                    int increment = Integer.MIN_VALUE;
                    MovesWritable target = new MovesWritable();
                    for (MovesWritable item : lose) {
                        int decide = item.getMovesToEnd();
                        if (decide > increment) {
                            increment = decide;
                            target = item;
                        }
                    }
                    target.setMovesToEnd(target.getMovesToEnd() + 1);
                    final_input = target.getValue();
                }

                String current_state = Proj2Util.gameUnhasher(value, boardWidth, boardHeight);
                for (int i = 0; i < current_state.length(); i++) {
                    StringBuilder input = new StringBuilder(current_state);
                    if (!OTurn) {
                        if (current_state.charAt(i) == 'O') {
                            input.setCharAt(i, ' ');
                            String hash = input.toString();
                            int number_rep = Proj2Util.gameHasher(hash, boardWidth, boardHeight);
                            possible_parents.push(number_rep);
                        }
                    } else {
                        if (current_state.charAt(i) == 'X') {
                            input.setCharAt(i, ' ');
                            String hash = input.toString();
                            int number_rep = Proj2Util.gameHasher(hash, boardWidth, boardHeight);
                            possible_parents.push(number_rep);
                        }
                    }
                }


                int total = possible_parents.size();
                if (total != 0) {
                    int[] valueofparents = new int[total];
                    for (int i = 0; i < total; i++) {
                        valueofparents[i] = possible_parents.pop();
                    }
                    MovesWritable everything = new MovesWritable(final_input, valueofparents);
                    context.write(key, everything);
                }
            }
        }
    }
}
