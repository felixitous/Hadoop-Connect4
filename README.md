#Hadoop & MapReduce

This project was a take on implementing an AI for connect4 except using Hadoop to calculate the best moves for the AI to operate. We were able to implement MapReduce for generating all the possible moves a player could use and using MapReduce again to decide on what exactly the best possible move is. Below are some demonstrations on generating moves and applying a heuristic to each move seen. 

Possible Moves Map Function
--
```java
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
```

Possible Moves Reduce Function
--
```java
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
```

Solve Moves Map Function
--
```java
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
```

Solve Moves Reduce Function
--
```java
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
```
