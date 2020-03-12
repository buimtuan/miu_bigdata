hadoop jar ./target/bd-1.0-SNAPSHOT-jar-with-dependencies.jar bd.WordCount /bd/input /bd/wordcount_output
hadoop jar ./target/bd-1.0-SNAPSHOT-jar-with-dependencies.jar bd.ApacheLog /bd/apache /bd/output
hadoop fs -cat /bd/output/*

hadoop jar ./target/bd-1.0-SNAPSHOT-jar-with-dependencies.jar bd.Part2 /bd/comatrix/input /bd/comatrix/output2


hadoop jar ./target/bd-1.0-SNAPSHOT-jar-with-dependencies.jar bd.Part2 /bd/comatrix/input /bd/comatrix/output2


For the purpose of this project you can assume that historical customer data is available in the following form. Each record contains the product IDs of all the product bought by one customer.

TEST DATA (You must use this)

B11 C31 A10 D76 A12 B12 C31 D76 C31 A10 B12 D76 C31 B11     // items bought by a customer, listed in the order she bought it

A10 D76 D76 B12 B11 C31 D76 B12  C31 B11 A12 C31 B12 B11 // items bought by another customer, listed in the order she bought it

…

Let the neighborhood of X, N(X) be set of all term after X and before the next X.

Example: Let Data block be [a b c a d e]

N(a) = {b, c}, N(b) = {c, a, d, e}, N(c) = {a, d, e}, N(a) ={d, e}, N(d) = {e}, N(e) = {}.

Part 2. Implement Pairs algorithm to compute relative frequencies.

    [2 points] Create Java classes (.java files)
    [1 points] Show input, output and batch file to execute your program at command line in Hadoop.

Part 3. Implement Stripes algorithm to compute relative frequencies.

    [2 points] Create Java classes (.java files)
    [1 points] Show input, output and batch file to execute your program at command line in Hadoop.

Part 4. Implement Pairs in Mapper and Stripes in Reducer to compute relative frequencies.

    [2 points] Create Java classes (.java files)
    [1 points] Show input, output and batch file to execute your program at command line in Hadoop.
