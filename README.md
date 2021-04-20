# Large Scale Data Processing: Project 3
## Group 1
Collaborators: Baichuan Guo and Byungmoo Kim

Link to code: https://github.com/baocongchen/project_3/blob/main/src/main/scala/project_3/main.scala



## REPORT 

1. **(4 points)** Implement the `verifyMIS` function. The function accepts a Graph[Int, Int] object as its input. Each vertex of the graph is labeled with 1 or -1, indicating whether or not a vertex is in the MIS. `verifyMIS` should return `true` if the labeled vertices form an MIS and `false` otherwise. To execute the function, run the following:
```
// Linux
spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar verify [path_to_graph] [path_to_MIS]

// Unix
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify [path_to_graph] [path_to_MIS]
```
Apply `verifyMIS` locally with the parameter combinations listed in the table below and **fill in all blanks**.
|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | Yes        |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | Yes        |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes        |

2. **(3 points)** Implement the `LubyMIS` function. The function accepts a Graph[Int, Int] object as its input. You can ignore the two integers associated with the vertex RDD and the edge RDD as they are dummy fields. `LubyMIS` should return a Graph[Int, Int] object such that the integer in a vertex's data field denotes whether or not the vertex is in the MIS, with 1 signifying membership and -1 signifying non-membership. The output will be written as a CSV file to the output path you provide. To execute the function, run the following:
```
// Linux
spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar compute [path_to_input_graph] [path_for_output_graph]

// Unix
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute [path_to_input_graph] [path_for_output_graph]
```
Apply `LubyMIS` locally on the graph files listed below and report the number of iterations and running time that the MIS algorithm consumes for **each file**. You may need to include additional print statements in `LubyMIS` in order to acquire this information. Finally, verify your outputs with `verifyMIS`.
|        Graph file       |   Running time  | Verification | Iterations |
| ----------------------- |-----------------|--------------|-------|
| small_edges.csv         |        3s       |      yes     |  1    |
| line_100_edges.csv      |        4s       |      yes     |  2    |
| twitter_100_edges.csv   |        4s       |      yes     |  2    |
| twitter_1000_edges.csv  |        4s       |      yes     |  2    |
| twitter_10000_edges.csv |        8s       |      yes     |  3    |

3. **(3 points)**  
a. Run `LubyMIS` on `twitter_original_edges.csv` in GCP with 3x4 cores. Report the number of iterations, running time, and remaining active vertices (i.e. vertices whose status has yet to be determined) at the end of **each iteration**. You may need to include additional print statements in `LubyMIS` in order to acquire this information. Finally, verify your outputs with `verifyMIS`.  

There are 4 iterations.
- Iteration 1: remaining vertices is 6874609
- Iteration 2: remaining vertices is 33842
- Iteration 4: remaining vertices is 423
- Iteration 5: remaining vertices is 0

Luby's algorithm is completed in 620s.

b. Run `LubyMIS` on `twitter_original_edges.csv` with 4x2 cores and then 2x2 cores. Compare the running times between the 3 jobs with varying core specifications that you submitted in **3a** and **3b**.

