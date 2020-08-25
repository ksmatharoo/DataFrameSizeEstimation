# DataFrameSizeEstimation
this project has 2 way of writing parquet file for data size estimation. 
1 avroparquet writer
  in this i am launching tasks based on the required row count progressively.
2 parquetoutput writer
  in this all tasks are converged to one executor and task end when required row count reached.