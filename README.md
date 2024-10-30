# Prerequsites 

```
sudo apt-get install protobuf-compiler
sudo apt  install rustup
rustup default stable

git clone https://github.com/apache/datafusion.git
cd datafusion
cargo build -j 1
```


# Guidelines and implementation

The greatest function in Spark returns the largest value given a list of columns for each row. This implementation should handle:
* Variable numbers of arguments.
* Multiple data types, especially numerical and string types.
* Edge cases, such as all-null inputs or mixed data types.

Tips are in DataFusion’s current function library in datafusion/src/physical_plan/expressions 

To test this greatest function with a real DataFusion query, I integrate it as a user-defined function (UDF) in DataFusion, 
then create a simple query to find the greatest value in a column.

# larion
