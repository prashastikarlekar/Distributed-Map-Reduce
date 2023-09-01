# Distributed Map Reduce

This repository presents a comprehensive implementation of the MapReduce programming methodology, designed for processing massive data collections efficiently. This project encompasses two primary applications: Word Count and Inverted Index.

## Architecture

The implementation comprises several key components:

1. **Server:** A Python 3 TCP server that receives client requests and performs MapReduce operations based on the provided input file. The server is capable of handling multiple client connections concurrently, and it creates and invokes a `main` object located in the Master implementation. The server returns the results of the `main` execution to the client.

2. **Master:** The master node coordinates the MapReduce tasks. Upon receiving a request from a client, the master initiates the corresponding map and reduce functions on the input data. Depending on the map function type (e.g., "word_count" or "inverted_index"), the master creates instances of the appropriate classes to handle the operations.

3. **Key-Value Server:** This serves as a database for intermediate files and output files, allowing mappers and reducers to read from and write to these files. It provides functionalities such as retrieving file data and saving to files.

4. **MapReduce:** This section includes the implementations of the `WordCount` and `InvertedIndex` classes:
   - **WordCount Class:** The mapper task reads input files based on file splits, emitting key-value pairs. These pairs are then passed to the combiner, which organizes the mapper output into key-value pairs that are suitable for the reducer. After combining, this data is stored in the key-value database. The reduce function aggregates the computed results from each worker, producing the final output by summing the values.
   - **InvertedIndex Class:** The input file is divided into chunks equal to the number of mappers. Each mapper processes a chunk and outputs are combined to create a `MergedData`. The reducer processes unique keys and combines their data. Once all reducers are executed, the results are combined and stored as an output file.

5. **Utils:** Contains helper functions such as `split_input`, `merge_files`, and `join_files`.

6. **Helper:** Contains the map and reduce functions corresponding to the `WordCount` and `InvertedIndex` objects.

7. **Config:** Contains host and port information that can be configured.

8. **Log Files:** Log files are created to document the process information and any errors.

## Workflow

The project follows this workflow:

1. Configure the `config.ini` file according to requirements, setting port numbers, hosts, and other parameters.
2. Execute `start_server.sh` to launch the key-value server and main server.
3. Run `test.sh` to initiate test files: `client_wordCount`, `client_invertedIndex`, `clients_multiple`.
4. Clients connect to the Master server, and requests are routed to the appropriate server components based on attributes like `num_mappers`, `num_reducers`, `input_location`, `output_location`, `map_function`, and `reduce_function`.
5. The Master connects to the Key-Value server and creates class instances according to the client's request.
6. Depending on the application, the Master dispatches mapper tasks asynchronously to run in parallel. The Master connects to mappers to assign map tasks to each mapper.
7. Mappers read allocated data from the key-value store and write key-value pairs to it.
8. After processing all pairs, mappers write intermediate outputs to the key-value store, using hashing to allocate keys to reducer inputs.
9. After all mappers finish, reducers are initiated to complete the remaining tasks.
10. Each reducer processes its input and combines values for each key.
11. Upon completion, reducers write their output to the output file at the key-value server.

## Bonus Components

- **Protocol Buffers:** Implemented for efficient communication of intermediate data between components.
- **Fault Tolerance:** In the event of failures, clients can resend requests. If map or reduce functions fail, they are restarted. Reducer failures lead to reprocessing the stored intermediate data.

## Comparisons with Original Map-Reduce

| Original Map-Reduce | Distributed Map Reduce |
|---------------------|------------------------|
| Employs job scheduling with tasks assigned to machines | No job scheduling implemented |
| Implements fault tolerance through heartbeat detection | Implements fault tolerance by detecting lack of worker output |
| Uses task backups to improve execution time | Does not create backups of worker tasks |

## Limitations

- Supports only Word Count and Inverted Index applications.
- Concurrent client connections are constrained by server hardware capabilities.

## Future Scope

- Implement load balancing for better execution times.
- Integrate a distributed data store like HDFS for enhanced scalability and performance.

---
