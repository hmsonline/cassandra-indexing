
## Rationale

If you use Cassandra long enough, eventually you'll need to support queries that secondary indexes won't accomodate. Instead, you can use wide-rows in Cassandra.  Roughly speaking, you then use a single row as an index using composite keys as column names.  Since columns are stored in a sorted data structure, querying for a slice of columns is fast (vs. a Range slice).

Much like our cassandra-triggers implementation, we used AOP to implement a generic mechanism for wide-row indexing.  Read on.

## Design

Below is the design we used to implement a generic wide-row indexing mechanism.

### Storage

We use two column families to implement the solution: Configuration and Indexes.  Both of these are under a keyspace, Indexing.

#### Configuration CF 

The Configuration CF contains which column families need indexing, and which columns should be used for indexing.  
You are able to configure multiple indexes for the same column family.  Each configured row is an index.  The rowkey is the anem of the index.  The columns in that row would then specify the target keyspace and column family, and the columns to be used in the index (in order).

#### Indexes CF
There should be a row per index.  That row will contain a column for each row in the target column family being indexed.  The name for that column will be a composite type that includes the columns to be indexed from the original row and the rowkey.  

### Usage
To fetch records perform a column slice on the row in the Indexes column family.  Then use the results to perform specific key fetches in the source table.  Since columns are always sorted when stored, and specific key fetches are fast, the overall extract should be fast.
