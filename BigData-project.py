# Databricks notebook source
# MAGIC %md
# MAGIC ## Introduction
# MAGIC
# MAGIC ### Graphs
# MAGIC
# MAGIC A graph is a non-linear data structure consisting of vertices (nodes) connected by edges (or arcs). A graph can be denoted as G = (V, E), where V represents the set of vertices and E represents the set of edges.
# MAGIC
# MAGIC A graph can be:
# MAGIC  - **Undirected**: This means that the edges of the graph have no direction, they connect nodes without a bias. We can also say that the edges are symmetric.
# MAGIC  - **Directed**  : The edges of the graph have a specific orientation. An edge from vertex u to vertex v is distinct from an edge from v to u.
# MAGIC
# MAGIC Graphs are used to model many systems we use today, such as social networks, mapping systems, the internet, recommendation systems, and much more. Graph mining techniques (techniques used to extract information and analyze features of complex networks) have been thoroughly researched throughout Computer Science history. However, over the past few years, the magnitude of data has experienced exponential growth. In order to extract information from huge networks having hundreds of millions vertices and billions of edges, we need to adopt new, distributed approaches. In this report, we will be implement one such used for finding connected components in a graph. 
# MAGIC
# MAGIC ### What are connected components in a graph
# MAGIC
# MAGIC Given an undirected graph G = (V, E), \\(C = (C_{1},C_{2},...,C_{n}) \\) is the set of **disjoint** connected components in the graph where \\((C_{1} \bigcup C_{2} \bigcup ... \bigcup C{n}) = V\\) and \\((C_{1} \bigcap C_{2} \bigcap ... \bigcap C{n}) = \varnothing \\). This means that, for each \\(C_{i} \in C\\), there exists a path between any two verticies \\(v_{k}\\) and \\(v_{l}\\), where \\((v_{k},v_{l}) \in C_{i}\\). Additionally, for any distinct component \\((C_{i},C_{j}) \in C\\), there is no path between any pair \\(v_{k}\\) and \\(v_{l}\\), where \\(v_{k} \in C_{i}, v_{l} \in C_{j} \\). Here is a graphical representation of the definition:
# MAGIC
# MAGIC ![Simple graph example](https://i.ibb.co/R4Mx2Xq/example-graph.png)
# MAGIC
# MAGIC Here, there are two distinct connected components, \\(C_{1} = (1, 2, 3)\\) and \\(C_{2} = (4, 5, 6)\\). We can see that there are no edges connecting the two components.
# MAGIC
# MAGIC ![Connected components for the graph](https://i.ibb.co/q9T4H58/example-graph-connected-components.png)
# MAGIC
# MAGIC ### Connected component finder (CCF)
# MAGIC
# MAGIC [Connected component finder](https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf) is an efficient and scalable approach developed at Intelius (previously known as Inome). Implemented with Hadoop, it relies on applying a set of map and reduce tasks to the list of edges in a graph, that run iteratively until convergence. The method is used to find all the different connected components of a graph. In this next section, we will dive deeper into how the approach works, and then we will implement the solution in Spark.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## How does Connected Component finder work
# MAGIC
# MAGIC **Important notions**:
# MAGIC  - From here on, when we reference or compare vertices, we are always talking about a vertix label (or vertix id). A vertix id is simply the unique identifier of a vertix in the graph (in the example above, (1,2,3,4,5,6) are all vertix identifiers).
# MAGIC  - The authors of CCF set the componentID to be equal to the id of the smallest node included in the component. In our previous example, C1 would have componentID=1, and C2 would have componentID= 4. In this report, we will consider \\(C\\) to be the set containing all the component ids, i.e the smallest nodes for each connected component of the graph. \\(v_{C_{i}}\\) will be the unique componentID, corresponding to the component \\(C_{i}\\), such that \\(v \in C_{i}\\)
# MAGIC
# MAGIC ### Algorithm description
# MAGIC
# MAGIC CCF takes in as input the list of all the edges of the graph. An edge can be denoted as \\(e = (v_{1}, v_{2}) \\), where \\( (v_{1},v_{2}) \in V \\) are two connected vertices of the graph. In MapReduce terms, we will consider \\(v_{1} \\) to be the key, and \\(v_{2} \\) the corresponding value parameter. Here is the pseudo-code for the connected component finder algorithm:
# MAGIC
# MAGIC <img src="https://i.ibb.co/Cm2NLw4/naive-cff-implementation.png" width="400px" />
# MAGIC
# MAGIC Let's examine in detail what the following code is doing:
# MAGIC  - In the map phase, all edges are passed in as inputs in the form of (key, value) pairs. The map function simply takes, each pair of verticies and produces both (key,value) and (value,key) pairs as output.
# MAGIC  - During the reduce phase, we receive as input a key representing a vertex, and a list of values containing associated vertices. On the first iteration, the list of values contains simply all the adjecent vertices to the key vertix. The reduce phase starts by finding the vertix from the list of values who has the smallest label (let's call this vertix minVertix). If \\(key < minVertix\\), the reduce function will not emit anything. However, if \\(key > minVertix \\), then the reduce function will emit \\((minVertix, key) \\) and \\((\forall v \in values, v != minVertix) \\), we will emit \\((v,minVertix) \\). Furthermore, for each couple \\((v, minVertex))\\ emitted, we increment the "new pair" counter.
# MAGIC  - The condition for convergence is that, after the iteration, no new pairs were identifier (new pair counter is 0).
# MAGIC
# MAGIC **Why do we do this**?
# MAGIC
# MAGIC When the algorithm converges, for each vertix \\((v \in V, v \not\in C)\\), we will have the \\((v,v_{C_{i}})\\) mapping.
# MAGIC
# MAGIC Counting the number of distinct values in the output will give us the number of connected components in the graph. Aggregating the outputs by value will give us all the vertices for each component. Both of these operations can be easily computed by inverting the key and value pairs of our final CCF-Iterate job, and then aggregating by key.
# MAGIC
# MAGIC **Why does it work?**
# MAGIC
# MAGIC Let's illustrate the algorithm with a visual example. We will use the same graph that was given as an example in the CCF paper. The nodes that will serve as component IDs for their respective component are outlined in red:
# MAGIC
# MAGIC ![CCF Initial graph example](https://i.ibb.co/jhWs561/ccf-initial-graph.png)
# MAGIC
# MAGIC The initial list of edges for the following graph, that will serve as input for our first iteration of the job, is:
# MAGIC \\[
# MAGIC     E = 
# MAGIC \begin{bmatrix}
# MAGIC     (1, 2) \newline
# MAGIC     (2, 3) \newline
# MAGIC     (2, 4) \newline
# MAGIC     (2, 5) \newline
# MAGIC     (6, 7) \newline
# MAGIC     (7, 8)
# MAGIC \end{bmatrix}
# MAGIC \\]
# MAGIC
# MAGIC Let's explore the output of the algorithm after the first iteration:
# MAGIC
# MAGIC ![first iteration ccf](https://i.ibb.co/qpVpby6/ccf-first-iteration.png)
# MAGIC
# MAGIC We consider a vertix as "correctly mapped" when the only unique output after the reduce operation for that vertix v is \\((v, v_{C_{i}})\\)
# MAGIC
# MAGIC The second iteration takes the deduplicated output of the previous step:
# MAGIC
# MAGIC ![second iteration ccf](https://i.ibb.co/ky5vFBd/ccf-second-iteration.png)
# MAGIC
# MAGIC We can determine what the algorithm does: it groups, for every key vertex, associated vertices, such that there exists a path between the vertices in the values iterable and the key vertix. This means that there is also a path between any two vertix contained in values, and that all nodes in the values iterable + the key node are necessarily in the same component. The next step is then to map all the values + key to the smallest node id in the set (except if the key is the smallest node). We do this because we have set the identifier of the component to be the smallest value. By doing this in an iterative setting, thanks to this logic and deduplication, we are bound to arrive to the desired state described above.
# MAGIC
# MAGIC <h1 style="text-color:red;"> A Améliorer </h1>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implémentation
# MAGIC
# MAGIC Commençons par implémenter cette première solution, non optimisée, avec Spark

# COMMAND ----------

from pyspark import RDD

'''
Defining accumulator
'''

class NaiveCFF:
    def cff_iterate(rdd: RDD) -> RDD:
        """
        Arguments:
            rdd: ...

        Computes the naive implementation of cff-iterate
        """

        newPairCounter = sc.accumulator(0)
        
        def cff_map(x):
            res = [(x[0], x[1]), (x[1], x[0])]
            return res
        
        
        
        def cff_reduce(x):
            res = []
            key = x[0]
            values = list(x[1])
            minValue = min(values)
            if (key <= minValue):
                return res
            else:
                res.append((key, minValue))
                for v in values:
                    if v == minValue:
                        continue
                    else:
                        res.append((v, minValue))
                        newPairCounter.add(1)
            return res
        
        return (rdd.flatMap(cff_map).groupByKey().flatMap(cff_reduce).distinct(), newPairCounter)

    def cff_run(rdd: RDD, iterate=cff_iterate):
        """
        Arguments:
            ...
        
        Applies the logic defined in the iterate callback until the number of pairs is 0
        """
        new_rdd, pairCount = iterate(rdd)

        #We call an action to execute transformations, and thus compute pairCount
        new_rdd.first()
        newPairsByIteration = [pairCount.value]
        while not (pairCount.value == 0):
            new_rdd, pairCount = iterate(new_rdd)
            new_rdd.collect()
            newPairsByIteration.append(pairCount.value)
        
        return (new_rdd, newPairsByIteration)

    def connected_components(rdd: RDD):
        """
        Arguments:
            rdd: PythonRDD after cff_run transformation
            
        Returns: All connected components, as a list of (componentID, componentNodes (iterable))
        """
        new_rdd = rdd.map(lambda x: (x[1],x[0])).groupByKey()
        return new_rdd

book_example = sc.parallelize([
    (1, 2),
    (2, 3),
    (2, 4),
    (4, 5),
    (6, 7),
    (7, 8)
])
# After approach, we expect [(8, 6), (5, 1), (4, 1), (3, 1), (2, 1), (7, 6)], newCouples = [4,9,4,0]
# Number of components is, output of connected_components should be: [(1, [5, 4, 3, 2]), (6, [8, 7])]
NaiveCFF.connected_components(NaiveCFF.cff_run(book_example)[0]).mapValues(list).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimizations
# MAGIC
# MAGIC This first implementation can be optimized by adding second-order sorting. In fact, in the reduce function of cff_iterate, we need to traverse the whole array of associated values to find the minimum. If this array was sorted in ascending order during the groupBy transformation, it would be sufficient to just take the first element of the array.

# COMMAND ----------

"""
This works, but needs to be tested to see if it bring any improvement to speed
"""
from heapq import merge

class SortingCFF:
    def cff_iterate(rdd: RDD) -> RDD:
        """
        Arguments:
            rdd: ...

        Computes the naive implementation of cff-iterate
        """

        newPairCounter = sc.accumulator(0)
        
        def cff_map(x):
            res = [(x[0], [x[1]]), (x[1], [x[0]])]
            return res
        
        
        
        def cff_sortByKey(x,y):
            return list(merge(x,y))
        
        def cff_reduce(x):
            res = []
            key = x[0]
            values = x[1]
            minValue = values[0]
            if (key <= minValue):
                return res
            else:
                res.append((key, minValue))
                for v in values[1:]:
                    res.append((v, minValue))
                    newPairCounter.add(1)
            return res
        return (rdd.flatMap(cff_map).reduceByKey(cff_sortByKey).flatMap(cff_reduce), newPairCounter)

    def cff_run(rdd: RDD, iterate=cff_iterate):
        """
        Arguments:
            ...
        
        Applies the logic defined in the iterate callback until the number of pairs is 0
        """
        new_rdd, pairCount = iterate(rdd)

        #We call an action to execute transformations, and thus compute pairCount
        new_rdd.first()
        newPairsByIteration = [pairCount.value]
        while not (pairCount.value == 0):
            new_rdd, pairCount = iterate(new_rdd)
            new_rdd.collect()
            newPairsByIteration.append(pairCount.value)
        
        return (new_rdd, newPairsByIteration)

    def connected_components(rdd: RDD):
        """
        Arguments:
            rdd: PythonRDD after cff_run transformation
            
        Returns: All connected components, as a list of (componentID, componentNodes (iterable))
        """
        new_rdd = rdd.map(lambda x: (x[1],x[0])).groupByKey()
        connectedComponentsCount = new_rdd.count()
        return new_rdd

SortingCFF.connected_components(SortingCFF.cff_run(book_example)[0]).mapValues(list).collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing custom partitioning - next step
# MAGIC
# MAGIC In this section, we will test the execution of both algorithms against the same graph that was used in the connected compontent finder paper. We will first start by importing our dataset that can be found on here:
# MAGIC [Link](https://snap.stanford.edu/data/web-Google.html)
# MAGIC

# COMMAND ----------

# Because we saved the data in a table in databricks, we use the following command to retrieve it
# You should adapt this command, such as the file path, to your method of storing the graph
raw_result = spark.sql("SELECT * FROM bigdata_project.my_schema.web_google_without_headers").rdd

# Here, we transform the data so that it has the form of key value pairs
def convert_to_key_value(x):
    after_split = x.value.split("\t")
    return (int(after_split[0]),int(after_split[1]))

rdd = raw_result.map(convert_to_key_value).persist()

# Now, we need to test if this graph is undirected, and if it stores both directions or just single directions
def determine_graph_nature(input_rdd):
    """
    Arguments:
        input_rdd: the initial rdd
    Output:
        inverse_rdd_initial_size: the initial size of the inverse rdd = the initial size of the rdd
        inverse_rdd_sub_size: Size of the rdd resulting from the subtraction of the input_rdd from the inverse_rdd
    """
    inverse_rdd = input_rdd.map(lambda x: (x[1],x[0]))
    inverse_rdd_initial_size = inverse_rdd.count()
    inverse_rdd_sub_size = inverse_rdd.subtract(input_rdd).count()
    print(inverse_rdd_initial_size,inverse_rdd_sub_size)

determine_graph_nature(rdd)

# COMMAND ----------

# MAGIC %md
# MAGIC By subtracting the initial rdd from the inverted_rdd, we can get insights into the graph: is it undirected or directed, the notation used, etc.
# MAGIC For example, lets say that we were dealing with an undirected graph. In that case, we can have 2 ways for saving the graph:
# MAGIC  1. Either we save both directions: Say there is an edge between x and y, we would store both (x,y) and (y,x) into our file
# MAGIC  2. We store the edge only once, with the understanding that the edge is symetrical. In our previous example, we would store only (x,y) or (y,x)
# MAGIC
# MAGIC If we had case 1, subtracting the inverted rdd from the initial rdd should give us an empty rdd, since both datasets have the same couples.
# MAGIC If we had case 2, there should be no intersection between the initial rdd and the inverted rdd.
# MAGIC
# MAGIC However, with our run we can see that neither is the case: the initial rdd (and consequently, the inverted rdd) has 5 105 039 edges. When we subtract the initial rdd from the inverted rdd, we get 3 539 063. This means that the graph is directed. However, finding connected components is a problem used for undirected graphs. Thus, our first action will be to convert the initial rdd into an undirected graph. We will use the saving method described in case 2, since it makes more sense to save a graph in such a manner. The rule will be the following: provided there is an edge between x and y, we will save the couple (x,y) in our rdd, such that x < y.

# COMMAND ----------

def undirected_graph_map(x):
    """
    Input:
        x: (key, value) pair representing an edge from key to value
    Output:
        (smallerValue, biggerValue), the undirected edge
    """
    smallerValue = min(x[0],x[1])
    biggerValue = max(x[0],x[1])
    return (smallerValue, biggerValue)

# After applying the map function, we call distinct because bi-directional edges would be saved as (x,y) and (y,x)
# The undirected graph map would thus produce duplicates.
web_undirected_graph = rdd.map(undirected_graph_map).distinct().persist()

# COMMAND ----------

# MAGIC %md
# MAGIC We now have our undirected graph, that we will use for our experimentations

# COMMAND ----------

naive_result = NaiveCFF.connected_components(NaiveCFF.cff_run(web_undirected_graph)[0]).persist()
naive_result.count()

# COMMAND ----------

# This command doesn't work, need to revisit optimisation
sorted_result = SortingCFF.connected_components(SortingCFF.cff_run(web_undirected_graph)[0]).persist()
sorted_result.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sources
# MAGIC
# MAGIC [1] https://www.geeksforgeeks.org/applications-of-graph-data-structure/
# MAGIC
# MAGIC [2] https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf 

# COMMAND ----------



# COMMAND ----------


