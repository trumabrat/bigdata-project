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
# MAGIC  - The authors of CCF set the componentID to be equal to the id of the smallest node included in the component. In our previous example, C1 would have componentID=1, and C2 would have componentID= 4
# MAGIC
# MAGIC CCF takes in as input the list of all the edges of the graph. An edge can be denoted as \\(e = (v_{1}, v_{2}) \\), where \\( (v_{1},v_{2}) \in V \\) are two connected vertices of the graph. In MapReduce terms, we will consider \\(v_{1} \\) to be the key, and \\(v_{2} \\) the corresponding value parameter. Here is the pseudo-code for the connected component finder algorithm:
# MAGIC
# MAGIC <img src="https://i.ibb.co/Cm2NLw4/naive-cff-implementation.png" width="400px" />
# MAGIC
# MAGIC Let's examine in detail what the following code is doing:
# MAGIC  - In the map phase, all edges are passed in as inputs in the form of (key, value) pairs. The map function simply takes, each pair of verticies and produces both (key,value) and (value,key) pairs as output.
# MAGIC  - During the reduce phase, we receive as input a key representing a vertex, and a list of values containing associated vertices. On the first iteration, the list of values contains simply all the adjecent vertices to the key vertix. The reduce phase starts by finding the vertix from the list of values who has the smallest label (let's call this vertix minVertix). If \\(key < minVertix\\), the reduce function will not emit anything. However, if \\(key > minVertix \\), then the reduce function will emit \\((minVertix, key) \\) and \\((\forall v \in values, v != minVertix) \\), we will emit \\((v,minVertix) \\).

# COMMAND ----------

# MAGIC %md
# MAGIC # Sources
# MAGIC
# MAGIC [1] https://www.geeksforgeeks.org/applications-of-graph-data-structure/
# MAGIC
# MAGIC [2] https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf 

# COMMAND ----------


