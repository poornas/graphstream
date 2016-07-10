#example of program that calculates the  median degree of a 
# venmo transaction graph
import collections 
import datetime  
import json
Edge = collections.namedtuple('Edge',("v1","v2"))
DataPoint = collections.namedtuple('DataPoint',("edge1", "edge2","created_time"))

def addEdge(edge,graph): 
	#add edge only if not already present.
	if edge.v1 not in graph: 
		graph[edge.v1] = [edge.v2]
	elif edge.v2 not in graph[edge.v1]:
		graph[edge.v1].append(edge.v2)	

def delEdge(to_del,graph):
	if to_del.v1 in graph: 
		graph[to_del.v1] = [node for node in graph[to_del.v1] if node != to_del.v2 ]
		if len(graph[to_del.v1]) == 0: 
			del graph[to_del.v1]

def modify_graph(point,edges_to_delete,graph): 
	#add edges and delete those that have gone past the 60 second window
	addEdge(point.edge1,graph)
	addEdge(point.edge2,graph)
	if edges_to_delete is None:
		return 
	for edge in edges_to_delete: 
		delEdge(edge,graph)
		 

def add_to_window(point,window,max_timestamp): 
	# determine if new data point fits the 60 sec sliding window, and add it if so.
	# also identify the edges that roll off because they are older than 60 seconds
	# and remove them from the window 
	deleted_edges = []
	skip = False
	if max_timestamp is None: 
		window[point.edge1],window[point.edge2] = point.created_time,point.created_time		 
		max_timestamp  = point.created_time
		return skip,max_timestamp,deleted_edges
	
	delta = (point.created_time - max_timestamp).total_seconds()

	if abs(delta) <= 60: 
		#accept point 	 
		window[point.edge1],window[point.edge2] = point.created_time,point.created_time		 
		#find the new maximum timestamp
		max_timestamp = max(max_timestamp,point.created_time)
		#find edges that slide off 
		deleted_edges = [edge for edge,time in window.items() if abs(time - max_timestamp).total_seconds() > 60]

		window = { key:val for key,val in window.items() if key not in deleted_edges }
	else:
		skip = True	
	return skip,max_timestamp,deleted_edges
	
def calc_median(graph):
	# find degree of each vertex and identify the median
	degrees = [] 
	for vertex,edges in graph.items():
		degrees.append(len(edges))
	degrees.sort()
	n = len(degrees)
	if n % 2 == 0: 
		return (degrees[n/2] + degrees[n/2 - 1]) / 2.0
	else: 
		return degrees[n/2]

def parse_json(data):
#  parse json and store edge and time as a datapoint.Note 2 edges for undirected graph
	try:
		parsed_json = json.loads(data)
		date_object = datetime.datetime.strptime(parsed_json["created_time"], '%Y-%m-%dT%H:%M:%SZ')
		e1  = Edge(v1= parsed_json["actor"], v2= parsed_json["target"])
		e2  = Edge(v2= parsed_json["actor"], v1= parsed_json["target"])
		point = DataPoint(edge1=e1,edge2 = e2,created_time = date_object)
		return point
	except:
		return None
	


if __name__ == '__main__': 
	in1 = open("venmo_input/venmo-trans.txt")
	#in1 = open("venmo_input/in2.txt")
	out = open("venmo_output/output.txt","w")
	
	window = {}             # dictionary mapping edge to created_time of edges of graph in range
	graph = {}              # adjacency list of vertex to edges
	max_timestamp_so_far = None          
	
	for line in in1:
		point = parse_json(line)
		if point is None:
			continue

		skip,max_timestamp_so_far,delete_edges = add_to_window(point,window,max_timestamp_so_far)
		
		if point.edge1 in window and skip is False:
			modify_graph(point,delete_edges,graph)
		median = calc_median(graph)
		out.write("%0.2f\n" % median)

	out.close()

