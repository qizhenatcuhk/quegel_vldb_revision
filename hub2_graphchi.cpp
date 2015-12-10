//
/**
 * @file
 * @author  Qizhen Zhang <qizhen_cuhk@outlook.com>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2012] [Qizhen Zhang, Da Yan, James Cheng / The Chinese University of Hong Kong]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.s
 */

#define GRAPHCHI_DISABLE_COMPRESSION

#include <string>
#include <climits>
#include <vector>
#include "graphchi_basic_includes.hpp"
#include "api/vertex_aggregator.hpp"

#include <ext/hash_set>
#define hash_set __gnu_cxx::hash_set


using namespace graphchi;
using namespace std;
/**
 * Type definitions. Remember to create suitable graph shards using the
 * Sharder-program. 
 */

const char FLAG_NONE=0;
const char FLAG_FORWARD=1;
const char FLAG_BACKWARD=2;
const char FLAG_MEET=3;

/*vertex and edge share the same data type: <distance, preH>*/
struct VertexEdgeDT
{
		unsigned distance;
		char backforward;

		public:
		VertexEdgeDT(unsigned _distance, unsigned _backforward):distance(_distance),backforward(_backforward){}
		VertexEdgeDT():distance(INT_MAX),backforward(FLAG_NONE){}

		public:
		VertexEdgeDT& operator=(VertexEdgeDT other)
		{
				distance=other.distance;
				backforward=other.backforward;
				return *this;
		}
}; 

typedef VertexEdgeDT VertexDataType;
typedef VertexEdgeDT EdgeDataType;

//predefine some vertex value
VertexDataType V_INFINITE_NONE(INT_MAX, FLAG_NONE);
VertexDataType V_ZERO_FORWARD(0, FLAG_FORWARD);
VertexDataType V_ZERO_BACKWARD(0, FLAG_BACKWARD);

//predefine some edge value
EdgeDataType E_INFINITE_NONE(INT_MAX, FLAG_NONE);
EdgeDataType E_ZERO_FORWARD(0, FLAG_FORWARD);
EdgeDataType E_ZERO_BACKWARD(0, FLAG_BACKWARD);

/**
 * Query tructure
 */
struct Query
{
		unsigned src;
		unsigned dst;
		unsigned distance;
		unsigned upperbound;

		public:
		Query& operator=(Query other)
		{
				src=other.src;
				dst=other.dst;
				distance=other.distance;
				upperbound=other.upperbound;

				return *this;
		}
};

hash_set<unsigned> gHubSet;
vector<Query> gQueryList;
Query gCurQuery;

/**
 * GraphChi programs need to subclass GraphChiProgram<vertex-type, edge-type> 
 * class. The main logic is usually in the update function.
 */
struct BFSProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {
		/**
		 *  Vertex update function.
		 */
		void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {

				if (gcontext.iteration == 0) {

						if(vertex.id()==gCurQuery.src) {
								vertex.set_data(V_ZERO_FORWARD);
								for(int i=0; i < vertex.num_outedges(); i++) {
										vertex.outedge(i)->set_data(E_ZERO_FORWARD);
										gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());//add neighbors to scheduler
								}
						}
						else if(vertex.id()==gCurQuery.dst){
								vertex.set_data(V_ZERO_BACKWARD);
								for(int i=0; i < vertex.num_inedges(); i++) {
										vertex.inedge(i)->set_data(E_ZERO_BACKWARD);
										gcontext.scheduler->add_task(vertex.inedge(i)->vertex_id());//add neighbors to scheduler
								}
						}
						else {
								vertex.set_data(V_INFINITE_NONE);
					    	for(int i=0;i<vertex.num_edges();i++)
					    	{
                                if(vertex.id()!=gCurQuery.src&&vertex.id()!=gCurQuery.dst)
					    			vertex.edge(i)->set_data(E_INFINITE_NONE);
					    	}
						}

				} else {
						/* Do computation */ 
						if(gHubSet.find(vertex.id())==gHubSet.end()) {//if not a hub 
								EdgeDataType minLevelBackward = E_INFINITE_NONE;
								EdgeDataType minLevelForward=E_INFINITE_NONE;

								for(int i=0;i<vertex.num_inedges();i++){//since this graph is undirected, thus we need to check the direction
										if((vertex.inedge(i)->get_data().backforward&FLAG_FORWARD) && vertex.inedge(i)->get_data().distance<minLevelForward.distance){
												minLevelForward=vertex.inedge(i)->get_data();
												minLevelForward.distance+=1;
										}
								}
								for(int i=0;i<vertex.num_outedges();i++){//backward
										if((vertex.outedge(i)->get_data().backforward&FLAG_BACKWARD) && vertex.outedge(i)->get_data().distance<minLevelBackward.distance){
												minLevelBackward=vertex.outedge(i)->get_data();
												minLevelBackward.distance+=1;
										}
								}

								unsigned inDistMin=INT_MAX, outDistMin=INT_MAX;
								if(vertex.get_data().backforward&FLAG_FORWARD)//if v is forwarded
								{
										if(vertex.get_data().distance>minLevelForward.distance){
												vertex.set_data(minLevelForward);
												for(int i=0;i<vertex.num_outedges();i++){
														if(vertex.outedge(i)->get_data().distance == INT_MAX){
																vertex.outedge(i)->set_data(minLevelForward);
																/* Schedule neighbor for update */
																gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
														}
												}
										}
										if(minLevelBackward.distance<INT_MAX)
										{
												outDistMin=minLevelBackward.distance;
										}
										inDistMin=vertex.get_data().distance;
								}
								else if(vertex.get_data().backforward&FLAG_BACKWARD)//if v is backwarded
								{
										if(vertex.get_data().distance>minLevelBackward.distance)
										{
												vertex.set_data(minLevelBackward);
												for(int i=0;i<vertex.num_inedges();i++){
														if(vertex.inedge(i)->get_data().distance == INT_MAX){
																vertex.inedge(i)->set_data(minLevelBackward);
																/* Schedule neighbor for update */
																gcontext.scheduler->add_task(vertex.inedge(i)->vertex_id());
														}
												}
										}
										if(minLevelForward.distance<INT_MAX)
										{
												inDistMin=minLevelForward.distance;
										}
										outDistMin=vertex.get_data().distance;
								}
								else
								{
										if(minLevelForward.distance<INT_MAX)
										{
												vertex.set_data(minLevelForward);
												for(int i=0;i<vertex.num_outedges();i++){
														if(vertex.outedge(i)->get_data().distance == INT_MAX){
																vertex.outedge(i)->set_data(minLevelForward);
																/* Schedule neighbor for update */
																gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
														}
												}
												inDistMin=vertex.get_data().distance;
										}
										if(minLevelBackward.distance<INT_MAX)
										{
												vertex.set_data(minLevelBackward);
												for(int i=0;i<vertex.num_inedges();i++){
														if(vertex.inedge(i)->get_data().distance == INT_MAX){
																vertex.inedge(i)->set_data(minLevelBackward);
																/* Schedule neighbor for update */
																gcontext.scheduler->add_task(vertex.inedge(i)->vertex_id());
														}
												}
												outDistMin=vertex.get_data().distance;
										}
								}

								if((minLevelForward.backforward|minLevelBackward.backforward|vertex.get_data().backforward)==FLAG_MEET){
										//find the result
										if(gCurQuery.distance>inDistMin+outDistMin) gCurQuery.distance=inDistMin+outDistMin;
								}
						}
				}
		}

		/**
		 * Called before an iteration starts.
		 */
		void before_iteration(int iteration, graphchi_context &gcontext) {
				cout<<"before_iteration callback of iteration:"<<iteration<<endl;
		}

		/**
		 * Called after an iteration has finished.
		 */
		void after_iteration(int iteration, graphchi_context &gcontext) {
				cout<<"after_iteration callback of iteration:"<<iteration<<endl;
				if(gCurQuery.distance<INT_MAX)
				{
						gcontext.scheduler->remove_tasks(0,gcontext.nvertices);
						gcontext.set_last_iteration(iteration);
						cout<<"Converged! Distance="<<gCurQuery.distance<<endl;
				}
		}

		/**
		 * Called before an execution interval is started.
		 */
		void before_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &gcontext) {        
		}

		/**
		 * Called after an execution interval has finished.
		 */
		void after_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &gcontext) {        
		}
};

/*
   Load hubs from file, and the format is:
   hubId deg
   hubId deg
   ...
   */
void load_top_vertices(const char* fileName)
{
		ifstream pTopVerticesFile(fileName);

		unsigned vId, deg;
		while(pTopVerticesFile>>vId>>deg)
		{
				gHubSet.insert(vId);
		}
}

/**
 * Load queries from file, and the format is:
 * src dst upperbound
 * src dst upperbound
 * ...
 */
void load_queries(const char* fileName)
{
		ifstream  pQueryFile(fileName);

		Query aQ;
		while(pQueryFile>>aQ.src>>aQ.dst>>aQ.upperbound)
		{
				aQ.distance=INT_MAX;
				gQueryList.push_back(aQ);
		}
}

int main(int argc, const char ** argv) {
		/* GraphChi initialization will read the command line 
		   arguments and the configuration file. */
		graphchi_init(argc, argv);
		metrics m("indexing for hub2");

		//load the hub file
		load_top_vertices("top_vertices.txt");

		load_queries("hub2_queries_for_graphchi.txt");

		global_logger().set_log_level(LOG_DEBUG);

		/* Basic arguments for application */
		std::string filename = get_option_string("file");  // Base filename
		int niters           = get_option_int("niters", 100); // Number of iterations
		bool scheduler       = true;

		//int nshards = get_option_int("nshards", 2);
		//delete_shards<EdgeDataType>(filename, nshards); 
		/* Detect the number of shards or preprocess an input to create them */
		int nshards = convert_if_notexists<EdgeDataType>(filename,get_option_string("nshards", "auto"));

		//extract a hub, and process on it.
		unsigned count=0;
		while(count<gQueryList.size())
		{
				gCurQuery=gQueryList[count++];

				if(gHubSet.find(gCurQuery.src)!=gHubSet.end()||gHubSet.find(gCurQuery.dst)!=gHubSet.end())
				{
						gCurQuery.distance=gCurQuery.upperbound;
						string dirName="hub2_dumped_result_"+to_string(gCurQuery.src)+"_"+to_string(gCurQuery.dst)+"_"+to_string(gCurQuery.upperbound)+"__"+to_string(gCurQuery.distance);
						mkdir(dirName.c_str(), 0700);
						continue;
				}

				/* Run */
				graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);
				//cout<<"#ZQZ OUTPUT# This graph has "<<engine.num_vertices()<<" vertices."<<endl;

				//set upper bound of iterations.
				niters=(gCurQuery.upperbound+1)/2;

				BFSProgram program;
				engine.run(program, niters);

                ofstream dumpFile;
                dumpFile.open("hub2_dumped_result", ios::out|ios::app);
                dumpFile<<gCurQuery.src<<" "<<gCurQuery.dst<<" "<<gCurQuery.upperbound<<" "<<gCurQuery.distance<<endl;
                dumpFile.close();
                
                //std::chrono::steady_clock::time_point end= std::chrono::steady_clock::now();
				
		}

		metrics_report(m);
		return 0;
}
