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
 * limitations under the License.
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

/*vertex and edge share the same data type: <distance, preH>*/
struct VertexEdgeDT
{
    unsigned distance;
    bool preH;

    public:
    VertexEdgeDT(unsigned _distance, unsigned _preH):distance(_distance),preH(_preH){}
    VertexEdgeDT():distance(INT_MAX),preH(false){}

    public:
    VertexEdgeDT& operator=(VertexEdgeDT other)
    {
        distance=other.distance;
        preH=other.preH;
        return *this;
    }
}; 

typedef VertexEdgeDT VertexDataType;
typedef VertexEdgeDT EdgeDataType;

//predefine some vertex value
VertexDataType V_INFINITE_FALSE(INT_MAX, false);
VertexDataType V_INFINITE_TRUE(INT_MAX, true);
VertexDataType V_ZERO_FALSE(0, false);

//predefine some edge value
EdgeDataType E_ZERO_FALSE(0, false);
EdgeDataType E_INFINITE_FALSE(INT_MAX, false);
EdgeDataType E_INFINITE_TRUE(INT_MAX, true);

vector<unsigned> gHubs;
hash_set<unsigned> gHubSet;
unsigned gCurHub;

/*
   Vertex callback to dump results.
   */
class VertexResultDumper: public VCallback<VertexDataType>{
    FILE** pOutFile=NULL;
    //		string dirPath=NULL;

    public:
    VertexResultDumper(FILE** _pOutFile):pOutFile(_pOutFile){}
    void callback(vid_t vertex_id, VertexDataType &vecvalue){
        if(*pOutFile!=NULL){
            string aVertexData=to_string(vertex_id)+" ";
            if(vecvalue.distance<INT_MAX&&((!(vecvalue.preH))||(gHubSet.find(vertex_id)!=gHubSet.end())))
                aVertexData+=(to_string(vecvalue.distance)+"\n");
            else aVertexData+="-1\n";

            fputs(aVertexData.c_str(), *pOutFile);
        }
        else cout<<"Dump failed! File open failed!"<<endl;
    }
};

/**
 * GraphChi programs need to subclass GraphChiProgram<vertex-type, edge-type> 
 * class. The main logic is usually in the update function.
 */
struct BFSProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {
    bool hasNewTasksForNexIter=false;
    /**
     *  Vertex update function.
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {

        //std::cout << "vertex " << vertex.id() << std::endl;
        if (gcontext.iteration == 0) {

            if(vertex.id()==gCurHub){
                vertex.set_data(V_ZERO_FALSE);

                for(int i=0; i < vertex.num_outedges(); i++) {
                    vertex.outedge(i)->set_data(E_ZERO_FALSE);
                    gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());//add neighbors to scheduler
                    hasNewTasksForNexIter=true;
                }
            }
            else {
                if(gHubSet.find(vertex.id())!=gHubSet.end()) vertex.set_data(V_INFINITE_TRUE);
                else vertex.set_data(V_INFINITE_FALSE);

                for(int i=0;i<vertex.num_edges();i++){
                    if(vertex.edge(i)->vertex_id()!=gCurHub)
                        vertex.edge(i)->set_data(E_INFINITE_FALSE);
                }
            }
        } else {
            /* Do computation */ 
            if(vertex.get_data().distance==INT_MAX) {
                EdgeDataType minLevel = E_INFINITE_FALSE;
                for(int i=0;i<vertex.num_inedges();i++){
                    if(minLevel.distance>vertex.inedge(i)->get_data().distance){
                        minLevel=vertex.inedge(i)->get_data();
                    }
                    else if(minLevel.distance==vertex.inedge(i)->get_data().distance&&vertex.inedge(i)->get_data().preH){
                        minLevel.preH=true;
                    }
                }
                if(minLevel.distance < INT_MAX) {
                    minLevel.distance+=1;
                    if(!vertex.get_data().preH) minLevel.preH=false;

                    vertex.set_data(minLevel);
                    for(int i=0;i<vertex.num_outedges();i++){
                        if(vertex.outedge(i)->get_data().distance == INT_MAX){
                            vertex.outedge(i)->set_data(minLevel);
                            /* Schedule neighbor for update */
                            gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                            hasNewTasksForNexIter=true;
                        }
                    }
                }
            }
        }
    }

    /**
     * Called before an iteration starts.
     */
    void before_iteration(int iteration, graphchi_context &gcontext) {
        hasNewTasksForNexIter=false;
    }

    /**
     * Called after an iteration has finished.
     */
    void after_iteration(int iteration, graphchi_context &gcontext) {
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
   Load hubs from file.
   */
void load_top_vertices(const char* fileName)
{
    std::ifstream pTopVerticesFile(fileName);

    unsigned vId, deg;
    while(pTopVerticesFile>>vId>>deg)
    {
        gHubs.push_back(vId);
        gHubSet.insert(vId);
    }
}

int main(int argc, const char ** argv) {
    /* GraphChi initialization will read the command line 
       arguments and the configuration file. */
    graphchi_init(argc, argv);
    metrics m("indexing for hub2");

    //load the hub file
    load_top_vertices("top_vertices.txt");

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
    while(count<gHubs.size())
    {
        gCurHub=gHubs[count++];

        /* Run */
        graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);

        BFSProgram program;
        engine.run(program, niters);

        string dirName="indexing_hub2_dumped_result_"+to_string(gCurHub);
        mkdir(dirName.c_str(), 0700);
        string dumpFileName=dirName+"/"+"result";
        FILE* pDumpFile=fopen(dumpFileName.c_str(), "w+");
        VertexResultDumper dumper(&pDumpFile);
        foreach_vertices(filename, 0, engine.num_vertices(), dumper);
        fclose(pDumpFile);
    }

    metrics_report(m);
    return 0;
}
