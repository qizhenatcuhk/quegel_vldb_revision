
/**
 * @file
 * @author  Jinfeng Li
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2012] [Jinfeng Li, Qizhen Zhang, Yan Da, James Cheng / The Chinese University of Hong Kong]
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
#include <cmath>
#include <string>
#include <ctime>
#include "graphchi_basic_includes.hpp"
#include "util/toplist.hpp"

using namespace graphchi;
using namespace std;

int         iterationcount = 0;
bool        scheduler = true;

/**
 * Type definitions. Remember to create suitable graph shards using the
 * Sharder-program. 
 */
vid_t SRC_ID = 0;
vid_t DST_ID = 0;
vid_t DISTANCE = 0;
bool MEETED = false;
const vid_t INF = 2000000000;

struct DistPair{
    vid_t toSrc;
    vid_t toDst;
    DistPair(){
        toSrc = INF;
        toDst = INF;
    }
    DistPair( vid_t setToSrc, vid_t setToDst){
        toSrc = setToSrc;
        toDst = setToDst;
    }
};
typedef DistPair VertexDataType;       // vid_t is the vertex id type
typedef DistPair EdgeDataType;

/**
 * GraphChi programs need to subclass GraphChiProgram<vertex-type, edge-type> 
 * class. The main logic is usually in the update function.
 */
struct BIBFSProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {


    /**
     *  Vertex update function.
     *  On first iteration ,each vertex chooses a label = the vertex id.
     *  On subsequent iterations, each vertex chooses the minimum of the neighbor's
     *  label (and itself). 
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {

        // if(scheduler) gcontext.scheduler->remove_tasks(vertex.id(), vertex.id());

        if (gcontext.iteration == 0) {
            if (vertex.id() == SRC_ID)
            {
                DistPair vData = vertex.get_data();
                vData.toSrc = 0;
                vData.toDst = INF;
                vertex.set_data(vData);
                for(int i=0; i < vertex.num_outedges(); i++) {
                    DistPair eData = vertex.outedge(i)->get_data();
                    eData.toSrc = 1;
                    eData.toDst = INF;
                    vertex.outedge(i)->set_data( eData );
                    // Schedule neighbors for update
                    if(scheduler){
                        gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                    }
                }
            }else if(vertex.id() == DST_ID){
               DistPair vData = vertex.get_data(); 
               vData.toDst = 0;
               vData.toSrc = INF;
               vertex.set_data(vData);
               for(int i = 0;  i < vertex.num_inedges(); ++i){
                   DistPair eData = vertex.inedge(i)->get_data();
                   eData.toDst = 1;
                   eData.toSrc = INF;
                   vertex.inedge(i)->set_data( eData );
                    if(scheduler){
                        gcontext.scheduler->add_task(vertex.inedge(i)->vertex_id());
                    }
               }
            }
            else 
            {
                DistPair infData(INF,INF);
                vertex.set_data( infData );
                for(int i=0; i < vertex.num_inedges(); i++) {
                   if( vertex.inedge(i)->vertex_id() != SRC_ID )
                       vertex.inedge(i)->set_data(infData);
                }
                for(int i = 0; i < vertex.num_outedges(); ++i){
                    if(vertex.outedge(i)->vertex_id() != DST_ID)
                        vertex.outedge(i)->set_data(infData);
                
                }
            }
        }
        else
        {
            DistPair vData = vertex.get_data();
            vid_t curToSrcMin = vData.toSrc;
            vid_t curToDstMin = vData.toDst;
            for (int i = 0; i < vertex.num_inedges(); i++) {
                curToSrcMin = std::min(curToSrcMin, vertex.inedge(i)->get_data().toSrc);
            }
            for (int i = 0; i < vertex.num_outedges(); ++i){
                curToDstMin = std::min( curToDstMin, vertex.outedge(i)->get_data().toDst);
            }
            
            if ( !MEETED && curToSrcMin != INF && curToDstMin != INF){
                MEETED = true;
                DISTANCE = curToSrcMin + curToDstMin; 
                return ;
            }else{
                for(int i = 0; i < vertex.num_outedges(); ++i){
                    DistPair eData = vertex.outedge(i)->get_data();
                    if( eData.toSrc <= curToSrcMin + 1) continue;
                    eData.toSrc = curToSrcMin + 1;
                    vertex.outedge(i)->set_data( eData );
                    if(scheduler) gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                }
                for(int i = 0; i < vertex.num_inedges(); ++i){
                    DistPair eData = vertex.inedge(i)->get_data();
                    if( eData.toDst <= curToDstMin + 1) continue;
                    eData.toDst = curToDstMin + 1;
                    vertex.inedge(i)->set_data( eData );
                    if(scheduler) gcontext.scheduler->add_task( vertex.inedge(i)->vertex_id());
                }
            
            }

        }
    }    
    /**
     * Called before an iteration starts.
     */
    void before_iteration(int iteration, graphchi_context &info) {
        iterationcount++;
    }

    /**
     * Called after an iteration has finished.
     */
    void after_iteration(int iteration, graphchi_context &ginfo) {
        if (MEETED) {
            ginfo.set_last_iteration(iteration);
        }
    }

    /**
     * Called before an execution interval is started.
     */
    void before_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &ginfo) {        
    }

    /**
     * Called after an execution interval has finished.
     */
    void after_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &ginfo) {        
    }

};

int main(int argc, const char ** argv) {
    /* GraphChi initialization will read the command line 
       arguments and the configuration file. */


    DistPair mypair;
    std::cout << "my debug:" << mypair.toSrc << ", " << mypair.toDst << std::endl;
    graphchi_init(argc, argv);

    /* Metrics object for keeping track of performance counters
       and other information. Currently required. */
    metrics m("bibfs");

    /* Basic arguments for application */
    std::string filename = get_option_string("file");  // Base filename
    int niters           = get_option_int("niters", 1000); // Number of iterations (max)
    SRC_ID = get_option_int("src", 0);
    DST_ID = get_option_int("dst", 1);
    std::cout << "src:   " << SRC_ID << std::endl;
    std::cout << "dst:   " << DST_ID << std::endl;

    //scheduler            = get_option_int("scheduler", false);
    scheduler = true;  // Always run with scheduler

    clock_t start, finish;
    start = clock();

    /* Process input file - if not already preprocessed */
    int nshards             = (int) convert_if_notexists<EdgeDataType>(filename, get_option_string("nshards", "auto"));

    finish = clock();

    std::cout<<"my time counter, sharding or loading time: " << (double) (finish - start) / CLOCKS_PER_SEC << std::endl;

    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m); 

    /* Run */
    bool inmemmode = engine.num_vertices() * sizeof(EdgeDataType) < (size_t)engine.get_membudget_mb() * 1024L * 1024L;
    std::cout << "memory messurement:" << std::endl;
    std::cout << engine.num_vertices() * sizeof(EdgeDataType) << " " << (size_t)engine.get_membudget_mb() * 1024L * 1024L << std::endl;

    std::cout << "Running bibfs with EM mode!" << std::endl;
    BIBFSProgram program;
    engine.run(program, niters);
    
    /* Report execution metrics */
    metrics_report(m);
    return 0;
}
