
/**
 * @file
 * @author  Jinfeng Li
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2015] [Jinfeng Li, Qizhen Zhang, Da Yan, James Cheng / The Chinese University of Hong Kong]
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


#include <cmath>
#include <string>
#include <time.h>

#include "graphchi_basic_includes.hpp"
#include "util/toplist.hpp"

using namespace graphchi;

int         iterationcount = 0;
bool        scheduler = true;

/**
 * Type definitions. Remember to create suitable graph shards using the
 * Sharder-program. 
 */
typedef vid_t VertexDataType;       // vid_t is the vertex id type
typedef vid_t EdgeDataType;

vid_t SRC_ID = 0;
vid_t DST_ID = 0;
vid_t DISTANCE = 0;
bool REACH_DST = false;
const vid_t INF = 2000000000;
/**
 * GraphChi programs need to subclass GraphChiProgram<vertex-type, edge-type> 
 * class. The main logic is usually in the update function.
 */
struct BFSProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {


    /**
     *  Vertex update function.
     *  On first iteration ,each vertex chooses a label = the vertex id.
     *  On subsequent iterations, each vertex chooses the minimum of the neighbor's
     *  label (and itself). 
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {

        // if(scheduler) gcontext.scheduler->remove_tasks(vertex.id(), vertex.id());

        if (gcontext.iteration == 0) {
            /* std::cout << "vertex Id and adj infor: " << vertex.id() << " " << vertex.num_outedges() ;
               for( int i = 0; i < vertex.num_outedges(); ++i){
               std::cout << " " << vertex.outedge(i)->vertex_id();
               }
               std::cout<<std::endl;
               */
            if (vertex.id() == SRC_ID)
            {
                vertex.set_data(0);
                //gcontext.scheduler->add_task(vertex.id());
                for(int i=0; i < vertex.num_outedges(); i++) {
                    vertex.outedge(i)->set_data(1);
                    // Schedule neighbors for update
                    if(scheduler){
                        // std::cout<<"scheduler added vertex : "   << vertex.outedge(i)->vertex_id() << std::endl;
                        gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                    }
                }
            }
            else 
            {
                vertex.set_data(INF);
                for(int i=0; i < vertex.num_outedges(); i++) {
                    vertex.outedge(i)->set_data(INF);
                }
            }
        }
        else
        {
            vid_t curmin = vertex.get_data();
            for (int i = 0; i < vertex.num_inedges(); i++) {
                curmin = std::min(curmin, vertex.inedge(i)->get_data());
            }
            if ( vertex.id() == DST_ID && curmin != INF){
                REACH_DST = true;
                DISTANCE = curmin; 
                // std::cout<<"find DST vertex! " << ", and vertex data is : " << vertex.get_data() << std::endl;
                // for (int i = 0; i < vertex.num_inedges(); i++) {
                //     // curmin = std::min(curmin, vertex.inedge(i)->get_data());
                //     std::cout<<" vertex:   " << vertex.id() << ", neighbor " << vertex.inedge(i)->vertex_id() << " , and neghbor data: ";
                //     std::cout<< vertex.inedge(i)->get_data();
                // }
                return ;
            }
            if (curmin < vertex.get_data())
            {
                vertex.set_data(curmin);
                for (int i = 0; i < vertex.num_outedges(); i++)
                {
                    if( vertex.outedge(i)->get_data() <= curmin + 1) continue; 
                    else{
                        vertex.outedge(i)->set_data(curmin+1);
                        // Schedule neighbors for update
                        if(scheduler) gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                    }
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
        std::cout<<"my counter: iteration " << iteration << " finished" << std::endl;
        if (REACH_DST) {
            std::cout << "reach destination! " << std::endl;
            std::cout << "Source Id: " << SRC_ID << ", Destination Id: " << DST_ID << ", and distance is: " << DISTANCE <<std::endl;
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
    graphchi_init(argc, argv);

    /* Metrics object for keeping track of performance counters
       and other information. Currently required. */
    metrics m("bfs");

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

    BFSProgram program;
    engine.run(program, niters);

    /* Report execution metrics */
    metrics_report(m);
    return 0;
}
