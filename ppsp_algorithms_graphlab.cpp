#include <vector>
#include <string>
#include <fstream>
#include <cassert>

#include <graphlab.hpp>
#include <cstdio>

#include <ext/hash_set>
#define hash_set __gnu_cxx::hash_set


const int inf = 1e9;

typedef int vertex_data;
typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> UndirectedGraph;

struct BiVertex_data : graphlab::IS_POD_TYPE
{
    int forward;
    int backward;
};

typedef graphlab::distributed_graph<BiVertex_data, edge_data> DirectedGraph;

struct IndexingVertex_data : graphlab::IS_POD_TYPE
{
    int distance;
    bool preH;
    IndexingVertex_data(int distance=inf, bool preH=false):distance(distance), preH(preH){}
};

typedef graphlab::distributed_graph<IndexingVertex_data, edge_data> IndexingGraph;

struct IndexingVertex_Directed_data : graphlab::IS_POD_TYPE
{
    int fDistance;
    int bDistance;
    bool fPreH;
    bool bPreH;
    IndexingVertex_Directed_data(int fDistance=inf, int bDistance=inf, bool fPreH=false,bool bPreH=false):fDistance(fDistance), bDistance(bDistance), fPreH(fPreH), bPreH(bPreH){}
};

typedef graphlab::distributed_graph<IndexingVertex_Directed_data, edge_data> IndexingGraph_Directed;


int SOURCE_VERTEX, DEST_VERTEX;
int ForwardCover, BackwardCover;
int BiDist, BiIteration;

//variables for indexing of hub2
int CUR_HUB;
hash_set<int> HUBSET;
std::vector<int> HUBLIST;

int UPPER_BOUND;


/**
 *  * Query tructure
 *   */
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
std::vector<Query> gQueryList;
Query gCurQuery;


int KHop = 3;

void init_vertex(UndirectedGraph::vertex_type& vertex)
{
    vertex.data() = inf; 
}

void init_bivertex(DirectedGraph::vertex_type& vertex)
{
    if(vertex.id() == SOURCE_VERTEX)
        vertex.data().forward = 0;
    else
        vertex.data().forward = inf; 

    if(vertex.id() == DEST_VERTEX)
        vertex.data().backward = 0; 
    else
        vertex.data().backward = inf;
}

//init the vertices for hub2 indexing
void init_indexingvertex(IndexingGraph::vertex_type& vertex)
{
    if(vertex.id()==CUR_HUB)
    {
        vertex.data().distance=0;
        vertex.data().preH=false;
    }
    else if(HUBSET.find(vertex.id())!=HUBSET.end())
    {
        vertex.data().distance=inf;
        vertex.data().preH=true;
    }
    else
    {
        vertex.data().distance=inf;
        vertex.data().preH=false;
    }
}

void init_indexingvertex_Directed(IndexingGraph_Directed::vertex_type& vertex)
{
    if(vertex.id()==CUR_HUB)
    {
        vertex.data().fDistance=0;
        vertex.data().bDistance=0;
        vertex.data().fPreH=false;
        vertex.data().bPreH=false;
    }
    else if(HUBSET.find(vertex.id())!=HUBSET.end())
    {
        vertex.data().fDistance=inf;
        vertex.data().bDistance=inf;
        vertex.data().fPreH=true;
        vertex.data().bPreH=true;
    }
    else
    {
        vertex.data().fDistance=inf;
        vertex.data().bDistance=inf;
        vertex.data().fPreH=false;
        vertex.data().bPreH=false;
    }
}

void set_dist(DirectedGraph::vertex_type& vertex)
{
    if(vertex.id() == DEST_VERTEX)
        vertex.data().forward = BiDist; 
}

size_t count_forward_cover(const DirectedGraph::vertex_type& vertex) {
    return vertex.data().forward != inf;
}

size_t count_backward_cover(const DirectedGraph::vertex_type& vertex) {
    return vertex.data().backward != inf;
}

struct min_t : public graphlab::IS_POD_TYPE {
    int value;
    min_t()
        : value(inf)
    {
    }
    min_t(int _value)
        : value(_value)
    {
    }
    min_t& operator+=(const min_t& other)
    {
        value = std::min(value, other.value);
        return *this;
    }
};

min_t get_dist(const DirectedGraph::vertex_type& vertex)
{
    return min_t(vertex.data().forward + vertex.data().backward);
}

struct SingleSourceBFS_type : graphlab::IS_POD_TYPE {
    int dist;
    SingleSourceBFS_type(int dist = inf) : dist(dist) { }
    SingleSourceBFS_type& operator+=(const SingleSourceBFS_type& other) {
        dist = std::min(dist, other.dist);
        return *this;
    }
};

typedef SingleSourceBFS_type KHop_type;

struct BiBFS_type : graphlab::IS_POD_TYPE {
    int flag;
    BiBFS_type(int flag = 0) : flag(flag) { }
    BiBFS_type& operator+=(const BiBFS_type& other) {
        flag |= other.flag;
        return *this;
    }
};

struct Hub2Indexing_type : graphlab::IS_POD_TYPE{
    bool preH;
    int distance;
    Hub2Indexing_type(bool _preH=false, int _distance=inf):preH(_preH),distance(_distance){}
    Hub2Indexing_type& operator+=(const Hub2Indexing_type& other){
        preH=(preH||other.preH);
        distance=std::min(distance, other.distance);
        return *this;
    }
};

const char MSG_DIRECTION_FORWARD=1;
const char MSG_DIRECTION_BACKWARD=2;
const char MSG_FORWARD_PREH=4;
const char MSG_BACKWARD_PREH=8;

struct Hub2Indexing_Directed_type : graphlab::IS_POD_TYPE{
    char msgType;
    int fDistance;
    int bDistance;
    Hub2Indexing_Directed_type(char _msgType=0, int _fDistance=inf, int _bDistance=inf):msgType(_msgType),fDistance(_fDistance),bDistance(_bDistance){}
    Hub2Indexing_Directed_type& operator+=(const Hub2Indexing_Directed_type& other){
        msgType|=other.msgType;
        fDistance=std::min(fDistance, other.fDistance);
        bDistance=std::min(bDistance, other.bDistance);
        return *this;
    }
};



// gather type is graphlab::empty, then we use message model
class SingleSourceBFS : public graphlab::ivertex_program<UndirectedGraph, graphlab::empty, SingleSourceBFS_type>,
    public graphlab::IS_POD_TYPE
{
    int min_dist;
    bool changed;
    public:
    void init(icontext_type& context, const vertex_type& vertex,
            const SingleSourceBFS_type& msg) {
        min_dist = msg.dist;
    } 

    edge_dir_type gather_edges(icontext_type& context, 
            const vertex_type& vertex) const { 
        return graphlab::NO_EDGES;
    };


    void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty)
    {
        changed = false;
        if(vertex.data() > min_dist) {
            changed = true;
            vertex.data() = min_dist;
            if(vertex.id() == DEST_VERTEX)
            {
                context.stop();
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
            const vertex_type& vertex) const
    {
        if (changed)
            return graphlab::OUT_EDGES;
        else
            return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
            edge_type& edge) const
    {
        int newd = vertex.data() + 1;
        const SingleSourceBFS_type msg(newd);
        if(edge.target().data() > newd)
        {
            context.signal(edge.target(), msg);
        }
    }
};

class Hub2Indexing_Directed:public graphlab::ivertex_program<IndexingGraph_Directed, graphlab::empty, Hub2Indexing_Directed_type>,
    public graphlab::IS_POD_TYPE
{
    char my_msgType;
    bool fChanged;
    bool bChanged;
    int my_fDistance;
    int my_bDistance;

    public:
    void init(icontext_type& context, const vertex_type &vertex, const Hub2Indexing_Directed_type & msg)
    {
        my_msgType=msg.msgType;
        my_fDistance=msg.fDistance;
        my_bDistance=msg.bDistance;
    }

    edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const{
        return graphlab::NO_EDGES;
    }

    void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty)
    {
        fChanged=false;
        bChanged=false;
        if(context.iteration()==0){}
        else
        {
            if(my_msgType&MSG_DIRECTION_FORWARD && vertex.data().fDistance > my_fDistance)
            {
                vertex.data().fDistance=my_fDistance;
                vertex.data().fPreH=(my_msgType & MSG_FORWARD_PREH);
                fChanged=true;
            }
            if(my_msgType&MSG_DIRECTION_BACKWARD && vertex.data().bDistance>my_bDistance)
            {
                vertex.data().bDistance=my_bDistance;
                vertex.data().bPreH=(my_msgType & MSG_BACKWARD_PREH);
                bChanged=true;
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const
    {
        if(context.iteration()==0)//because in iteration 0, the changed variable is not used.
        {
            return graphlab::ALL_EDGES;
        }
        else
        {
            if(fChanged&&bChanged) return graphlab::ALL_EDGES;//only the changed vertices will send msg.
            else if(fChanged) return graphlab::OUT_EDGES;
            else if(bChanged) return graphlab::IN_EDGES;
            else return graphlab::NO_EDGES;
        }
    }

    void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const
    {
        if(edge.source().id()==vertex.id())//OUT_EDGE
        {
            const Hub2Indexing_Directed_type msg(vertex.data().fPreH|MSG_DIRECTION_FORWARD, vertex.data().fDistance+1, inf);
            if(edge.target().data().fDistance>msg.fDistance)
            {
                context.signal(edge.target(),msg);
            }
        }
        else if(edge.target().id()==vertex.id())//IN_EDGE
        {
            const Hub2Indexing_Directed_type msg(vertex.data().bPreH|MSG_DIRECTION_BACKWARD, inf, vertex.data().bDistance+1);
            if(edge.source().data().bDistance>msg.bDistance)
            {
                context.signal(edge.source(), msg);
            }
        }
        //        }
    }
};


class Hub2Indexing:public graphlab::ivertex_program<IndexingGraph, graphlab::empty, Hub2Indexing_type>,
    public graphlab::IS_POD_TYPE
{
    bool my_preH;
    bool changed;
    int my_distance;

    public:
    void init(icontext_type& context, const vertex_type &vertex, const Hub2Indexing_type & msg)
    {
        my_preH=msg.preH||my_preH;
        my_distance=msg.distance;
    }

    edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const{
        return graphlab::NO_EDGES;
    }

    void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty)
    {
        changed=false;
        if(context.iteration()==0){}
        else
        {
            if(vertex.data().distance>my_distance)
            {
                vertex.data().distance=my_distance;
                vertex.data().preH=my_preH;
                changed=true;
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const
    {
        if(context.iteration()==0)//because in iteration 0, the changed variable is not used.
        {
            return graphlab::OUT_EDGES;
        }
        else
        {
            if(changed) return graphlab::OUT_EDGES;//only the changed vertices will send msg.
            else return graphlab::NO_EDGES;
        }
    }

    void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const
    {
        const Hub2Indexing_type msg(vertex.data().preH, vertex.data().distance+1);
        if(edge.target().data().distance>msg.distance)
        {
            context.signal(edge.target(),msg);
        }
    }
};

// gather type is graphlab::empty, then we use message model
class BiBFS : public graphlab::ivertex_program<DirectedGraph, graphlab::empty, BiBFS_type>,
    public graphlab::IS_POD_TYPE
{
    int my_flag;
    public:
    void init(icontext_type& context, const vertex_type& vertex,
            const BiBFS_type& msg) {
        my_flag = msg.flag;
    } 

    edge_dir_type gather_edges(icontext_type& context, 
            const vertex_type& vertex) const { 
        return graphlab::NO_EDGES;
    };


    void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty)
    {
        if(context.iteration() == 0)
        {
            ; // send msgs
        }
        else // if(context.iteration() == 1)
        {
            if( vertex.data().forward == inf && (my_flag & 1) )
            {
                vertex.data().forward = BiIteration;
            }

            if( vertex.data().backward == inf && (my_flag & 2) )
            {
                vertex.data().backward = BiIteration;
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
            const vertex_type& vertex) const
    {
        if(context.iteration() == 0)
        {
            if(vertex.data().forward != inf)
                return graphlab::OUT_EDGES;
            else
                return graphlab::IN_EDGES;
        }
        else
        {
            return graphlab::NO_EDGES;
        }
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
            edge_type& edge) const
    {
        if(edge.source().id() == vertex.id()) // OUT_EDGE
        {
            const BiBFS_type msg(1);
            if (edge.target().data().forward == inf)
            {
                context.signal(edge.target(), msg);
            }
        }
        else
        {
            const BiBFS_type msg(2);
            if (edge.source().data().backward == inf)
            {
                context.signal(edge.source(), msg);
            }
        }
    }
};

class Hub2 : public graphlab::ivertex_program<DirectedGraph, graphlab::empty, BiBFS_type>,
    public graphlab::IS_POD_TYPE
{
    int my_flag;
    public:
    void init(icontext_type& context, const vertex_type& vertex,
            const BiBFS_type& msg) {
        my_flag = msg.flag;
    } 

    edge_dir_type gather_edges(icontext_type& context, 
            const vertex_type& vertex) const { 
        return graphlab::NO_EDGES;
    };


    void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty)
    {
        if(context.iteration() == 0)
        {
            ; // send msgs
        }
        else // if(context.iteration() == 1)
        {
            if( vertex.data().forward == inf && (my_flag & 1) )
            {
                vertex.data().forward = BiIteration;
            }

            if( vertex.data().backward == inf && (my_flag & 2) )
            {
                vertex.data().backward = BiIteration;
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
            const vertex_type& vertex) const
    {
        if(context.iteration() == 0)
        {
            if(vertex.data().forward != inf)
                return graphlab::OUT_EDGES;
            else
                return graphlab::IN_EDGES;
        }
        else
        {
            return graphlab::NO_EDGES;//no edge? How to propagate the msg?
        }
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
            edge_type& edge) const
    {
        if(edge.source().id() == vertex.id()) // OUT_EDGE
        {
            const BiBFS_type msg(1);
            if (edge.target().data().forward == inf && HUBSET.find(edge.target().id())==HUBSET.end())
            {
                context.signal(edge.target(), msg);
            }
        }
        else
        {
            const BiBFS_type msg(2);
            if (edge.source().data().backward == inf && HUBSET.find(edge.source().id())==HUBSET.end())
            {
                context.signal(edge.source(), msg);
            }
        }
    }
};

class Hub2_New : public graphlab::ivertex_program<DirectedGraph, graphlab::empty, BiBFS_type>,
    public graphlab::IS_POD_TYPE
{
    int my_flag;
    public:
    void init(icontext_type& context, const vertex_type& vertex,
            const BiBFS_type& msg) {
        my_flag = msg.flag;
    } 

    edge_dir_type gather_edges(icontext_type& context, 
            const vertex_type& vertex) const { 
        return graphlab::NO_EDGES;
    };


    void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty)
    {
        if(context.iteration()==UPPER_BOUND)
        {
            context.stop();
        }
        else if(context.iteration() == 0)
        {
            ; // send msgs
        }
        else // if(context.iteration() == 1)
        {
            if( vertex.data().forward == inf && (my_flag & 1) )
            {
                vertex.data().forward = context.iteration();
            }

            if( vertex.data().backward == inf && (my_flag & 2) )
            {
                vertex.data().backward = context.iteration();
            }

            if((my_flag&1) && (my_flag&2))
            {
                context.stop();//met, quit now
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
            const vertex_type& vertex) const
    {
            if(vertex.data().forward != inf)
                return graphlab::OUT_EDGES;
            else if(vertex.data().backward !=inf)
                return graphlab::IN_EDGES;
            else
                return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
            edge_type& edge) const
    {
        if(edge.source().id() == vertex.id()) // OUT_EDGE
        {
            const BiBFS_type msg(1);
            if (edge.target().data().forward == inf && HUBSET.find(edge.target().id())==HUBSET.end())
            {
                context.signal(edge.target(), msg);
            }
        }
        else
        {
            const BiBFS_type msg(2);
            if (edge.source().data().backward == inf && HUBSET.find(edge.source().id())==HUBSET.end())
            {
                context.signal(edge.source(), msg);
            }
        }
    }
};


typedef graphlab::synchronous_engine<BiBFS> engine_type;

graphlab::empty signal_vertices(engine_type::icontext_type& ctx,
        const DirectedGraph::vertex_type& vertex) {
    if (vertex.data().forward != inf || vertex.data().backward != inf) {
        ctx.signal(vertex);
    }
    return graphlab::empty();
}

// gather type is graphlab::empty, then we use message model
class KHopBFS : public graphlab::ivertex_program<UndirectedGraph, graphlab::empty, KHop_type>,
    public graphlab::IS_POD_TYPE
{
    int min_dist;
    bool changed;
    public:
    void init(icontext_type& context, const vertex_type& vertex,
            const KHop_type& msg) {
        min_dist = msg.dist;
    } 

    edge_dir_type gather_edges(icontext_type& context, 
            const vertex_type& vertex) const { 
        return graphlab::NO_EDGES;
    };


    void apply(icontext_type& context, vertex_type& vertex, const graphlab::empty& empty)
    {
        if(context.iteration() == KHop + 1)
        {
            context.stop();
            return;
        }

        changed = false;
        if(vertex.data() > min_dist) {
            changed = true;
            vertex.data() = min_dist;
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
            const vertex_type& vertex) const
    {
        if (changed && context.iteration() < KHop)
            return graphlab::OUT_EDGES;
        else
            return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
            edge_type& edge) const
    {
        int newd = vertex.data() + 1;
        const SingleSourceBFS_type msg(newd);
        if(edge.target().data() > newd)
        {
            context.signal(edge.target(), msg);
        }
    }
};



struct SingleSourceBFS_writer {
    std::string save_vertex(const UndirectedGraph::vertex_type& vtx)
    {
        if(vtx.id() == DEST_VERTEX)
        {
            std::stringstream strm;
            strm << vtx.id() << "\t" << vtx.data() << "\n";
            return strm.str();
        }
        else
        {
            return "";
        }
    }
    std::string save_edge(UndirectedGraph::edge_type e)
    {
        return "";
    }
};

struct Hub2Indexing_writer
{
    std::string save_vertex(const IndexingGraph::vertex_type& vertex)
    {
        if(HUBSET.find(vertex.id())!=HUBSET.end())
        {
            std::stringstream strm;
            strm<<vertex.id()<<"\t";
            if(vertex.data().distance==inf)
                strm<<"inf\n";
            else
                strm<<vertex.data().distance<<"\n";
            return strm.str();
        }
        else if(!vertex.data().preH)
        {
            std::stringstream strm;
            strm<<vertex.id()<<"\t";
            if(vertex.data().distance==inf)
                strm<<"inf\n";
            else
                strm<<vertex.data().distance<<"\n";
            return strm.str();
        }
        else
        {
            std::stringstream strm;
            strm<<vertex.id()<<"\t-1\n";
            return strm.str();
        }
        return "";//ban the dump
    }

    std::string save_edge(IndexingGraph::edge_type e)
    {
        return "";
    }
};

struct Hub2Indexing_Directed_writer
{
    std::string save_vertex(const IndexingGraph_Directed::vertex_type& vertex)
    {
        if(HUBSET.find(vertex.id())!=HUBSET.end())
        {
            std::stringstream strm;
            strm<<vertex.id()<<"\t";
            if(vertex.data().fDistance==inf)
                strm<<"inf\t";
            else
                strm<<vertex.data().fDistance<<"\t";
            if(vertex.data().bDistance==inf)
                strm<<"inf\n";
            else
                strm<<vertex.data().bDistance<<"\n";

            return strm.str();
        }
        else if(!vertex.data().fPreH && !vertex.data().bPreH)
        {
            std::stringstream strm;
            strm<<vertex.id()<<"\t";
            if(vertex.data().fDistance==inf)
                strm<<"inf\t";
            else
                strm<<vertex.data().fDistance<<"\t";
            if(vertex.data().bDistance==inf)
                strm<<"inf\n";
            else
                strm<<vertex.data().bDistance<<"\n";

            return strm.str();
        }
        else if(vertex.data().fPreH && !vertex.data().bPreH)
        {
            std::stringstream strm;
            strm<<vertex.id()<<"\t-1\t";
            if(vertex.data().bDistance==inf)
                strm<<"inf\n";
            else
                strm<<vertex.data().bDistance<<"\n";
            return strm.str();
        }
        else if(!vertex.data().fPreH && vertex.data().bPreH)
        {
            std::stringstream strm;
            strm<<vertex.id()<<"\t";
            if(vertex.data().fDistance==inf)
                strm<<"inf\t-1\n";
            else
                strm<<vertex.data().fDistance<<"\t-1\n";
            return strm.str();
        }
        else
        {
            std::stringstream strm;
            strm<<vertex.id()<<"\t-1\t-1\n";
            return strm.str();
        }
        return "";
    }

    std::string save_edge(IndexingGraph_Directed::edge_type e)
    {
        return "";
    }
};

struct BiBFS_writer {
    std::string save_vertex(const DirectedGraph::vertex_type& vtx)
    {
        if(vtx.id() == DEST_VERTEX)
        {
            std::stringstream strm;
            int dist = vtx.data().forward; // dist is stored at forward
            strm << vtx.id() << "\t" << dist << "\n";
            return strm.str();
        }
        else
        {
            return "";
        }
    }
    std::string save_edge(DirectedGraph::edge_type e)
    {
        return "";
    }
};


struct KHop_writer {
    std::string save_vertex(const UndirectedGraph::vertex_type& vtx)
    {
        if(vtx.data() != inf)
        {
            std::stringstream strm;
            strm << vtx.id() << "\t" << vtx.data() << "\n";
            return strm.str();
        }
        else
        {
            return "";
        }
    }
    std::string save_edge(UndirectedGraph::edge_type e)
    {
        return "";
    }
};


bool UndirectedParser(UndirectedGraph& graph, const std::string& filename,
        const std::string& textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    int num;
    ssin >> num;
    for(int i = 0 ;i < num ; i ++)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        if (vid != other_vid)
            graph.add_edge(vid, other_vid);
    }
    return true;
}

bool UndirectedBiParser(DirectedGraph& graph, const std::string& filename,
        const std::string& textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    graph.add_vertex(vid);
    int num;
    ssin >> num;

    if(num==0)
        graph.add_vertex(vid);

    while(num--)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        if (vid != other_vid)
            graph.add_edge(vid, other_vid);
    }

    return true;
}


bool DirectedBiParser(DirectedGraph& graph, const std::string& filename,
        const std::string& textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    int in_num, out_num;
    ssin >> in_num;
    if(in_num==0)
        graph.add_vertex(vid);
    while(in_num--)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid; // skip in_neighbors;
    }
    ssin >> out_num;
    if(out_num==0)
        graph.add_vertex(vid);
    while(out_num--)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        if (vid != other_vid)
            graph.add_edge(vid, other_vid);
    }
    return true;
}

bool UndirectedIndexingParser(IndexingGraph& graph, const std::string& filename,
        const std::string& textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    graph.add_vertex(vid);
    int num;
    ssin >> num;

    if(num==0)
        graph.add_vertex(vid);

    while(num--)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        if (vid != other_vid)
            graph.add_edge(vid, other_vid);
    }

    return true;
}

bool DirectedIndexingParser(IndexingGraph_Directed& graph, const std::string& filename, const std::string& textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    int in_num, out_num;
    ssin >> in_num;
    if(in_num==0)
        graph.add_vertex(vid);
    while(in_num--)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid; // skip in_neighbors;
    }
    ssin >> out_num;
    if(out_num==0)
        graph.add_vertex(vid);
    while(out_num--)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        if (vid != other_vid)
            graph.add_edge(vid, other_vid);
    }
    return true;
}

bool _DirectedIndexingParser(IndexingGraph& graph, const std::string& filename, const std::string& textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    int in_num, out_num;
    ssin >> in_num;
    for(int i = 0 ;i < in_num; i ++)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid; // skip in_neighbors;
    }
    ssin >> out_num;
    for(int i = 0 ;i < out_num ; i ++)
    {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        if (vid != other_vid)
            graph.add_edge(vid, other_vid);
    }
    return true;
}

void load_top_vertices(const char* fileName)
{
    std::ifstream pTopVerticesFile(fileName);
    unsigned vId, deg;
    while(pTopVerticesFile>>vId>>deg)
    {
        HUBSET.insert(vId);
        HUBLIST.push_back(vId);
    }
}

void load_queries(const char* fileName)
{
    std::ifstream pQueryFile(fileName);

    Query aQ;
    while(pQueryFile>>aQ.src>>aQ.dst>>aQ.upperbound)
    {
        aQ.distance=INT_MAX;
        gQueryList.push_back(aQ);
    }
}

int main(int argc, char** argv)
{
    graphlab::mpi_tools::init(argc, argv);

    char *dataset=argv[1];

    char* input_file;
    char* output_file;
    char* opt;
    char* hub_file;
    char* query_file;

    if(strcmp(dataset,"li")==0)
    {
        input_file = "hdfs://[livej_path]";
        output_file = "hdfs://[output_path]";
        opt="hub2_indexing_UG";
        hub_file="";
    }
    else if(strcmp(dataset, "tw")==0)
    {
        input_file = "hdfs://[twitter_path]";
        output_file = "hdfs://[output_path]";
        opt="hub2_indexing_DG";
        hub_file="[twitter_hub_path]";
    }
    else if(strcmp(dataset, "bt")==0)
    {
        input_file = "hdfs://[btc_path]";
        output_file = "hdfs://[output_path]";
        opt="hub2_indexing_UG";
        hub_file="[btc_hub_path]";
    }
    else if(strcmp(dataset, "bthub2")==0)
    {
        input_file = "hdfs://[btc_path]";
        output_file = "hdfs://[output_path]";
        opt="hub2_UG";
        hub_file="[btc_hub_path]";
        query_file="[btc_query_path]";
    }
    else if(strcmp(dataset, "twhub2")==0)
    {
        input_file = "hdfs://[twitter_path]";
        output_file = "hdfs://[output_path]";
        opt="hub2_DG";
        hub_file="[twitter_hub_path]";
        query_file="[twitter_query_path]";
    }
    else if(strcmp(dataset, "bthub2new")==0)
    {
        input_file = "hdfs://[btc_path]";
        output_file = "hdfs://[output_path]";
        opt="hub2new_UG";
        hub_file="[btc_hub_path]";
        query_file="[btc_query_path]";
    }
    else if(strcmp(dataset, "twhub2new")==0)
    {
        input_file = "hdfs://[twitter_path]";
        output_file = "hdfs://[output_path]";
        opt="hub2new_DG";
        hub_file="[twitter_hub_path]";
        query_file="[twitter_query_path]";
    }

    else if(strcmp(dataset, "btbibfs")==0)
    {
        input_file = "hdfs://[btc_path]";
        output_file = "hdfs://[output_path]";
        query_file="[btc_query_path]";
        opt="bibfsUG";

    }
    else if(strcmp(dataset, "twbibfs")==0)
    {
        input_file = "hdfs://[twitter_path]";
        output_file = "hdfs://[output_path]";
        query_file="[twitter_query_path]";
        opt="bibfsDG";

    }
    else return 0;

    std::string exec_type = "synchronous";
    graphlab::distributed_control dc;
    global_logger().set_log_level(LOG_INFO);

    dc.cout()<<"Start to run!"<<std::endl;

    if(strcmp(opt, "bfs") == 0)
    {
        graphlab::timer t;
        t.start();

        UndirectedGraph graph(dc);
        graph.load(input_file, UndirectedParser);
        graph.finalize();


        dc.cout() << "Loading graph in " << t.current_time() << " seconds." << std::endl;

        double t_compute = 0, t_dump = 0;
        for(int i = 0 ;i < QUERY_SIZE; i ++)
        {
            SOURCE_VERTEX = SOURCE_LIST[i];
            DEST_VERTEX = DEST_LIST[i];

            t.start();

            graph.transform_vertices(init_vertex);
            graphlab::omni_engine<SingleSourceBFS> engine(dc, graph, exec_type);
            engine.signal(SOURCE_VERTEX, SingleSourceBFS_type(0));
            engine.start();

            t_compute +=  t.current_time();

            t.start();

            std::stringstream srtm;
            srtm << output_file << "_" << i;
            graph.save(srtm.str().c_str(), SingleSourceBFS_writer(), false, // set to true if each output file is to be gzipped
                    true, // whether vertices are saved
                    false); // whether edges are saved

            t_dump += t.current_time();
        }

        dc.cout() << "Finished Running engine in " << t_compute << " seconds." << std::endl;
        dc.cout() << "Dumping graph in " << t_dump << " seconds."   << std::endl;
    }
    else if(strcmp(opt, "hub2_indexing_UG")==0||strcmp(opt, "hub2_indexing_DG")==0)
    {
        graphlab::timer t;
        t.start();

        dc.cout()<<"Loading graph..."<<std::endl;



        double t_compute=0, t_dump=0;

        if(strcmp(opt,"hub2_indexing_DG")==0)
        {
            IndexingGraph_Directed graph(dc);
            dc.cout()<<"Loading directed graph: "<<input_file<<std::endl;
            graph.load(input_file, DirectedIndexingParser);

            graph.finalize();

            dc.cout()<<"Load finished."<<std::endl;

            dc.cout()<<"Loading graph in "<<t.current_time()<<" seconds."<<std::endl;

            load_top_vertices(hub_file);

            dc.cout()<<"HUBLIST size is: "<<HUBLIST.size()<<std::endl;

            for(int i=0;i<HUBLIST.size();i++)
            {
                CUR_HUB=HUBLIST[i];

                dc.cout()<<"Processing hub"<<i<<":"<<CUR_HUB<<std::endl;

                t.start();

                graph.transform_vertices(init_indexingvertex_Directed);

                graphlab::omni_engine<Hub2Indexing_Directed> engine(dc, graph, exec_type);
                engine.signal(CUR_HUB, Hub2Indexing_Directed_type((char)3, 0, 0));
                engine.start();

                double cur_computetime = t.current_time();
                t_compute+=cur_computetime;

                dc.cout()<<"Process finished with "<<cur_computetime<<"s! Dumping .."<<std::endl;
                t.start();
                std::stringstream srtm;
                srtm<<output_file <<"_"<<i;
                graph.save(srtm.str().c_str(), Hub2Indexing_Directed_writer(), false, true, false);

                double cur_dumptime=t.current_time();
                t_dump+=cur_dumptime;

                dc.cout()<<"Dump finished with "<<cur_dumptime<<"s! Next!"<<std::endl;
            }

        }
        else
        {
            IndexingGraph graph(dc);
            dc.cout()<<"Loading undirected graph: "<<input_file<<std::endl;
            graph.load(input_file, UndirectedIndexingParser);

            graph.finalize();

            dc.cout()<<"Load finished."<<std::endl;

            dc.cout()<<"Loading graph in "<<t.current_time()<<" seconds."<<std::endl;

            load_top_vertices(hub_file);

            dc.cout()<<"HUBLIST size is: "<<HUBLIST.size()<<std::endl;

            //double t_compute=0, t_dump=0;
            for(int i=0;i<HUBLIST.size();i++)
            {
                CUR_HUB=HUBLIST[i];

                dc.cout()<<"Processing hub"<<i<<":"<<CUR_HUB<<std::endl;

                t.start();

                graph.transform_vertices(init_indexingvertex);

                graphlab::omni_engine<Hub2Indexing> engine(dc, graph, exec_type);
                engine.signal(CUR_HUB, Hub2Indexing_type(false, 0));
                engine.start();

                double cur_computetime = t.current_time();
                t_compute+=cur_computetime;

                dc.cout()<<"Process finished with "<<cur_computetime<<"s! Dumping .."<<std::endl;
                t.start();
                std::stringstream srtm;
                srtm<<output_file <<"_"<<i;
                graph.save(srtm.str().c_str(), Hub2Indexing_writer(), false, true, false);

                double cur_dumptime=t.current_time();
                t_dump+=cur_dumptime;

                dc.cout()<<"Dump finished with "<<cur_dumptime<<"s! Next!"<<std::endl;
            }

        }


        dc.cout()<<"Finishing running engine in "<<t_compute<<" seconds."<<std::endl;
        dc.cout()<<"Dumping graph in "<<t_dump<<" seconds."<<std::endl<<std::endl;

    }
    else if(strcmp(opt, "hub2_DG") == 0 || strcmp(opt, "hub2_UG") == 0)
    {
        graphlab::timer t;
        t.start();

        DirectedGraph graph(dc);
        if(strcmp(opt, "hub2_UG") == 0 )
        {
            graph.load(input_file, UndirectedBiParser);
        }
        else
        {
            graph.load(input_file, DirectedBiParser);
        }
        graph.finalize();


        dc.cout() << "Loading graph in " << t.current_time() << " seconds." << std::endl;

        load_top_vertices(hub_file);
        load_queries(query_file);

        dc.cout()<<"HUBLIST size is: "<<HUBLIST.size()<<std::endl;
        dc.cout()<<"QUERYLIST size is:"<<gQueryList.size()<<std::endl;

        double t_compute = 0, t_dump = 0;
        for(int i = 0 ;i < gQueryList.size(); i ++)
        {
            SOURCE_VERTEX = gQueryList[i].src;
            DEST_VERTEX = gQueryList[i].dst;
            int upperBound=gQueryList[i].upperbound;

            ForwardCover = 0;
            BackwardCover = 0;

            dc.cout()<<"Processing query"<<i<<" <"<<SOURCE_VERTEX<<","<<DEST_VERTEX<<"> with upper bound: "<<upperBound<<std::endl;


            t.start();
            graph.transform_vertices(init_bivertex);

            BiIteration = 0;
            upperBound=(upperBound+1)/2;
            while(true)
            {
                BiIteration ++;

                graphlab::omni_engine<Hub2> engine(dc, graph, exec_type);
                engine.map_reduce_vertices<graphlab::empty>(signal_vertices);
                engine.start();

                BiDist = graph.map_reduce_vertices<min_t>(get_dist).value;

                //dc.cout() << "BiDist: " << BiDist << std::endl;

                if(BiDist != inf)
                    break;

                int fc = graph.map_reduce_vertices<size_t>(count_forward_cover);
                int bc = graph.map_reduce_vertices<size_t>(count_backward_cover);

                //dc.cout() << "FC: " << fc << " BC: " << bc << std::endl;

                if(fc - ForwardCover == 0 || bc - BackwardCover == 0 || BiIteration == upperBound -1)
                {
                    BiDist = upperBound;
                    break;
                }
                else
                {
                    ForwardCover = fc;
                    BackwardCover = bc;
                }
            }
            graph.transform_vertices(set_dist);
            double cur_tcompute=t.current_time();
            t_compute += cur_tcompute;

            dc.cout()<<"Process finished with "<<cur_tcompute<<"s! Dumping .."<<std::endl;

            t.start();

            std::stringstream srtm;
            srtm << output_file << "_" << i;
            graph.save(srtm.str().c_str(), BiBFS_writer(), false, // set to true if each output file is to be gzipped
                    true, // whether vertices are saved
                    false); // whether edges are saved

            double cur_dumptime=t.current_time();
            t_dump+=cur_dumptime;

            dc.cout()<<"Dump finished with "<<cur_dumptime<<"s! Next!"<<std::endl;
        }

        dc.cout() << "Finished running engine in " << t_compute << " seconds." << std::endl;
        dc.cout() << "Dumping graph in " << t_dump << " seconds."   << std::endl;
    }
    else if(strcmp(opt, "bibfsDG") == 0 || strcmp(opt, "bibfsUG") == 0)
    {
        graphlab::timer t;
        t.start();

        DirectedGraph graph(dc);
        if(strcmp(opt, "bibfsUG") == 0 )
        {
            graph.load(input_file, UndirectedBiParser);
        }
        else
        {
            graph.load(input_file, DirectedBiParser);
        }
        graph.finalize();


        dc.cout() << "Loading graph in " << t.current_time() << " seconds." << std::endl;
        
        load_queries(query_file);

        dc.cout()<<"QUERYLIST size is:"<<gQueryList.size()<<std::endl;

        double t_compute = 0, t_dump = 0;
        for(int i = 0 ;i < gQueryList.size(); i ++)
        {
            SOURCE_VERTEX = gQueryList[i].src;
            DEST_VERTEX = gQueryList[i].dst;

            ForwardCover = 0;
            BackwardCover = 0;

            t.start();
            graph.transform_vertices(init_bivertex);

            BiIteration = 0;
            while(true)
            {
                BiIteration ++;

                graphlab::omni_engine<BiBFS> engine(dc, graph, exec_type);
                engine.map_reduce_vertices<graphlab::empty>(signal_vertices);
                engine.start();

                BiDist = graph.map_reduce_vertices<min_t>(get_dist).value;

                //dc.cout() << "BiDist: " << BiDist << std::endl;

                if(BiDist != inf)
                    break;

                int fc = graph.map_reduce_vertices<size_t>(count_forward_cover);
                int bc = graph.map_reduce_vertices<size_t>(count_backward_cover);

                //dc.cout() << "FC: " << fc << " BC: " << bc << std::endl;

                if(fc - ForwardCover == 0 || bc - BackwardCover == 0)
                {
                    BiDist = inf;
                    break;
                }
                else
                {
                    ForwardCover = fc;
                    BackwardCover = bc;
                }
            }
            graph.transform_vertices(set_dist);
            t_compute +=  t.current_time();

            t.start();

            std::stringstream srtm;
            srtm << output_file << "_" << i;
            graph.save(srtm.str().c_str(), BiBFS_writer(), false, // set to true if each output file is to be gzipped
                    true, // whether vertices are saved
                    false); // whether edges are saved

            t_dump += t.current_time();
        }

        dc.cout() << "Finished Running engine in " << t_compute << " seconds." << std::endl;
        dc.cout() << "Dumping graph in " << t_dump << " seconds."   << std::endl;
    }
    else if(strcmp(opt, "KHop") == 0)
    {
        graphlab::timer t;
        t.start();

        UndirectedGraph graph(dc);
        graph.load(input_file, UndirectedParser);
        graph.finalize();


        dc.cout() << "Loading graph in " << t.current_time() << " seconds." << std::endl;

        double t_compute = 0, t_dump = 0;
        for(int i = 0 ;i < QUERY_SIZE; i ++)
        {
            SOURCE_VERTEX = SOURCE_LIST[i];

            t.start();

            graph.transform_vertices(init_vertex);
            graphlab::omni_engine<KHopBFS> engine(dc, graph, exec_type);
            engine.signal(SOURCE_VERTEX, KHop_type(0));
            engine.start();

            t_compute +=  t.current_time();

            t.start();

            std::stringstream srtm;
            srtm << output_file << "_" << i;
            graph.save(srtm.str().c_str(), KHop_writer(), false, // set to true if each output file is to be gzipped
                    true, // whether vertices are saved
                    false); // whether edges are saved

            t_dump += t.current_time();
        }

        dc.cout() << "Finished Running engine in " << t_compute << " seconds." << std::endl;
        dc.cout() << "Dumping graph in " << t_dump << " seconds."   << std::endl;
    }
    else if(strcmp(opt, "hub2new_DG") == 0 || strcmp(opt, "hub2new_UG") == 0)
    {
        graphlab::timer t;
        t.start();

        DirectedGraph graph(dc);
        if(strcmp(opt, "hub2new_UG") == 0 )
        {
            graph.load(input_file, UndirectedBiParser);
        }
        else
        {
            graph.load(input_file, DirectedBiParser);
        }
        graph.finalize();


        dc.cout() << "Loading graph in " << t.current_time() << " seconds." << std::endl;

        load_top_vertices(hub_file);
        load_queries(query_file);

        dc.cout()<<"HUBLIST size is: "<<HUBLIST.size()<<std::endl;
        dc.cout()<<"QUERYLIST size is:"<<gQueryList.size()<<std::endl;

        double t_compute = 0, t_dump = 0;
        for(int i = 0 ;i < gQueryList.size(); i ++)
        {
            SOURCE_VERTEX = gQueryList[i].src;
            DEST_VERTEX = gQueryList[i].dst;
            UPPER_BOUND=gQueryList[i].upperbound;
            UPPER_BOUND=(UPPER_BOUND+1)/2;

            dc.cout()<<"Processing query"<<i<<" <"<<SOURCE_VERTEX<<","<<DEST_VERTEX<<"> with upper bound: "<<UPPER_BOUND<<std::endl;

            t.start();
            graph.transform_vertices(init_bivertex);

            graphlab::omni_engine<Hub2_New> engine(dc, graph, exec_type);
            engine.signal(SOURCE_VERTEX, 1);
            engine.signal(DEST_VERTEX, 2);
            engine.start();

            BiDist = graph.map_reduce_vertices<min_t>(get_dist).value;
            if(BiDist>=inf) BiDist=UPPER_BOUND;

            double cur_tcompute=t.current_time();
            t_compute += cur_tcompute;

            dc.cout()<<"Process finished with "<<cur_tcompute<<"s! Result is:"<<BiDist<<" Dumping .."<<std::endl;

            t.start();

            std::stringstream srtm;
            srtm << output_file << "_" << i;
            graph.save(srtm.str().c_str(), BiBFS_writer(), false, // set to true if each output file is to be gzipped
                    true, // whether vertices are saved
                    false); // whether edges are saved

            double cur_dumptime=t.current_time();
            t_dump+=cur_dumptime;

            dc.cout()<<"Dump finished with "<<cur_dumptime<<"s! Next!"<<std::endl;
        }

        dc.cout() << "Finished running engine in " << t_compute << " seconds." << std::endl;
        dc.cout() << "Dumping graph in " << t_dump << " seconds."   << std::endl;
    }

    graphlab::mpi_tools::finalize();

    return 0;
}
