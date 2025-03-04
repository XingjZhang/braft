// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#ifndef  BRAFT_NODE_MANAGER_H
#define  BRAFT_NODE_MANAGER_H

#include <butil/memory/singleton.h>
#include <butil/containers/doubly_buffered_data.h>
#include "braft/raft.h"
#include "braft/util.h"

namespace braft {

class NodeImpl;

class NodeManager {
public:
    static NodeManager* GetInstance() {
        return Singleton<NodeManager>::get();
    }

    // add raft node
    bool add(NodeImpl* node);
    // add learner node
    bool add(LearnerImpl* node);

    // remove raft node
    bool remove(NodeImpl* node);
    // remove learner node
    bool remove(LearnerImpl* node);

    // get node by group_id and peer_id
    scoped_refptr<NodeImpl> get_node(const GroupId& group_id, const PeerId& peer_id);
    // get node by group_id and peer_id
    scoped_refptr<LearnerImpl> get_learner(const GroupId& group_id, const PeerId& peer_id);

    // get all the nodes of |group_id|
    void get_nodes_by_group_id(const GroupId& group_id, 
                               std::vector<scoped_refptr<NodeImpl> >* nodes);

    // get all the learner nodes of |group_id|
    void get_learners_by_group_id(const GroupId& group_id, 
                               std::vector<scoped_refptr<LearnerImpl> >* learner_nodes);
    void get_all_nodes(std::vector<scoped_refptr<NodeImpl> >* nodes);
    void get_all_learner_nodes(std::vector<scoped_refptr<LearnerImpl> >* learner_nodes);

    // Add service to |server| at |listen_addr|
    int add_service(brpc::Server* server, 
                    const butil::EndPoint& listen_addr);

    // Return true if |addr| is reachable by a RPC Server
    bool server_exists(butil::EndPoint addr);

    // Remove the addr from _addr_set when the backing service is destroyed
    void remove_address(butil::EndPoint addr);

private:
    NodeManager();
    ~NodeManager();
    DISALLOW_COPY_AND_ASSIGN(NodeManager);
    friend struct DefaultSingletonTraits<NodeManager>;
    
    // TODO(chenzhangyi01): replace std::map with FlatMap
    // To make implementation simplicity, we use two maps here, although
    // it works practically with only one GroupMap
    typedef std::map<NodeId, scoped_refptr<NodeImpl> > NodeMap;
    typedef std::map<NodeId, scoped_refptr<LearnerImpl> > LearnerMap;
    typedef std::multimap<GroupId, NodeImpl* > NodeGroupMap;
    typedef std::multimap<GroupId, LearnerImpl* > LearnerGroupMap;
    struct Maps {
        NodeMap node_map;
        LearnerMap learner_map;
        NodeGroupMap node_group_map;
        LearnerGroupMap learner_group_map;
    };
    // Functor to modify DBD
    static size_t _add_node(Maps&, const NodeImpl* node);
    static size_t _remove_node(Maps&, const NodeImpl* node);
    static size_t _add_learner(Maps&, const LearnerImpl* node);
    static size_t _remove_learner(Maps&, const LearnerImpl* node);

    butil::DoublyBufferedData<Maps> _nodes;

    raft_mutex_t _mutex;
    std::set<butil::EndPoint> _addr_set;
};

#define global_node_manager NodeManager::GetInstance()

}   //  namespace braft

#endif  // BRAFT_NODE_MANAGER_H
