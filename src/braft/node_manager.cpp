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

#include "braft/node.h"
#include "braft/learner.h"
#include "braft/node_manager.h"
#include "braft/file_service.h"
#include "braft/builtin_service_impl.h"
#include "braft/cli_service.h"
#include "braft/snapshot_service.h"

namespace braft {

NodeManager::NodeManager() {}

NodeManager::~NodeManager() {}

bool NodeManager::server_exists(butil::EndPoint addr) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (addr.ip != butil::IP_ANY) {
        butil::EndPoint any_addr(butil::IP_ANY, addr.port);
        if (_addr_set.find(any_addr) != _addr_set.end()) {
            return true;
        }
    }
    return _addr_set.find(addr) != _addr_set.end();
}

void NodeManager::remove_address(butil::EndPoint addr) {
    BAIDU_SCOPED_LOCK(_mutex);
    _addr_set.erase(addr);
}

int NodeManager::add_service(brpc::Server* server, 
                             const butil::EndPoint& listen_address) {
    if (server == NULL) {
        LOG(ERROR) << "server is NULL";
        return -1;
    }
    if (server_exists(listen_address)) {
        return 0;
    }

    if (0 != server->AddService(file_service(), brpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(ERROR) << "Fail to add FileService";
        return -1;
    }

    if (0 != server->AddService(
                new RaftServiceImpl(listen_address), 
                brpc::SERVER_OWNS_SERVICE)) {
        LOG(ERROR) << "Fail to add RaftService";
        return -1;
    }

    if (0 != server->AddService(new RaftStatImpl, brpc::SERVER_OWNS_SERVICE)) {
        LOG(ERROR) << "Fail to add RaftStatService";
        return -1;
    }
    if (0 != server->AddService(new CliServiceImpl, brpc::SERVER_OWNS_SERVICE)) {
        LOG(ERROR) << "Fail to add CliService";
        return -1;
    }

    if (0 != server->AddService(
                new SnapshotServiceImpl,
                brpc::SERVER_OWNS_SERVICE)) {
        LOG(ERROR) << "Fail to add SnapshotService";
        return -1;
    }

    {
        BAIDU_SCOPED_LOCK(_mutex);
        _addr_set.insert(listen_address);
    }
    return 0;
}

size_t NodeManager::_add_node(Maps& m, const NodeImpl* node) {
    NodeId node_id = node->node_id();
    std::pair<NodeMap::iterator, bool> ret = m.node_map.insert(
            NodeMap::value_type(node_id, const_cast<NodeImpl*>(node)));
    if (ret.second) {
        m.node_group_map.insert(NodeGroupMap::value_type(
                    node_id.group_id, const_cast<NodeImpl*>(node)));
        return 1;
    }
    return 0;
}

size_t NodeManager::_remove_node(Maps& m, const NodeImpl* node) {
    NodeMap::iterator iter = m.node_map.find(node->node_id());
    if (iter == m.node_map.end() || iter->second.get() != node) {
                                  // ^^
                                  // Avoid duplicated nodes
        return 0;
    }
    m.node_map.erase(iter);
    std::pair<NodeGroupMap::iterator, NodeGroupMap::iterator> 
            range = m.node_group_map.equal_range(node->node_id().group_id);
    for (NodeGroupMap::iterator it = range.first; it != range.second; ++it) {
        if (it->second == node) {
            m.node_group_map.erase(it);
            return 1;
        }
    }
    CHECK(false) << "Can't reach here";
    return 0;
}

size_t NodeManager::_add_learner(Maps& m, const LearnerImpl* node) {
    NodeId node_id = node->node_id();
    std::pair<LearnerMap::iterator, bool> ret = m.learner_map.insert(
            LearnerMap::value_type(node_id, const_cast<LearnerImpl*>(node)));
    if (ret.second) {
        m.learner_group_map.insert(LearnerGroupMap::value_type(
                    node_id.group_id, const_cast<LearnerImpl*>(node)));
        return 1;
    }
    return 0;
}

size_t NodeManager::_remove_learner(Maps& m, const LearnerImpl* node) {
    LearnerMap::iterator iter = m.learner_map.find(node->node_id());
    if (iter == m.learner_map.end() || iter->second.get() != node) {
                                  // ^^
                                  // Avoid duplicated nodes
        return 0;
    }
    m.learner_map.erase(iter);
    std::pair<LearnerGroupMap::iterator, LearnerGroupMap::iterator> 
            range = m.learner_group_map.equal_range(node->node_id().group_id);
    for (LearnerGroupMap::iterator it = range.first; it != range.second; ++it) {
        if (it->second == node) {
            m.learner_group_map.erase(it);
            return 1;
        }
    }
    CHECK(false) << "Can't reach here";
    return 0;
}

bool NodeManager::add(NodeImpl* node) {
    // check address ok?
    if (!server_exists(node->node_id().peer_id.addr)) {
        return false;
    }

    return _nodes.Modify(_add_node, node) != 0;
}

bool NodeManager::remove(NodeImpl* node) {
    return _nodes.Modify(_remove_node, node) != 0;
}

bool NodeManager::add(LearnerImpl* node) {
    // check address ok?
    if (!server_exists(node->node_id().peer_id.addr)) {
        return false;
    }

    return _nodes.Modify(_add_learner, node) != 0;
}

bool NodeManager::remove(LearnerImpl* node) {
    return _nodes.Modify(_remove_learner, node) != 0;
}

scoped_refptr<LearnerImpl> NodeManager::get_learner(const GroupId& group_id, const PeerId& peer_id) {
    butil::DoublyBufferedData<Maps>::ScopedPtr ptr;
    if (_nodes.Read(&ptr) != 0) {
        return NULL;
    }
    LearnerMap::const_iterator it = ptr->learner_map.find(NodeId(group_id, peer_id));
    if (it != ptr->learner_map.end()) {
        return it->second;
    }
    return NULL;
}

scoped_refptr<NodeImpl> NodeManager::get_node(const GroupId& group_id, const PeerId& peer_id) {
    butil::DoublyBufferedData<Maps>::ScopedPtr ptr;
    if (_nodes.Read(&ptr) != 0) {
        return NULL;
    }
    NodeMap::const_iterator it = ptr->node_map.find(NodeId(group_id, peer_id));
    if (it != ptr->node_map.end()) {
        return it->second;
    }
    return NULL;
}

void NodeManager::get_nodes_by_group_id(
        const GroupId& group_id, std::vector<scoped_refptr<NodeImpl> >* nodes) {

    nodes->clear();
    butil::DoublyBufferedData<Maps>::ScopedPtr ptr;
    if (_nodes.Read(&ptr) != 0) {
        return;
    }
    std::pair<NodeGroupMap::const_iterator, NodeGroupMap::const_iterator> 
            range = ptr->node_group_map.equal_range(group_id);
    for (NodeGroupMap::const_iterator it = range.first; it != range.second; ++it) {
        nodes->push_back(it->second);
    }
}

void NodeManager::get_all_nodes(std::vector<scoped_refptr<NodeImpl> >* nodes) {
    nodes->clear();
    butil::DoublyBufferedData<Maps>::ScopedPtr ptr;
    if (_nodes.Read(&ptr) != 0) {
        return;
    }
    nodes->reserve(ptr->node_group_map.size());
    for (NodeGroupMap::const_iterator 
            it = ptr->node_group_map.begin(); it != ptr->node_group_map.end(); ++it) {
        nodes->push_back(it->second);
    }
}

void NodeManager::get_learners_by_group_id(
        const GroupId& group_id, std::vector<scoped_refptr<LearnerImpl> >* learner_nodes) {

    learner_nodes->clear();
    butil::DoublyBufferedData<Maps>::ScopedPtr ptr;
    if (_nodes.Read(&ptr) != 0) {
        return;
    }
    std::pair<LearnerGroupMap::const_iterator, LearnerGroupMap::const_iterator> 
            range = ptr->learner_group_map.equal_range(group_id);
    for (LearnerGroupMap::const_iterator it = range.first; it != range.second; ++it) {
        learner_nodes->push_back(it->second);
    }
}

void NodeManager::get_all_learner_nodes(std::vector<scoped_refptr<LearnerImpl> >* learner_nodes) {
    learner_nodes->clear();
    butil::DoublyBufferedData<Maps>::ScopedPtr ptr;
    if (_nodes.Read(&ptr) != 0) {
        return;
    }
    learner_nodes->reserve(ptr->learner_group_map.size());
    for (LearnerGroupMap::const_iterator 
            it = ptr->learner_group_map.begin(); it != ptr->learner_group_map.end(); ++it) {
        learner_nodes->push_back(it->second);
    }
}

}  //  namespace braft

