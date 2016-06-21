// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
//         Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/04 14:00:08

#ifndef PUBLIC_RAFT_RAFT_SNAPSHOT_H
#define PUBLIC_RAFT_RAFT_SNAPSHOT_H

#include <string>
#include "raft/storage.h"
#include "raft/macros.h"
#include "raft/local_file_meta.pb.h"

namespace raft {

class LocalSnapshotMetaTable {
public:
    LocalSnapshotMetaTable();
    ~LocalSnapshotMetaTable();
    // Add file to the meta
    int add_file(const std::string& filename, 
                 const LocalFileMeta& file_meta);
    int remove_file(const std::string& filename);
    int save_to_file(const std::string& path) const;
    int load_from_file(const std::string& path);
    int get_file_meta(const std::string& filename, LocalFileMeta* file_meta) const;
    void list_files(std::vector<std::string> *files) const;
    bool has_meta() { return _meta.IsInitialized(); }
    const SnapshotMeta& meta() { return _meta; }
    void set_meta(const SnapshotMeta& meta) { _meta = meta; }
    int save_to_iobuf_as_remote(base::IOBuf* buf) const;
    int load_from_iobuf_as_remote(const base::IOBuf& buf);
    void swap(LocalSnapshotMetaTable& rhs);
private:
    // Intentionally copyable
    typedef std::map<std::string, LocalFileMeta> Map;
    Map    _file_map;
    SnapshotMeta _meta;
};

// Describe the Snapshot on another machine
class RemoteSnapshot : public Snapshot {
friend class LocalSnapshotStorage;
public:
    // Get the path of the Snapshot
    virtual std::string get_path() = 0;
    // List all the existing files in the Snapshot currently
    virtual void list_files(std::vector<std::string> *files);
    // Get the implementation-defined file_meta
    virtual int get_file_meta(const std::string& filename, 
                              ::google::protobuf::Message* file_meta);
private:
    LocalSnapshotMetaTable _meta_table;
};

class LocalSnapshotWriter : public SnapshotWriter {
friend class LocalSnapshotStorage;
public:
    int64_t snapshot_index();
    virtual int init();
    virtual int save_meta(const SnapshotMeta& meta);
    virtual std::string get_path() { return _path; }
    // Add file to the snapshot. It would fail it the file doesn't exist nor
    // references to any other file.
    // Returns 0 on success, -1 otherwise.
    virtual int add_file(const std::string& filename, 
                         const ::google::protobuf::Message* file_meta);
    // Remove a file from the snapshot, it doesn't guarantees that the real file
    // would be removed from the storage.
    virtual int remove_file(const std::string& filename);
    // List all the existing files in the Snapshot currently
    virtual void list_files(std::vector<std::string> *files);

    // Get the implementation-defined file_meta
    virtual int get_file_meta(const std::string& filename, 
                              ::google::protobuf::Message* file_meta);
    // Sync meta table to disk
    int sync();
private:
    // Users shouldn't create LocalSnapshotWriter Directly
    LocalSnapshotWriter(const std::string& path);
    virtual ~LocalSnapshotWriter();

    std::string _path;
    LocalSnapshotMetaTable _meta_table;
};

class LocalSnapshotReader: public SnapshotReader {
friend class LocalSnapshotStorage;
public:
    int64_t snapshot_index();
    virtual int init();
    virtual int load_meta(SnapshotMeta* meta);
    // Get the path of the Snapshot
    virtual std::string get_path() { return _path; }
    // Generate uri for other peers to copy this snapshot.
    // Return an empty string if some error has occcured
    virtual std::string generate_uri_for_copy();
    // List all the existing files in the Snapshot currently
    virtual void list_files(std::vector<std::string> *files);

    // Get the implementation-defined file_meta
    virtual int get_file_meta(const std::string& filename, 
                              ::google::protobuf::Message* file_meta);
private:
    // Users shouldn't create LocalSnapshotReader Directly
    LocalSnapshotReader(const std::string& path,
                        base::EndPoint server_addr);
    virtual ~LocalSnapshotReader();
    void destroy_reader_in_file_service();

    std::string _path;
    LocalSnapshotMetaTable _meta_table;
    base::EndPoint _addr;
    int64_t _reader_id;
};

class LocalSnapshotStorage : public SnapshotStorage {
public:
    explicit LocalSnapshotStorage(const std::string& path);
    LocalSnapshotStorage() {}
    virtual ~LocalSnapshotStorage();

    static const char* _s_temp_path;

    virtual int init();
    virtual SnapshotWriter* create() WARN_UNUSED_RESULT;
    virtual int close(SnapshotWriter* writer);

    virtual SnapshotReader* open() WARN_UNUSED_RESULT;
    virtual int close(SnapshotReader* reader);
    virtual SnapshotReader* copy_from(const std::string& uri) WARN_UNUSED_RESULT;

    SnapshotStorage* new_instance(const std::string& uri) const;
    void set_server_addr(base::EndPoint server_addr) { _addr = server_addr; }
    bool has_server_addr() { return _addr != base::EndPoint(); }
private:
    void ref(const int64_t index);
    void unref(const int64_t index);

    raft_mutex_t _mutex;
    std::string _path;
    int64_t _last_snapshot_index;
    std::map<int64_t, int> _ref_map;
    base::EndPoint _addr;
};

}  // namespace raft

#endif //~PUBLIC_RAFT_RAFT_SNAPSHOT_H
