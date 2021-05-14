#ifndef _API_
#define _API_

#include "Buffer_Manager.h"
#include "Catalog_Manager.h"
#include <cstring>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <vector>
#include <deque>
#include <bits/stdc++.h>
#include "Data_Convert.h"
using namespace std;
class BPT_File;
class Table_File;
class BPT_Info_File;
typedef offset_t node_pointer;
typedef int Max_Son_Node_Num; // B+树的阶
class BPT_Node;
class BPstring;
typedef int type_t;
class Table_File
{
      public:
         File_Descriptor fd;
         offset_t cur_pos;
         Table_File();
         ~Table_File() { close(fd); }
         void update();
};
class Record
{
      public:
         std::vector<std::string> attr;
         Record() { }
         ~Record() { }
};
class Record_Set
{
      public:
         std::vector<Record> record;
         Record_Set() { }
         ~Record_Set() { }
};
class BPT_Info_File
{
      public:
         File_Descriptor fd;
         offset_t offset_file;
         BPT_Info_File();
         ~BPT_Info_File(); 
};
typedef node_pointer BPT_Pointer;
typedef node_pointer leaf_node;
class BPT_Info
{
      public:
         bool is_del;
         std::string table_name;
         std::string attr_name;
         BPT_Pointer BPT;
         BPT_Info()
         {
            BPT = -1;
            is_del = false;
         }
         ~BPT_Info() { }
};
class BPT_File
{
      public:
         File_Descriptor fd;
         offset_t offset_file;
         BPT_File();
         ~BPT_File();
};
typedef struct Data_Pointer
{
        offset_t offset_file;
        offset_t offset_block;
} data_pointer;
class BPT_Node
{
      public:
         int key_num; // 关键字的数量
         bool is_leaf;
         node_pointer parent;
         node_pointer pre_leaf;
         node_pointer next_leaf;
         std::list<string> key;
         std::list<node_pointer> child;
         std::list<data_pointer> data;
          // 是否是叶子节点
         BPT_Node();
         ~BPT_Node() { }
};
class All_Data
{
      public: 
         std::vector<data_pointer> data;
         All_Data() { }
         ~All_Data() { }
};
class BPT
{
      public:
         BPT_Pointer bpt;
         data_type type;
         Max_Son_Node_Num m;
         node_pointer root;
         bool greater(string a, string b) const;
         bool smaller(string a, string b) const;
         bool equal(string a, string b) const;
         bool greater_equal(string a, string b) const;
         bool smaller_equal(string a, string b) const;
         BPT(size_t key_size, BPT_Pointer bpt, data_type type);
         BPT(node_pointer root, Max_Son_Node_Num m, data_type type, BPT_Pointer bpt);
         void write_bpt_to_file() const;
         int insert(string one_key, data_pointer one_data); // 插入一个关键字
         int del(string one_key);
         int add_key_and_child(node_pointer des_node, string one_key, node_pointer src_node);
         void update(BPT_Node *fa_node, node_pointer src_node, string des_key);
         void del_key_and_child(BPT_Node *fa_node, node_pointer des_node);
         void update_l(BPT_Node *fa_node, BPT_Node *l_brother, BPT_Node *cur_node, node_pointer l_node);
         void update_r(BPT_Node *fa_node, BPT_Node *r_brother, BPT_Node *cur_node, node_pointer r_node);
         void merge_l(BPT_Node *fa_node, BPT_Node *l_brother, BPT_Node *cur_node, node_pointer l_node);
         void merge_r(BPT_Node *fa_node, BPT_Node *r_brother, BPT_Node *cur_node, node_pointer r_node);
         leaf_node find(string des_key) const;
         Record * find_equal(string des_key) const; 
         bool check_if_exist(string des_key) const;
         leaf_node find_leftest() const; // 寻找最左边的叶子节点
         Record_Set * find_not_equal(string des_key) const;
         Record_Set * find_greater(string des_key) const;
         Record_Set * find_smaller(string des_key) const;
         Record_Set * find_greater_equal(string des_key) const;
         Record_Set * find_smaller_equal(string des_key) const;
         Record_Set * find_all() const;
         All_Data * find_all_data() const;
};

#endif