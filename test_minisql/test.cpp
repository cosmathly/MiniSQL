#include <bits/stdc++.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "Buffer_Manager.h"
#include <deque>
using namespace std;
#include <deque>
using namespace std;
typedef int node_pointer; 
typedef int offset_t;
typedef struct Data_Pointer
{
        offset_t offset_file;
        offset_t offset_block;
} data_pointer;
class BPT_Node
{
      public:
         int key_num; // 关键字的数量
         node_pointer parent;
         std::deque<string> key;
         std::deque<node_pointer> child;
         std::deque<data_pointer> data;
         node_pointer pre_leaf;
         node_pointer next_leaf;
         bool is_leaf; // 是否是叶子节点
         BPT_Node() { }
         ~BPT_Node() { }
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
class BPT_File
{
      public:
         int fd;
         offset_t offset_file;
         BPT_File();
         ~BPT_File();
};
BPT_File::BPT_File()
{
          fd = open("./DB_Files/BPT_File", O_RDWR|O_CREAT, 0777);
          offset_t cur_offset = lseek(fd, 0, SEEK_END);
          if(cur_offset==0) offset_file = 0;
          else offset_file = cur_offset+Block_Size-(cur_offset%Block_Size);
}
BPT_File::~BPT_File()
{
          close(fd);
}
int main()
{
    BPT_Node a;
    a.key.push_back("hello");
    a.key.push_back("world");
    char *buf;
    BPT_Node *b = new BPT_Node;
    Buffer buffer;
    BPT_File bpt_file;
    buffer.Write(bpt_file.fd, 0, 0, &a, sizeof(a));
    buffer.Read(bpt_file.fd, 0, 0, &buf);
    for(int i = 0; i < Block_Size; i++)
    if((*(buf+i))=='\n')
    {
       memcpy(b, buf, i);
       break;
    }
    for(auto it = b->key.begin(); it != b->key.end(); it++)
    cout<<(*it)<<endl;
    BPT_Node *c = new BPT_Node;
    buffer.Read(bpt_file.fd, 0, 0, &buf);
    for(int i = 0; i < Block_Size; i++)
    if((*(buf+i))=='\n')
    {
       memcpy(c, buf, i);
       break;
    }
    for(auto it = c->key.begin(); it != c->key.end(); it++)
    cout<<(*it)<<endl;
    return 0;
}