#include "API.h"
BPT_Info_File::BPT_Info_File()
{
               fd = open("./DB_Files/BPT_Info_File", O_RDWR|O_CREAT, 0777);
               offset_t cur_offset = lseek(fd, 0, SEEK_END);
               if(cur_offset==0) offset_file = 0;
               else offset_file = cur_offset+Block_Size-(cur_offset%Block_Size);
}
BPT_Info_File::~BPT_Info_File()
{ 
     close(fd); 
}
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
Table_File::Table_File()
{
            fd = open("./DB_Files/Table", O_RDWR|O_CREAT, 0777);
            cur_pos = lseek(fd, 0, SEEK_END);       
}
void Table_File::update()
{
     cur_pos = cur_pos+Block_Size-(cur_pos%Block_Size);
}
extern Buffer *buffer;
extern BPT_File *bpt_file;
extern Table_File *table_file;
extern char *err_info;

void BPT::write_bpt_to_file() const
{
        buffer->Write(bpt_file->fd, bpt, 0, (void *)this, sizeof(*this));
}

BPT::BPT(size_t key_size, BPT_Pointer bpt, data_type type)
{
        if(type==Int||type==Float) key_size += 20;
        this->m = (Block_Size-100)/(key_size+sizeof(data_pointer)+sizeof(node_pointer));
        this->root = -1;
        this->bpt = bpt;
        this->type = type;
}

int BPT::add_key_and_child(node_pointer des_node, string one_key, node_pointer src_node)
{
     BPT_Node *cur_node = new BPT_Node;
     char *buf;
     int ret = buffer->Read(bpt_file->fd, des_node, 0, &buf);
     if(ret==-1) return -1;
              for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(cur_node, buf, i);
            break;
         }
     cur_node->key_num++;
     int cur_id = 0;
     for(auto it = cur_node->key.begin(); it != cur_node->key.end(); it++)
     {
         if((*it)>one_key) 
         {
             cur_node->key.insert(it, one_key);
             break;
         }
         cur_id++;
     }
     int insert_id = 0;
     for(auto new_it = cur_node->child.begin(); new_it != cur_node->child.end(); new_it++)
     {
         if(insert_id==cur_id+1)
         {
            cur_node->child.insert(new_it, src_node);
            break;
         }
         insert_id++;
     }
     buffer->Write(bpt_file->fd, des_node, 0, cur_node, sizeof(*cur_node));
     return 0;
}
using namespace std;
bool BPT::greater(string a, string b) const
{
     if(type==Int)
     {
        int _a;
        int _b;
        stringstream s1;
        s1 << a;
        s1 >> _a;
        stringstream s2;
        s2 << b;
        s2 >> _b;
        return _a>_b;
     }
     else
     {
        float _a;
        float _b;
        stringstream s1;
        s1 << a;
        s1 >> _a;
        stringstream s2;
        s2 << b;
        s2 >> _b;
        return _a>_b;
     }
}
bool BPT::smaller(string a, string b) const
{
     if(type==Int)
     {
        int _a;
        int _b;
        stringstream s1;
        s1 << a;
        s1 >> _a;
        stringstream s2;
        s2 << b;
        s2 >> _b;
        return _a<_b;
     }
     else
     {
        float _a;
        float _b;
        stringstream s1;
        s1 << a;
        s1 >> _a;
        stringstream s2;
        s2 << b;
        s2 >> _b;
        return _a<_b;
     }
}
bool BPT::equal(string a, string b) const
{
     return a==b;
}
bool BPT::greater_equal(string a, string b) const
{
     if(type==Int)
     {
        int _a;
        int _b;
        stringstream s1;
        s1 << a;
        s1 >> _a;
        stringstream s2;
        s2 << b;
        s2 >> _b;
        return _a>=_b;
     }
     else
     {
        float _a;
        float _b;
        stringstream s1;
        s1 << a;
        s1 >> _a;
        stringstream s2;
        s2 << b;
        s2 >> _b;
        return _a>=_b;
     }
}
bool BPT::smaller_equal(string a, string b) const
{
     if(type==Int)
     {
        int _a;
        int _b;
        stringstream s1;
        s1 << a;
        s1 >> _a;
        stringstream s2;
        s2 << b;
        s2 >> _b;
        return _a<=_b;
     }
     else
     {
        float _a;
        float _b;
        stringstream s1;
        s1 << a;
        s1 >> _a;
        stringstream s2;
        s2 << b;
        s2 >> _b;
        return _a<=_b;
     }
}
int BPT::insert(string one_key, data_pointer one_data) 
{ 
    if(root==-1) // 如果为空树
    {
       root = bpt_file->offset_file;
       bpt_file->offset_file += Block_Size;
       BPT_Node new_root;
       new_root.is_leaf = true;
       new_root.pre_leaf = -1;
       new_root.next_leaf = -1;
       new_root.parent = -1;
       new_root.key.push_back(one_key);
       new_root.data.push_back(one_data);
       buffer->Write(bpt_file->fd, root, 0, &new_root, sizeof(new_root));
       return 0;
    }
    // 如果树不为空
    int ret;
    int cur_id;
    int insert_id;
    BPT_Node *cur_node = new BPT_Node;
    char *buf;
    node_pointer now_node = root;
    while(true)
    {
         ret = buffer->Read(bpt_file->fd, now_node, 0, &buf);
         if(ret==-1) return -1; 
         for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(cur_node, buf, i);
            break;
         }
         if(cur_node->is_leaf==true) break;
         cur_id = 0;
         for(auto it = cur_node->key.begin(); it != cur_node->key.end(); it++)
         { 
             if(greater((*it), one_key)) break; 
             cur_id++;
         }
         insert_id = 0;
         for(auto it = cur_node->child.begin(); it != cur_node->child.end(); it++)
         {
             if(insert_id==cur_id) 
             {
                now_node = *it;
                break;
             }
             insert_id++;
         }
    }
    cur_id = 0;
    bool flag = false;
    for(auto it = cur_node->key.begin(); it != cur_node->key.end(); it++)
    {   
        if(greater((*it), one_key)) 
        {
            flag = true;
            cur_node->key.insert(it, one_key);
            cur_node->key_num++;
            insert_id = 0;
            for(auto cur_it = cur_node->data.begin(); cur_it != cur_node->data.end(); cur_it++)
            {
                if(insert_id==cur_id)
                {
                   cur_node->data.insert(cur_it, one_data);
                   break;
                }
            }
            break;
        }
        cur_id++;
    }
    if(flag==false)
    {
       cur_node->key.insert(cur_node->key.end(), one_key);
       cur_node->key_num++;
       cur_node->data.insert(cur_node->data.end(), one_data);
    }
    if(cur_node->key_num<m) return buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
    // 如果关键字的个数达到了m个
    while(now_node!=root&&cur_node->key_num==m)
    {
          if(cur_node->is_leaf==true)
          {
             BPT_Node new_node;
             node_pointer new_node_place = bpt_file->offset_file;
             bpt_file->offset_file += Block_Size;
             new_node.is_leaf = true;
             cur_node->key_num = floor((double)m/2.0);
             new_node.key_num = m-cur_node->key_num;
             new_node.parent = cur_node->parent;
             new_node.next_leaf = cur_node->next_leaf;
             cur_node->next_leaf = new_node_place;
             new_node.pre_leaf = now_node;
             cur_id = 0;
             while(cur_id<new_node.key_num)
             {
                   new_node.key.push_front(cur_node->key.back());
                   cur_node->key.pop_back();
                   new_node.data.push_front(cur_node->data.back());
                   cur_node->data.pop_back();
                   cur_id++;
             }
             ret = add_key_and_child(cur_node->parent, new_node.key.front(), new_node_place);
             if(ret==-1) return -1;
             ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
             if(ret==-1) return -1;
             ret = buffer->Write(bpt_file->fd, new_node_place, 0, &new_node, sizeof(new_node));
             if(ret==-1) return -1;
          }
          else 
          {
             BPT_Node new_node;
             node_pointer new_node_place = bpt_file->offset_file;
             bpt_file->offset_file += Block_Size;
             new_node.is_leaf = false;
             new_node.parent = cur_node->parent;
             cur_node->key_num = floor((double)(m-1)/2.0);
             new_node.key_num = m-cur_node->key_num-1;
             new_node.pre_leaf = -1;
             new_node.next_leaf = -1;
             cur_id = 0;
             while(cur_id<new_node.key_num)
             {
                   new_node.key.push_front(cur_node->key.back());
                   cur_node->key.pop_back();
                   new_node.child.push_front(cur_node->child.back());
                   cur_node->child.pop_back();
                   cur_id++;
                   if(cur_id==new_node.key_num)
                   {
                      new_node.child.push_front(cur_node->child.back());
                      cur_node->child.pop_back();
                   }
             }
             string tmp_key = cur_node->key.back();
             cur_node->key.pop_back();
             ret = add_key_and_child(cur_node->parent, tmp_key, new_node_place);
             if(ret==-1) return -1;
             ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
             if(ret==-1) return -1;
             ret = buffer->Write(bpt_file->fd, new_node_place, 0, &new_node, sizeof(new_node));
             if(ret==-1) return -1;
          }
          now_node = cur_node->parent;
          ret = buffer->Read(bpt_file->fd, now_node, 0, &buf);
          if(ret==-1) return -1;
                   for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(cur_node, buf, i);
            break;
         }
    }
    if(now_node==root&&cur_node->key_num==m)
    {
       BPT_Node new_root;
       node_pointer new_root_place = bpt_file->offset_file;
       bpt_file->offset_file += Block_Size;
       root = new_root_place;
       cur_node->parent = new_root_place;
       new_root.child.push_back(now_node);
       new_root.key_num = 0;
       new_root.parent = -1;
       new_root.is_leaf = false;
       new_root.pre_leaf = -1;
       new_root.next_leaf = -1;
       if(cur_node->is_leaf==true)
       {
          BPT_Node new_node;
          node_pointer new_node_place = bpt_file->offset_file;
          bpt_file->offset_file += Block_Size;
          new_node.is_leaf = true;
          cur_node->key_num = floor((double)m/2.0);
          new_node.key_num = m-cur_node->key_num;
          new_node.parent = cur_node->parent;
          new_node.next_leaf = cur_node->next_leaf;
          cur_node->next_leaf = new_node_place;
          new_node.pre_leaf = now_node;
          cur_id = 0;
          while(cur_id<new_node.key_num)
          {
                new_node.key.push_front(cur_node->key.back());
                cur_node->key.pop_back();
                new_node.data.push_front(cur_node->data.back());
                cur_node->data.pop_back();
                cur_id++;
          }
          new_root.key.push_back(new_node.key.front());
          new_root.key_num++;
          new_root.child.push_back(new_node_place);
          ret = buffer->Write(bpt_file->fd, new_root_place, 0, &new_root, sizeof(new_root));
          if(ret==-1) return -1;
          ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
          if(ret==-1) return -1;
          ret = buffer->Write(bpt_file->fd, new_node_place, 0, &new_node, sizeof(new_node));
          if(ret==-1) return -1;
       }
       else 
       {
             BPT_Node new_node;
             node_pointer new_node_place = bpt_file->offset_file;
             bpt_file->offset_file += Block_Size;
             new_node.is_leaf = false;
             new_node.parent = cur_node->parent;
             cur_node->key_num = floor((double)(m-1)/2.0);
             new_node.key_num = m-cur_node->key_num-1;
             new_node.pre_leaf = -1;
             new_node.next_leaf = -1;
             cur_id = 0;
             while(cur_id<new_node.key_num)
             {
                   new_node.key.push_front(cur_node->key.back());
                   cur_node->key.pop_back();
                   new_node.child.push_front(cur_node->child.back());
                   cur_node->child.pop_back();
                   cur_id++;
                   if(cur_id==new_node.key_num)
                   {
                      new_node.child.push_front(cur_node->child.back());
                      cur_node->child.pop_back();
                   }
             }
             new_root.key.push_back(cur_node->key.back());
             cur_node->key.pop_back();
             new_root.child.push_back(new_node_place);
             new_root.key_num++;
             ret = buffer->Write(bpt_file->fd, new_root_place, 0, &new_root, sizeof(new_root));
             if(ret==-1) return -1;
             ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
             if(ret==-1) return -1;
             ret = buffer->Write(bpt_file->fd, new_node_place, 0, &new_node, sizeof(new_node));
             if(ret==-1) return -1;
       }
    }
    return 0;
}
void BPT::update(BPT_Node *fa_node, node_pointer src_node, string des_key)
{
     int cur_id = 0;
     for(auto it = fa_node->child.begin(); it != fa_node->child.end(); it++)
     {
         if((*it)==src_node) break;
         cur_id++;
     }
     int insert_id = 0;
     for(auto it = fa_node->key.begin(); it != fa_node->key.end(); it++)
     {
         if(insert_id==cur_id-1)
         {
            (*it) = des_key;
            break;
         }
         insert_id++;
     }
}
void BPT::del_key_and_child(BPT_Node *fa_node, node_pointer des_node)
{
     int cur_id = 0;
     for(auto it = fa_node->child.begin(); it != fa_node->child.end(); it++)
     {
         if((*it)==des_node)
         {
            fa_node->child.erase(it);
            break;
         }
         cur_id++;
     }
     int insert_id = 0;
     for(auto it = fa_node->key.begin(); it != fa_node->key.end(); it++)
     {
         if(insert_id==cur_id-1)
         {
            fa_node->key.erase(it);
            fa_node->key_num--;
            break;
         }
         insert_id++;
     }
}
void BPT::update_l(BPT_Node *fa_node, BPT_Node *l_brother, BPT_Node *cur_node, node_pointer l_node)
{
     string fa_new_key = l_brother->key.back();
     l_brother->key.pop_back();
     l_brother->key_num--;
     cur_node->child.push_front(l_brother->child.back());
     l_brother->child.pop_back();
     string cur_new_key;
     int cur_id = 0;
     int insert_id = 0;
     for(auto it = fa_node->child.begin(); it != fa_node->child.end(); it++)
     {
         if((*it)==l_node) break;
         cur_id++;
     }
     for(auto it = fa_node->key.begin(); it != fa_node->key.end(); it++)
     {
         if(insert_id==cur_id)
         {
            cur_new_key = (*it);
            (*it) = fa_new_key;
            break;
         }
         insert_id++;
     }
     cur_node->key.push_front(cur_new_key);
     cur_node->key_num++;
}
void BPT::update_r(BPT_Node *fa_node, BPT_Node *r_brother, BPT_Node *cur_node, node_pointer r_node)
{
     string fa_new_key = r_brother->key.front();
     r_brother->key.pop_front();
     r_brother->key_num--;
     cur_node->child.push_back(r_brother->child.front());
     r_brother->child.pop_front();
     string cur_new_key;
     int cur_id = 0;
     int insert_id = 0;
     for(auto it = fa_node->child.begin(); it != fa_node->child.end(); it++)
     {
         if((*it)==r_node) break;
         cur_id++;
     }
     for(auto it = fa_node->key.begin(); it != fa_node->key.end(); it++)
     {
         if(insert_id==cur_id-1)
         {
            cur_new_key = (*it);
            (*it) = fa_new_key;
            break;
         }
         insert_id++;
     }
     cur_node->key.push_back(cur_new_key);
     cur_node->key_num++;
}
void BPT::merge_l(BPT_Node *fa_node, BPT_Node *l_brother, BPT_Node *cur_node, node_pointer l_node)
{ 
     string cur_new_key;
     int cur_id = 0;
     for(auto it = fa_node->child.begin(); it != fa_node->child.end(); it++)
     {
         if((*it)==l_node) 
         {
            fa_node->child.erase(it);
            break;
         }
         cur_id++;
     }
     int insert_id = 0;
     for(auto it = fa_node->key.begin(); it != fa_node->key.end(); it++)
     {
         if(cur_id==insert_id)
         {
            cur_new_key = (*it);
            fa_node->key.erase(it);
            fa_node->key_num--;
            break;
         }
         insert_id++;
     }
     cur_node->key.push_front(cur_new_key);
     cur_node->key_num++;
     cur_id = 0;
     while(cur_id<l_brother->key_num)
     {
           cur_node->key.push_front(l_brother->key.back());
           l_brother->key.pop_back();
           cur_node->child.push_front(l_brother->child.back());
           l_brother->child.pop_back();
           cur_id++;
           if(cur_id==l_brother->key_num)
           {
              cur_node->child.push_front(l_brother->child.back());
              l_brother->child.pop_back();
           }
     } 
     cur_node->key_num += l_brother->key_num;
}
void BPT::merge_r(BPT_Node *fa_node, BPT_Node *r_brother, BPT_Node *cur_node, node_pointer r_node)
{
     string r_new_key;
     int cur_id = 0;
     int insert_id = 0;
     for(auto it = fa_node->child.begin(); it != fa_node->child.end(); it++)
     {
         if((*it)==r_node)
         {
            it--;
            cur_id--;
            fa_node->child.erase(it);
            break;
         }
     }
     for(auto it = fa_node->key.begin(); it != fa_node->key.end(); it++)
     {
         if(insert_id==cur_id)
         {
            r_new_key = (*it);
            fa_node->key.erase(it);
            fa_node->key_num--;
            break;
         }
         insert_id++;
     }
     r_brother->key.push_front(r_new_key);
     r_brother->key_num++;
     cur_id = 0;
     while(cur_id<cur_node->key_num)
     {
           r_brother->key.push_front(cur_node->key.back());
           cur_node->key.pop_back();
           r_brother->child.push_front(cur_node->child.back());
           cur_node->child.pop_back();
           cur_id++;
           if(cur_id==cur_node->key_num)
           {
              r_brother->child.push_front(cur_node->child.back());
              cur_node->child.pop_back();
           }
     }
     r_brother->key_num += cur_node->key_num;
}
int BPT::del(string one_key) // 已经确保one_key存在
{
    if(root==-1) 
    {
       err_info = "key not exists.";
       return -1;
    }
    int ret;
    int cur_id;
    int insert_id;
    BPT_Node *cur_node = new BPT_Node;
    char *buf;
    node_pointer now_node = root;
    while(true)
    {
         ret = buffer->Read(bpt_file->fd, now_node, 0, &buf);
         if(ret==-1) return -1;
                  for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(cur_node, buf, i);
            break;
         }
         if(cur_node->is_leaf==true) break;
         cur_id = 0;
         for(auto it = cur_node->key.begin(); it != cur_node->key.end(); it++)
         { 
             if(greater((*it), one_key)) break; 
             cur_id++;
         }
         insert_id = 0;
         for(auto it = cur_node->child.begin(); it != cur_node->child.end(); it++)
         {
             if(insert_id==cur_id) 
             {
                now_node = (*it);
                break;
             }
             insert_id++;
         }
    }   
    cur_id = 0;
    bool if_found = false;
    for(auto it = cur_node->key.begin(); it != cur_node->key.end(); it++)
    {    
        if((*it)==one_key) 
        {
           if_found = true;
           cur_node->key.erase(it);
           cur_node->key_num--;
           break;
        }
        cur_id++; 
    }
    if(if_found==false) 
    {
       err_info = "key not exists.";
       return -1;
    }
    insert_id = 0;
    for(auto it = cur_node->data.begin(); it != cur_node->data.end(); it++)
    {
        if(insert_id==cur_id)
        {
           cur_node->data.erase(it);
           break;
        }
        insert_id++;
    }
    int min_key_num = ceil((double)m/2.0)-1;
    if(cur_node->key_num>=min_key_num) return buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
    if(now_node==root)
    {
       if(cur_node->key_num==0) 
       {
          root = -1;
          return 0;
       } 
       else return buffer->Write(bpt_file->fd, root, 0, cur_node, sizeof(*cur_node));
    }
    while(now_node!=root&&cur_node->key_num<min_key_num)
    {
          if(cur_node->is_leaf==true)
          {
             BPT_Node *fa_node = new BPT_Node;
             ret = buffer->Read(bpt_file->fd, cur_node->parent, 0, &buf);
             if(ret==-1) return -1; 
                      for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(fa_node, buf, i);
            break;
         }
             auto it = fa_node->child.begin();
             for(; it != fa_node->child.end(); it++)
             if((*it)==now_node) break;
             if((*it)==fa_node->child.front())
             {
                BPT_Node *brother_node = new BPT_Node;
                ret = buffer->Read(bpt_file->fd, cur_node->next_leaf, 0, &buf);
                if(ret==-1) return -1;
                         for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(brother_node, buf, i);
            break;
         }
                if(brother_node->key_num>min_key_num)
                {
                   cur_node->key.push_back(brother_node->key.front());
                   brother_node->key.pop_front();
                   cur_node->key_num++;
                   brother_node->key_num--;
                   cur_node->data.push_back(brother_node->data.front());
                   brother_node->data.pop_front();
                   update(fa_node, cur_node->next_leaf, brother_node->key.front());
                   ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                   if(ret==-1) return -1;
                   ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
                   if(ret==-1) return -1;
                   ret = buffer->Write(bpt_file->fd, cur_node->next_leaf, 0, brother_node, sizeof(*brother_node));
                   if(ret==-1) return -1;
                   return 0;
                }
                else 
                {
                   cur_id = 0;
                   while(cur_id<min_key_num)
                   {
                         cur_node->key.push_back(brother_node->key.front());
                         brother_node->key.pop_front();
                         cur_node->data.push_back(brother_node->data.front());
                         brother_node->data.pop_front();
                         cur_id++;
                   }
                   cur_node->key_num += min_key_num;
                   cur_node->next_leaf = brother_node->next_leaf;
                   BPT_Node *to_change_node = new BPT_Node;
                   if(brother_node->next_leaf!=-1)
                   {  
                       ret = buffer->Read(bpt_file->fd, brother_node->next_leaf, 0, &buf);
                       if(ret==-1) return -1;
                                for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(to_change_node, buf, i);
            break;
         }
                       to_change_node->pre_leaf = now_node;
                   }     
                   del_key_and_child(fa_node, cur_node->next_leaf);
                   ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                   if(ret==-1) return -1;
                   ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
                   if(ret==-1) return -1;
                   if(brother_node->next_leaf!=-1)
                   {
                      ret = buffer->Write(bpt_file->fd, brother_node->next_leaf, 0, to_change_node, sizeof(*to_change_node));
                      if(ret==-1) return -1;
                   }  
                }
             }
             else if((*it)==fa_node->child.back())
             {
                  BPT_Node *brother_node = new BPT_Node;
                  ret = buffer->Read(bpt_file->fd, cur_node->pre_leaf, 0, &buf);
                  if(ret==-1) return -1;
                           for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(brother_node, buf, i);
            break;
         }
                  if(brother_node->key_num>min_key_num)
                  {
                     string src_key = cur_node->key.front();
                     cur_node->key.push_front(brother_node->key.back());
                     brother_node->key.pop_back();
                     cur_node->key_num++;
                     brother_node->key_num--;
                     cur_node->data.push_front(brother_node->data.back());
                     brother_node->data.pop_back();
                     update(fa_node, now_node, cur_node->key.front());
                     ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                     if(ret==-1) return -1;
                     ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
                     if(ret==-1) return -1;
                     ret = buffer->Write(bpt_file->fd, cur_node->pre_leaf, 0, brother_node, sizeof(*brother_node));
                     if(ret==-1) return -1;
                     return 0;
                  }
                  else 
                  {
                     cur_id = 0;
                     while(cur_id<cur_node->key_num)
                     {
                          brother_node->key.push_back(cur_node->key.front());
                          cur_node->key.pop_front();
                          brother_node->data.push_back(cur_node->data.front());
                          cur_node->data.pop_front();
                          cur_id++;
                     }
                     brother_node->key_num += cur_node->key_num;
                     brother_node->next_leaf = cur_node->next_leaf;
                     BPT_Node *to_change_node = new BPT_Node;
                     if(cur_node->next_leaf!=-1)
                     {
                        ret = buffer->Read(bpt_file->fd, cur_node->next_leaf, 0, &buf);
                        if(ret==-1) return -1;
                                 for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(to_change_node, buf, i);
            break;
         }
                        to_change_node->pre_leaf = cur_node->pre_leaf;
                     }
                     del_key_and_child(fa_node, now_node);
                     ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                     if(ret==-1) return -1;
                     ret = buffer->Write(bpt_file->fd, cur_node->pre_leaf, 0, brother_node, sizeof(*brother_node));
                     if(ret==-1) return -1;
                     if(cur_node->next_leaf!=-1)
                     {
                        ret = buffer->Write(bpt_file->fd, cur_node->next_leaf, 0, to_change_node, sizeof(*to_change_node));
                        if(ret==-1) return -1;
                     }
                  }
             }
             else 
             {
                  BPT_Node *l_brother = new BPT_Node;
                  ret = buffer->Read(bpt_file->fd, cur_node->pre_leaf, 0, &buf);
                  if(ret==-1) return -1;
                           for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(l_brother, buf, i);
            break;
         }
                  if(l_brother->key_num>min_key_num)
                  {
                     cur_node->key.push_front(l_brother->key.back());
                     l_brother->key.pop_back();
                     cur_node->data.push_front(l_brother->data.back());
                     l_brother->data.pop_back();
                     cur_node->key_num++;
                     l_brother->key_num--;
                     update(fa_node, now_node, cur_node->key.front());
                     ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                     if(ret==-1) return -1;
                     ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
                     if(ret==-1) return -1;
                     ret = buffer->Write(bpt_file->fd, cur_node->pre_leaf, 0, l_brother, sizeof(*l_brother));
                     if(ret==-1) return -1;
                     return 0;
                  }
                  BPT_Node *r_brother = new BPT_Node;
                  ret = buffer->Read(bpt_file->fd, cur_node->next_leaf, 0, &buf);
                  if(ret==-1) return -1;
                           for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(r_brother, buf, i);
            break;
         }
                  if(r_brother->key_num>min_key_num)
                  {
                     cur_node->key.push_back(r_brother->key.front());
                     r_brother->key.pop_front();
                     cur_node->data.push_back(r_brother->data.front());
                     r_brother->key.pop_front();
                     cur_node->key_num++;
                     r_brother->key_num--;
                     update(fa_node, cur_node->next_leaf, r_brother->key.front());
                     ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                     if(ret==-1) return -1;
                     ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
                     if(ret==-1) return -1;
                     ret = buffer->Write(bpt_file->fd, cur_node->next_leaf, 0, r_brother, sizeof(*r_brother));
                     if(ret==-1) return -1;
                     return 0;
                  }
                  cur_id = 0;
                  while(cur_id<min_key_num)
                  {
                       cur_node->key.push_back(r_brother->key.front());
                       r_brother->key.pop_front();
                       cur_node->data.push_back(r_brother->data.front());
                       r_brother->data.pop_front();
                  }
                  cur_node->key_num += min_key_num;
                  cur_node->next_leaf = r_brother->next_leaf;
                  BPT_Node *to_change_node = new BPT_Node;
                  if(r_brother->next_leaf!=-1)
                  {
                     ret = buffer->Read(bpt_file->fd, r_brother->next_leaf, 0, &buf);
                     if(ret==-1) return -1;
                              for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(to_change_node, buf, i);
            break;
         }
                     to_change_node->pre_leaf = now_node;
                  }
                  del_key_and_child(fa_node, cur_node->next_leaf);
                  ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                  if(ret==-1) return -1;
                  ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
                  if(ret==-1) return -1;
                  if(r_brother->next_leaf!=-1)
                  {
                     ret = buffer->Write(bpt_file->fd, r_brother->next_leaf, 0, to_change_node, sizeof(*to_change_node));
                     if(ret==-1) return -1;
                  }
             }
          }
          else
          { 
             BPT_Node *fa_node = nullptr;
             ret = buffer->Read(bpt_file->fd, cur_node->parent, 0, &buf);
             if(ret==-1) return -1;
             fa_node = new BPT_Node;
                      for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(fa_node, buf, i);
            break;
         }
             BPT_Node *l_brother = nullptr;
             BPT_Node *r_brother = nullptr;
             auto it = fa_node->child.begin();
             for(; it != fa_node->child.end(); it++)
             if((*it)==now_node) break;
             if(now_node!=fa_node->child.front())
             {
                it--;
                ret = buffer->Read(bpt_file->fd, (*it), 0, &buf);
                if(ret==-1) return -1;
                l_brother = new BPT_Node;
                         for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(l_brother, buf, i);
            break;
         }
                if(l_brother->key_num>min_key_num)
                {
                   update_l(fa_node, l_brother, cur_node, (*it));
                   ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                   if(ret==-1) return -1;
                   ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
                   if(ret==-1) return -1;
                   ret = buffer->Write(bpt_file->fd, (*it), 0, l_brother, sizeof(*l_brother));
                   if(ret==-1) return -1;
                   return 0;
                }
                it++;
             }
             if(now_node!=fa_node->child.back())
             {
                it++;
                ret = buffer->Read(bpt_file->fd, (*it), 0, &buf);
                if(ret==-1) return -1;
                r_brother = new BPT_Node;
                         for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(r_brother, buf, i);
            break;
         }
                if(r_brother->key_num>min_key_num)
                {
                   update_r(fa_node, r_brother, cur_node, (*it));
                   ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                   if(ret==-1) return -1;
                   ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
                   if(ret==-1) return -1;
                   ret = buffer->Write(bpt_file->fd, (*it), 0, r_brother, sizeof(*r_brother));
                   if(ret==-1) return -1;
                   return 0;
                }
                it--;
             }
             if(l_brother!=nullptr)
             {
                it--;
                merge_l(fa_node, l_brother, cur_node, (*it));
                ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                if(ret==-1) return -1;
                ret = buffer->Write(bpt_file->fd, now_node, 0, cur_node, sizeof(*cur_node));
                if(ret==-1) return -1;
             }
             else 
             {
                it++;
                merge_r(fa_node, r_brother, cur_node, (*it));
                ret = buffer->Write(bpt_file->fd, (*it), 0, r_brother, sizeof(*r_brother));
                if(ret==-1) return -1;
                ret = buffer->Write(bpt_file->fd, cur_node->parent, 0, fa_node, sizeof(*fa_node));
                if(ret==-1) return -1;
             }
          }  
          now_node = cur_node->parent;
          ret = buffer->Read(bpt_file->fd, now_node, 0, &buf);
                   for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(cur_node, buf, i);
            break;
         }
          if(ret==-1) return -1;
    }
    if(now_node==root&&cur_node->key_num==0)
    {
       root = cur_node->child.front();
       BPT_Node *new_root = new BPT_Node;
       ret = buffer->Read(bpt_file->fd, root, 0, &buf);
       if(ret==-1) return -1;
                for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(new_root, buf, i);
            break;
         }
       new_root->parent = -1;
       ret = buffer->Write(bpt_file->fd, root, 0, new_root, sizeof(*new_root));
       if(ret==-1) return -1;
       return 0;
    }
    return 0;
}
leaf_node BPT::find(string des_key) const
{
          if(root==-1) return -1; 
          node_pointer now_node = root;
          BPT_Node *cur_node = new BPT_Node;
          int cur_id;
          int insert_id;
          char *buf;
          int ret = buffer->Read(bpt_file->fd, root, 0, &buf);
          if(ret==-1) return -1;
         for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(cur_node, buf, i);
            break;
         }
          while(true)
          {
                if(cur_node->is_leaf==true) return now_node;
                cur_id = 0;
                for(auto it = cur_node->key.begin(); it != cur_node->key.end(); it++)
                {
                    if(greater((*it), des_key)) break;
                    cur_id++;
                }
                insert_id = 0;
                for(auto it = cur_node->child.begin(); it != cur_node->child.end(); it++)
                {
                    if(insert_id==cur_id) 
                    {
                       now_node = (*it);
                       ret = buffer->Read(bpt_file->fd, now_node, 0, &buf);
                       if(ret==-1) return -1;
         for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(cur_node, buf, i);
            break;
         }                       
                       break;
                    }
                    insert_id++;
                }
          }
}
Record * BPT::find_equal(string des_key) const
{ 
         leaf_node leaf = find(des_key);
         if(leaf==-1) return nullptr;
         BPT_Node *cur_leaf = new BPT_Node;
         char *buf;
         int ret = buffer->Read(bpt_file->fd, leaf, 0, &buf);
         if(ret==-1) return nullptr;
         for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(cur_leaf, buf, i);
            break;
         }
         int cur_id = 0;
         bool if_found = false;
         for(auto it = cur_leaf->key.begin(); it != cur_leaf->key.end(); it++)
         {
             if(equal((*it), des_key))
             {
                if_found = true;
                break;
             }
             cur_id++;
         }
         if(if_found==false) return nullptr;
         int insert_id = 0;
         for(auto it = cur_leaf->data.begin(); it != cur_leaf->data.end(); it++)
         {
             if(insert_id==cur_id)
             {
                Record *des_data = new Record;
                ret = buffer->Read(table_file->fd, (*it).offset_file, (*it).offset_block, &buf);
                if(ret==-1) return nullptr;
                for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(des_data, buf, i);
                          break;
                       }
                return des_data;
                break;
             }
             insert_id++;
         }
}
leaf_node BPT::find_leftest() const
{
          if(root==-1) return -1;
          node_pointer now_node = root;
          BPT_Node *cur_node = new BPT_Node;
          char *buf;
          int ret = buffer->Read(bpt_file->fd, root, 0, &buf);
          if(ret==-1) return -1;
          for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(cur_node, buf, i);
                          break;
                       }
          while(true)
          {
                if(cur_node->is_leaf==true) return now_node;
                now_node = cur_node->child.front();
                ret = buffer->Read(bpt_file->fd, now_node, 0, &buf);
                if(ret==-1) return -1;
                for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(cur_node, buf, i);
                          break;
                       }
          }
} 
Record_Set * BPT::find_not_equal(string des_key) const 
{
             leaf_node leftest_leaf = find_leftest();
             if(leftest_leaf==-1) return nullptr;
             leaf_node now_leaf = leftest_leaf;
             BPT_Node *cur_leaf = new BPT_Node;
             char *buf;
             int ret = buffer->Read(bpt_file->fd, leftest_leaf, 0, &buf);
             if(ret==-1) return nullptr;
             for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(cur_leaf, buf, i);
                          break;
                       }
             Record_Set *ans = new Record_Set;
             Record *tmp_record = new Record;
             while(true)
             {
                   auto data_it = cur_leaf->data.begin();
                   auto key_it = cur_leaf->key.begin();
                   for(; key_it != cur_leaf->key.end(); key_it++, data_it++)
                   {
                       if(equal((*key_it), des_key)) continue;
                       
                       ret = buffer->Read(table_file->fd, (*data_it).offset_file, (*data_it).offset_block, &buf);
                       if(ret==-1) return nullptr;
                       for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(tmp_record, buf, i);
                          break;
                       }
                       ans->record.push_back(*tmp_record);
                   }
                   now_leaf = cur_leaf->next_leaf;
                   if(now_leaf==-1) 
                   {
                      if(ans->record.size()==0) return nullptr;
                      return ans;
                   }
                   ret = buffer->Read(bpt_file->fd, now_leaf, 0, &buf);
                   if(ret==-1) return nullptr;
                   for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(cur_leaf, buf, i);
                          break;
                       }
             }
}
Record_Set * BPT::find_greater(string des_key) const
{
             leaf_node des_leaf = find(des_key);
             if(des_leaf==-1) return nullptr;
             Record_Set *ans = new Record_Set;
             leaf_node now_leaf = des_leaf;
             BPT_Node *cur_leaf = new BPT_Node;
             char *buf;
             int ret = buffer->Read(bpt_file->fd, now_leaf, 0, &buf);
             if(ret==-1) return nullptr; 
             for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(cur_leaf, buf, i);
                          break;
                       }
             Record *tmp_record = new Record;
             while(true)
             {
                   auto key_it = cur_leaf->key.begin();
                   auto data_it = cur_leaf->data.begin();
                   for(; key_it != cur_leaf->key.end(); key_it++, data_it++)
                   {
                         if(smaller_equal((*key_it), des_key)) continue; 
                        
                         ret = buffer->Read(table_file->fd, (*data_it).offset_file, (*data_it).offset_block, &buf);
                         if(ret==-1) return nullptr;
                         for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(tmp_record, buf, i);
                          break;
                       }
                         ans->record.push_back(*tmp_record);
                   }
                   now_leaf = cur_leaf->next_leaf;
                   if(now_leaf==-1)
                   {
                      if(ans->record.size()==0) return nullptr;
                      return ans;
                   }
                   ret = buffer->Read(bpt_file->fd, now_leaf, 0, &buf);
                   if(ret==-1) return nullptr;
                   for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(cur_leaf, buf, i);
                          break;
                       }
             }   
} 
Record_Set * BPT::find_smaller(string des_key) const
{
             leaf_node des_leaf = find(des_key);
             if(des_leaf==-1) return nullptr;
             Record_Set *ans = new Record_Set;
             leaf_node now_leaf = des_leaf;
             BPT_Node *cur_leaf = new BPT_Node;
             char *buf;
             int ret = buffer->Read(bpt_file->fd, now_leaf, 0, &buf);
             if(ret==-1) return nullptr;
             for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(cur_leaf, buf, i);
                          break;
                       }
                       Record *tmp_record = new Record;
             while(true)
             {
                   auto key_it = cur_leaf->key.begin();
                   auto data_it = cur_leaf->data.begin();
                   for(; key_it != cur_leaf->key.end(); key_it++, data_it++)
                   {
                         if(greater_equal((*key_it), des_key)) break;  
                         ret = buffer->Read(table_file->fd, (*data_it).offset_file, (*data_it).offset_block, &buf);
                         if(ret==-1) return nullptr;
                         for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(tmp_record, buf, i);
                          break;
                       }
                         ans->record.push_back(*tmp_record);
                   }
                   now_leaf = cur_leaf->pre_leaf;
                   if(now_leaf==-1)
                   {
                      if(ans->record.size()==0) return nullptr;
                      return ans;
                   }
                   ret = buffer->Read(bpt_file->fd, now_leaf, 0, &buf);
                   if(ret==-1) return nullptr;
                   for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(cur_leaf, buf, i);
                          break;
                       }
             }   
} 
Record_Set * BPT::find_greater_equal(string des_key) const
{
             leaf_node des_leaf = find(des_key);
             if(des_leaf==-1) return nullptr;
             Record_Set *ans = new Record_Set;
             leaf_node now_leaf = des_leaf;
             BPT_Node *cur_leaf = new BPT_Node;
             char *buf;
             int ret = buffer->Read(bpt_file->fd, now_leaf, 0, &buf);
             if(ret==-1) return nullptr;
             for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(cur_leaf, buf, i);
                          break;
                       }
                       Record *tmp_record = new Record;
             while(true)
             {
                   auto key_it = cur_leaf->key.begin();
                   auto data_it = cur_leaf->data.begin();
                   for(; key_it != cur_leaf->key.end(); key_it++, data_it++)
                   {
                         if(smaller((*key_it), des_key)) continue; 
                         
                         ret = buffer->Read(table_file->fd, (*data_it).offset_file, (*data_it).offset_block, &buf);
                         if(ret==-1) return nullptr;
                         for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(tmp_record, buf, i);
                          break;
                       }
                         ans->record.push_back(*tmp_record);
                   }
                   now_leaf = cur_leaf->next_leaf;
                   if(now_leaf==-1)
                   {
                      if(ans->record.size()==0) return nullptr;
                      return ans;
                   }
                   ret = buffer->Read(bpt_file->fd, now_leaf, 0, &buf);
                   if(ret==-1) return nullptr;
                   for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(cur_leaf, buf, i);
                          break;
                       }
             }   
} 

Record_Set * BPT::find_smaller_equal(string des_key) const
{
             leaf_node des_leaf = find(des_key);
             if(des_leaf==-1) return nullptr;
             char *buf;
             Record_Set *ans = new Record_Set;
             leaf_node now_leaf = des_leaf;
             BPT_Node *cur_leaf = new BPT_Node;
             int ret = buffer->Read(bpt_file->fd, now_leaf, 0, &buf);
             for(int i = 0; i < Block_Size; i++)
             if((*(buf+i))=='\n')
             {
                 memcpy(cur_leaf, buf, i);
                 break;
             }
             if(ret==-1) return nullptr;
             Record *tmp_record = new Record;
             while(true)
             {
                   auto key_it = cur_leaf->key.begin();
                   auto data_it = cur_leaf->data.begin();
                   for(; key_it != cur_leaf->key.end(); key_it++, data_it++)
                   {
                         if(greater((*key_it), des_key)) break;
                         ret = buffer->Read(table_file->fd, (*data_it).offset_file, (*data_it).offset_block, &buf);
                         if(ret==-1) return nullptr;
                         for(int i = 0; i < Block_Size; i++)
                         if((*(buf+i))=='\n')
                         {
                           memcpy(tmp_record, buf, i);
                           break;
                         }
                         ans->record.push_back(*tmp_record);
                   }
                   now_leaf = cur_leaf->pre_leaf;
                   if(now_leaf==-1)
                   {
                      if(ans->record.size()==0) return nullptr;
                      return ans;
                   }
                   ret = buffer->Read(bpt_file->fd, now_leaf, 0, &buf);
                   if(ret==-1) return nullptr;
                   for(int i = 0; i < Block_Size; i++)
                   if((*(buf+i))=='\n')
                   {
                      memcpy(cur_leaf, buf, i);
                      break;
                   }
             }   
} 
using namespace std;
Record_Set * BPT::find_all() const 
{
             leaf_node leftest_leaf = find_leftest();
             if(leftest_leaf==-1) return nullptr;
             char *buf;
             leaf_node now_leaf = leftest_leaf;
             BPT_Node *cur_leaf = new BPT_Node;
             int ret = buffer->Read(bpt_file->fd, leftest_leaf, 0, &buf);
             if(ret==-1) return nullptr;
             for(int i = 0; i < Block_Size; i++)
             if((*(buf+i))=='\n')
             {
                memcpy(cur_leaf, buf, i);
                break;
             }
             Record_Set *ans = new Record_Set;
             Record *tmp_record = new Record;
             while(true)
             {
                   auto data_it = cur_leaf->data.begin();
                   auto key_it = cur_leaf->key.begin();
                   for(; key_it != cur_leaf->key.end(); key_it++, data_it++)
                   {   
                       ret = buffer->Read(table_file->fd, (*data_it).offset_file, (*data_it).offset_block, &buf);
                       if(ret==-1) return nullptr; 
                       for(int i = 0; i < Block_Size; i++)
                       if((*(buf+i))=='\n')
                       {
                          memcpy(tmp_record, buf, i);
                          break;
                       }
                       ans->record.push_back(*tmp_record);
                   }
                   now_leaf = cur_leaf->next_leaf;
                   if(now_leaf==-1) return ans; 
                   ret = buffer->Read(bpt_file->fd, now_leaf, 0, &buf);
                   if(ret==-1) return nullptr; 
                   for(int i = 0; i < Block_Size; i++)
                   if((*(buf+i))=='\n')
                   {
                       memcpy(cur_leaf, buf, i);
                       break;
                   }
             }
}

bool BPT::check_if_exist(string des_key) const
{
     Record *record = find_equal(des_key);
     if(record==nullptr) return false;
     return true;
}

All_Data * BPT::find_all_data() const
{  
              leaf_node leftest_leaf = find_leftest();
              if(leftest_leaf==-1) return nullptr;
              All_Data *all_data = new All_Data;
              leaf_node cur_leaf = leftest_leaf;
              BPT_Node *cur_node = new BPT_Node;
              char *buf;
              while(true)
              {
                    buffer->Read(bpt_file->fd, cur_leaf, 0, &buf);
                    for(int i = 0; i < Block_Size; i++)
                    if((*(buf+i))=='\n') 
                    {
                       memcpy(cur_node, buf, i);
                       break;
                    }
                    for(auto it = cur_node->data.begin(); it != cur_node->data.end(); it++)
                    all_data->data.push_back(*it);
                    cur_leaf = cur_node->next_leaf;
                    if(cur_leaf==-1) break;
              }
              return all_data;
}
