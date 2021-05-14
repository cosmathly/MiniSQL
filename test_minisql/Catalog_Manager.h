#ifndef _Catalog_
#define _Catalog_

#include <list>
#include <unistd.h>
#include <bits/stdc++.h>
#include <vector>
#include "Buffer_Manager.h"
typedef enum Data_Type { Int, Char, Float } data_type;
typedef std::string primary_key;
struct Data_Place
{
       offset_t offset_file; // 数据在文件当中所属于的块
       offset_t offset_block; // 数据在块中的起始位置
};
class Table_Info
{
      public:
         bool if_del; // 记录该表是否已经被删除
         std::string table_name;
         int attr_num;
         std::string primary_key;
         Table_Info() { if_del = false; }
         ~Table_Info() { }
};
typedef struct One_Attr_Info
{
        std::string attr_name;
        data_type type;
        size_t data_size;
        bool if_unique;    
} one_attr_info;
class All_Attr
{
      public:
         std::vector<one_attr_info> attr_info;
         All_Attr() { }
         ~All_Attr() { }
};
class Attr_Info
{
      public:
         bool if_del; //记录该表是否已经被删除
         std::string table_name;
         std::vector<one_attr_info> attr;
         Attr_Info() { if_del = false; }
         ~Attr_Info() { } 
};
class Attr_Set
{
      public:
         std::vector<std::string> attr_name;
         Attr_Set() { }
         ~Attr_Set() { }
};
class Index_Info
{
      public:
         bool if_del; // 记录该索引是否已经被删除
         std::string index_name;
         std::string table_name;
         std::string attr_name;
         Index_Info() { if_del = false; }
         ~Index_Info() { }
};
typedef offset_t cur_offset_t;
class Table_Info_File
{
      public:
         File_Descriptor fd;
         offset_t offset_file;
         Table_Info_File();
         ~Table_Info_File() { close(fd); }
};
class Attr_Info_File
{
      public:
         File_Descriptor fd;
         offset_t offset_file;
         Attr_Info_File();
         ~Attr_Info_File() { close(fd); }
};
class Index_Info_File
{
      public:
         File_Descriptor fd;
         offset_t offset_file;
         Index_Info_File();
         ~Index_Info_File() { close(fd); }
};
class All_Type
{   
      public:
         std::vector<data_type> type;
         All_Type() { }
         ~All_Type() { }
}; 
class Catalog_File
{
      public:
         Table_Info_File table_info;
         Attr_Info_File attr_info;
         Index_Info_File index_info;
         Catalog_File() : table_info(), attr_info(), index_info() { }
         ~Catalog_File() { }
         void drop_table(std::string table_name);
         void drop_attr(std::string table_name);
         void drop_index(std::string index_name);
         void insert_table_info( Table_Info *src_table_info);
         void insert_attr_info( Attr_Info *src_attr_info);
         void insert_index_info( Index_Info *src_index_info);
         primary_key find_primary_key(std::string table_name);
         bool check_table_if_exist(std::string table_name);
         bool check_index_if_exist(std::string index_name);
         bool check_attr_if_exist(std::string table_name, std::string attr_name) ;
         bool check_table_attr_if_exist(std::string table_name, std::string attr_name) ;
         bool check_attr_if_unique(std::string table_name, std::string attr_name) ;
         int get_attr_num(std::string table_name) ;
         size_t get_attr_size(std::string table_name, std::string attr_name) ;
         data_type get_data_type(std::string table_name, std::string attr_name) ;
         std::string get_table_name(std::string index_name) ;
         std::string get_attr_name(std::string index_name) ;
         Attr_Set * get_all_index_attr(std::string table_name) ;
         All_Attr * get_all_attr(std::string table_name) ;
         int find_attr_pos(std::string table_name, std::string attr_name) ;
         All_Type * get_all_type(std::string table_name) ;
};  

#endif 