#include "Catalog_Manager.h"
#include "Interpreter.h"
#include "Buffer_Manager.h"
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
using namespace std;
extern Buffer *buffer;
extern Data_Convert *data_convert;
extern Catalog_File *catalog_file;
Table_Info_File::Table_Info_File()
{
      fd = open("./DB_Files/Table_Info", O_RDWR|O_CREAT, 0777);
      offset_t cur_offset = lseek(fd, 0, SEEK_END);
      if(cur_offset==0) offset_file = 0;
      else offset_file = cur_offset+Block_Size-(cur_offset%Block_Size);
}
Attr_Info_File::Attr_Info_File()
{
      fd = open("./DB_Files/Attr_Info", O_RDWR|O_CREAT, 0777);
      offset_t cur_offset = lseek(fd, 0, SEEK_END);
      if(cur_offset==0) offset_file = 0;
      else offset_file = cur_offset+Block_Size-(cur_offset%Block_Size);
}
Index_Info_File::Index_Info_File()
{
      fd = open("./DB_Files/Index_Info", O_RDWR|O_CREAT, 0777);
      offset_t cur_offset = lseek(fd, 0, SEEK_END);
      if(cur_offset==0) offset_file = 0;
      else offset_file = cur_offset+Block_Size-(cur_offset%Block_Size);
}
bool Catalog_File::check_table_if_exist(std::string table_name) 
{
     offset_t offset_file = 0;
     Table_Info *cur_table_info;
     int ret;
     char *buf;
     while(offset_file<table_info.offset_file)
     {      
           
           buffer->Read(table_info.fd, offset_file, 0, &buf);
           size_t len;
           for(int i = 0; i < Block_Size; i++)
           if((*(buf+i))=='\n')
           {
               len = i;
               break;
           }
           cur_table_info = data_convert->parse_table_info(buf, len);
           offset_file += Block_Size;
           if(cur_table_info->if_del==true) continue;
           if((cur_table_info->table_name)==table_name) return true; 
     }
     return false;
}
bool Catalog_File::check_index_if_exist(std::string index_name) 
{
     offset_t offset_file = 0;
     Index_Info *cur_index_info;
     char *buf;
     while(offset_file<index_info.offset_file)
     {     
           buffer->Read(index_info.fd, offset_file, 0, &buf);
           size_t len;
           for(int i = 0; i < Block_Size; i++)
           if((*(buf+i))=='\n')
           {
             len = i;
             break;
           }
           cur_index_info = data_convert->parse_index_info(buf, len);
           offset_file += Block_Size;
           if(cur_index_info->if_del==true) continue;
           if(cur_index_info->index_name==index_name) return true;
     }
     return false;
}
bool Catalog_File::check_attr_if_exist(std::string table_name, std::string attr_name) 
{
     offset_t offset_file = 0;
     int ret;
     Attr_Info *cur_attr_info;
     char *buf;
     while(offset_file<attr_info.offset_file)
     {         
           buffer->Read(attr_info.fd, offset_file, 0, &buf);
           size_t len;
           for(int i = 0; i < Block_Size; i++)
           if((*(buf+i))=='\n')
           {
             (cur_attr_info, buf, i);
             break;
           }
           cur_attr_info = data_convert->parse_attr_info(buf, len);
           offset_file += Block_Size;
           if(cur_attr_info->if_del==true) continue;
           if(cur_attr_info->table_name!=table_name) continue;
           for(auto it = cur_attr_info->attr.begin(); it != cur_attr_info->attr.end(); it++)
           if((*it).attr_name==attr_name) return true;
           return false;
     }
     return false;
}
primary_key Catalog_File::find_primary_key(std::string table_name) 
{
            offset_t offset_file = 0;
            Table_Info *cur_table_info;
            char *buf;
            while(offset_file<table_info.offset_file)
            {    
                  buffer->Read(table_info.fd, offset_file, 0, &buf);
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_table_info = data_convert->parse_table_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_table_info->if_del==true) continue;
                  if(cur_table_info->table_name!=table_name) continue;
                  return cur_table_info->primary_key;
            }
}
bool Catalog_File::check_table_attr_if_exist(std::string table_name, std::string attr_name) 
{
     primary_key key_name = find_primary_key(table_name);
     if(key_name==attr_name) return true;
     offset_t offset_file = 0;
     Index_Info *cur_index_info ;
     int ret;
     char *buf;
     while(offset_file<index_info.offset_file)
     { 
           buffer->Read(index_info.fd, offset_file, 0, &buf);
           size_t len;
           for(int i = 0; i < Block_Size; i++)
           if((*(buf+i))=='\n')
           {  
              len = i;
              break;
           }
           cur_index_info = data_convert->parse_index_info(buf, len);
           offset_file += Block_Size;
           if(cur_index_info->if_del==true) continue;
           if(cur_index_info->table_name!=table_name) continue;
           if(cur_index_info->attr_name==attr_name) return true;
     }
     return false;
}
void Catalog_File::insert_table_info(Table_Info *src_table_info)
{
     size_t len;
     char *buf = data_convert->reverse_parse_table_info(src_table_info, &len);
     buffer->Write(table_info.fd, table_info.offset_file, 0, buf, len);
     delete [] buf;
     table_info.offset_file += Block_Size;
}
void Catalog_File::insert_attr_info(Attr_Info *src_attr_info)
{
     size_t len;
     char *buf = data_convert->reverse_parse_attr_info(src_attr_info, &len);
     buffer->Write(attr_info.fd, attr_info.offset_file, 0, buf, len);
     delete [] buf;
     attr_info.offset_file += Block_Size;
}
void Catalog_File::insert_index_info(Index_Info *src_index_info)
{
     size_t len;
     char *buf = data_convert->reverse_parse_index_info(src_index_info, &len);
     buffer->Write(index_info.fd, index_info.offset_file, 0, buf, len);
     delete [] buf;
     index_info.offset_file += Block_Size;
}
bool Catalog_File::check_attr_if_unique(std::string table_name, std::string attr_name) 
{
     offset_t offset_file = 0;
     primary_key key_name = find_primary_key(table_name);
     if(attr_name==key_name) return true;
     Attr_Info *cur_attr_info ;
     char *buf;
     while(offset_file<attr_info.offset_file)
     {       
           buffer->Read(attr_info.fd, offset_file, 0, &buf);
           size_t len;
           for(int i = 0; i < Block_Size; i++)
           if((*(buf+i))=='\n')
           {
              len = i;
              break;
           }
           cur_attr_info = data_convert->parse_attr_info(buf, len);
           offset_file += Block_Size;
           if(cur_attr_info==nullptr) continue;
           if(cur_attr_info->if_del==true) continue;
           if(cur_attr_info->table_name!=table_name) continue;
           for(auto it = cur_attr_info->attr.begin(); it != cur_attr_info->attr.end(); it++)
           {
               if((*it).attr_name!=attr_name) continue;
               if((*it).if_unique==true) return true;
               return false; 
           }
           return false;
     }
     return false;
}
size_t Catalog_File::get_attr_size(std::string table_name, std::string attr_name) 
{
            offset_t offset_file = 0;
            Attr_Info *cur_attr_info ;
            char *buf;
            while(offset_file<attr_info.offset_file)
            {        
                  buffer->Read(attr_info.fd, offset_file, 0, &buf);
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                     len = i;
                     break;
                  }
                  cur_attr_info = data_convert->parse_attr_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_attr_info->if_del==true) continue;
                  if(cur_attr_info->table_name!=table_name) continue;
                  for(auto it = cur_attr_info->attr.begin(); it != cur_attr_info->attr.end(); it++)
                  {
                        if((*it).attr_name!=attr_name) continue;
                        return (*it).data_size;
                  }
            }
}
data_type Catalog_File::get_data_type(std::string table_name, std::string attr_name) 
{
            offset_t offset_file = 0;
            Attr_Info *cur_attr_info ;
            char *buf;
            while(offset_file<attr_info.offset_file)
            {      
                  buffer->Read(attr_info.fd, offset_file, 0, &buf);
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_attr_info = data_convert->parse_attr_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_attr_info->if_del==true) continue;
                  if(cur_attr_info->table_name!=table_name) continue;
                  for(auto it = cur_attr_info->attr.begin(); it != cur_attr_info->attr.end(); it++)
                  {
                        if((*it).attr_name!=attr_name) continue;
                        return (*it).type;
                  }
            }
}
std::string Catalog_File::get_table_name(std::string index_name) 
{ 
            offset_t offset_file = 0;
            Index_Info *cur_index_info ;
            char *buf;
            while(offset_file<index_info.offset_file)
            {   
                  buffer->Read(index_info.fd, offset_file, 0, &buf);
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_index_info = data_convert->parse_index_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_index_info->if_del==true) continue;
                  if(cur_index_info->index_name==index_name) return cur_index_info->table_name;
            }
} 
std::string Catalog_File::get_attr_name(std::string index_name) 
{
            offset_t offset_file = 0;
            Index_Info *cur_index_info ;
            char *buf;
            while(offset_file<index_info.offset_file)
            { 
                  buffer->Read(index_info.fd, offset_file, 0, &buf);
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_index_info = data_convert->parse_index_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_index_info->if_del==true) continue;
                  if(cur_index_info->index_name==index_name) return cur_index_info->attr_name;
            }
}
Attr_Set * Catalog_File::get_all_index_attr(std::string table_name) 
{
           primary_key key_name = find_primary_key(table_name);
           Attr_Set * attr_set = new Attr_Set;
           attr_set->attr_name.push_back(key_name);
           offset_t offset_file = 0;
           Index_Info *cur_index_info ;
           char *buf;
           while(offset_file<index_info.offset_file)
           { 
                  buffer->Read(index_info.fd, offset_file, 0, &buf);
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_index_info = data_convert->parse_index_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_index_info->if_del==true) continue;
                  if(cur_index_info->table_name!=table_name) continue;
                  attr_set->attr_name.push_back(cur_index_info->attr_name);      
           }
           return attr_set;
}
All_Attr * Catalog_File::get_all_attr(std::string table_name) 
{
            All_Attr *all_attr = new All_Attr;
            offset_t offset_file = 0; 
            Attr_Info *cur_attr_info ;
            char *buf;
            while(offset_file<attr_info.offset_file)
            { 
                  buffer->Read(attr_info.fd, offset_file, 0, &buf);
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_attr_info = data_convert->parse_attr_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_attr_info->if_del==true) continue;
                  if(cur_attr_info->table_name!=table_name) continue;
                  for(auto it = cur_attr_info->attr.begin(); it != cur_attr_info->attr.end(); it++)
                  all_attr->attr_info.push_back(*it);
            }
            return all_attr;
}
int Catalog_File::find_attr_pos(std::string table_name, std::string attr_name) 
{
            offset_t offset_file = 0;
            Attr_Info *cur_attr_info ;
            char *buf;
            while(offset_file<attr_info.offset_file)
            { 
                  buffer->Read(attr_info.fd, offset_file, 0, &buf);
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_attr_info = data_convert->parse_attr_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_attr_info->if_del==true) continue;
                  if(cur_attr_info->table_name!=table_name) continue;
                  int size = cur_attr_info->attr.size();
                  for(int i = 0; i < size; i++)
                  if(cur_attr_info->attr[i].attr_name==attr_name) return i;
            }
}
void Catalog_File::drop_table(std::string table_name)
{
            
            offset_t offset_file = 0; 
            Table_Info *cur_table_info ;
            int ret;
            char *buf;
            while(offset_file<table_info.offset_file)
            {       
                  buffer->Read(table_info.fd, offset_file, 0, &buf);
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_table_info = data_convert->parse_table_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_table_info==nullptr) continue;
                  if(cur_table_info->if_del==true) continue;
                  if(cur_table_info->table_name!=table_name) continue;
                  cur_table_info->if_del = true;
                  buf = data_convert->reverse_parse_table_info(cur_table_info, &len);
                  buffer->Write(table_info.fd, offset_file-Block_Size, 0, buf, len);
                  delete [] buf;
            }
}
void Catalog_File::drop_attr(std::string table_name)
{
            offset_t offset_file = 0;
            Attr_Info *cur_attr_info ;
            int ret;
            char *buf;
            while(offset_file<attr_info.offset_file)
            {       
                  buffer->Read(attr_info.fd, offset_file, 0, &buf);
                  if(ret==-1) break;
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                     len = i;
                     break;
                  }
                  cur_attr_info = data_convert->parse_attr_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_attr_info->if_del==true) continue;
                  if(cur_attr_info->table_name!=table_name) continue;
                  cur_attr_info->if_del = true;
                  buf = data_convert->reverse_parse_attr_info(cur_attr_info, &len);
                  buffer->Write(attr_info.fd, offset_file-Block_Size, 0, buf,  len);
                  delete [] buf;
            }
}
void Catalog_File::drop_index(std::string index_name)
{
           offset_t offset_file = 0;
           Index_Info *cur_index_info ;
           int ret;
           char *buf;
           while(offset_file<index_info.offset_file)
           {     
                  buffer->Read(index_info.fd, offset_file, 0, &buf);
                  if(ret==-1) break;
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_index_info = data_convert->parse_index_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_index_info->if_del==true) continue;
                  if(cur_index_info->index_name!=index_name) continue;
                  cur_index_info->if_del = true;
                  buf = data_convert->reverse_parse_index_info(cur_index_info, &len);
                  buffer->Write(index_info.fd, offset_file-Block_Size, 0, buf, len);  
                  delete [] buf; 
           }
}
int Catalog_File::get_attr_num(std::string table_name) 
{
            offset_t offset_file = 0; 
            Table_Info *cur_table_info ;
            int ret;
            char *buf;
            while(offset_file<table_info.offset_file)
            {       
                  buffer->Read(table_info.fd, offset_file, 0, &buf);
                  if(ret==-1) break;
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_table_info = data_convert->parse_table_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_table_info->if_del==true) continue;
                  if(cur_table_info->table_name!=table_name) continue;
                  return cur_table_info->attr_num;
            }
}
All_Type * Catalog_File::get_all_type(std::string table_name) 
{
            offset_t offset_file = 0;
            All_Type *all_type = new All_Type;
            Attr_Info *cur_attr_info ;
            int ret;
            char *buf;
            while(offset_file<attr_info.offset_file)
            {       
                  buffer->Read(attr_info.fd, offset_file, 0, &buf);
                  if(ret==-1) break;
                  size_t len;
                  for(int i = 0; i < Block_Size; i++)
                  if((*(buf+i))=='\n')
                  {
                        len = i;
                        break;
                  }
                  cur_attr_info = data_convert->parse_attr_info(buf, len);
                  offset_file += Block_Size;
                  if(cur_attr_info->if_del==true) continue;
                  if(cur_attr_info->table_name!=table_name) continue;
                  for(auto it = cur_attr_info->attr.begin(); it != cur_attr_info->attr.end(); it++)
                  all_type->type.push_back((*it).type);
                  return all_type;
            }
}