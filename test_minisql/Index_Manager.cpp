#include "Index_Manager.h"
#include <sstream>
#include <string>
extern BPT_Info_File *bpt_info_file;
extern Catalog_File *catalog_file;
BPT_Info * Index_Manager::find_primary_bpt_info(std::string table_name, primary_key key_name)
{
           offset_t offset_file = 0;
           BPT_Info *primary_bpt_info = new BPT_Info;
           char *buf;
           while(offset_file<bpt_info_file->offset_file)
           {
                 int ret = buffer->Read(bpt_info_file->fd, offset_file, 0, &buf);
                 if(ret==-1) break;
                 for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(primary_bpt_info, buf, i);
            break;
         }
                 offset_file += Block_Size;
                 if(primary_bpt_info->is_del==true) continue;
                 if(primary_bpt_info->table_name!=table_name) continue;
                 if(primary_bpt_info->attr_name!=key_name) continue;
                 return primary_bpt_info;
           }
           return primary_bpt_info;
}
using namespace std;
void Index_Manager::create_index(std::string index_name, std::string table_name, std::string attr_name)
{ 
     size_t key_size = catalog_file->get_attr_size(table_name, attr_name);
     data_type type = catalog_file->get_data_type(table_name, attr_name);
     BPT_Info *bpt_info = new BPT_Info;
     bpt_info->table_name = table_name;
     bpt_info->attr_name = attr_name;
     bpt_info->is_del = false;
     bpt_info->BPT = bpt_file->offset_file;
     bpt_file->offset_file += Block_Size;
     primary_key key_name = catalog_file->find_primary_key(table_name);
     BPT_Info *primary_bpt_info = find_primary_bpt_info(table_name, key_name);
     All_Data *all_data;
     data_type key_type = catalog_file->get_data_type(table_name, key_name);
     BPT *primary_bpt_int = new BPT((size_t)4, (BPT_Pointer)0, (data_type)0);
     BPT *primary_bpt_string = new BPT((size_t)4, (BPT_Pointer)0, (data_type)0);
     BPT *primary_bpt_float = new BPT((size_t)4, (BPT_Pointer)0, (data_type)0);
     char *buf;
     switch(key_type)
     {
           case Int:
           buffer->Read(bpt_file->fd, primary_bpt_info->BPT, 0, &buf);
           for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(primary_bpt_int, buf, i);
            break;
         }
           all_data = primary_bpt_int->find_all_data();
           break;
           case Char:
           buffer->Read(bpt_file->fd, primary_bpt_info->BPT, 0, &buf);
           for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(primary_bpt_string, buf, i);
            break;
         }
           all_data = primary_bpt_string->find_all_data();
           break;
           case Float:
           buffer->Read(bpt_file->fd, primary_bpt_info->BPT, 0, &buf);
           for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(primary_bpt_float, buf, i);
            break;
         }
           all_data = primary_bpt_float->find_all_data();
           break;
     }
     Record *record = new Record;
     int pos = catalog_file->find_attr_pos(table_name, attr_name);
     int size = all_data->data.size();
     BPT *new_bpt_int = new BPT(key_size, bpt_info->BPT, type);
     BPT *new_bpt_string = new BPT(key_size, bpt_info->BPT, type);
     BPT *new_bpt_float = new BPT(key_size, bpt_info->BPT, type);
     switch(type)
     {
           case Int:
           for(int i = 0; i < size; i++)
           {
               buffer->Read(table_file->fd, all_data->data[i].offset_file, all_data->data[i].offset_block, &buf);
               for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(record, buf, i);
            break;
         }
               new_bpt_int->insert(record->attr[pos], all_data->data[i]);
           }
           new_bpt_int->write_bpt_to_file();
           break;
           case Char:
           for(int i = 0; i < size; i++)
           {
               buffer->Read(table_file->fd, all_data->data[i].offset_file, all_data->data[i].offset_block, &buf);
               for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(record, buf, i);
            break;
         }
               new_bpt_string->insert(record->attr[pos], all_data->data[i]);
           }
           new_bpt_string->write_bpt_to_file();
           break;
           case Float:
           for(int i = 0; i < size; i++)
           {
               buffer->Read(table_file->fd, all_data->data[i].offset_file, all_data->data[i].offset_block, &buf);
               for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(record, buf, i);
            break;
         }
               new_bpt_float->insert(record->attr[pos], all_data->data[i]);
           }
           new_bpt_float->write_bpt_to_file();
           break;
     }
     buffer->Write(bpt_info_file->fd, bpt_info_file->offset_file, 0, bpt_info, sizeof(*bpt_info));
     bpt_info_file->offset_file += Block_Size;
}
void Index_Manager::delete_index(std::string index_name)
{
     std::string table_name = catalog_file->get_table_name(index_name);
     std::string attr_name = catalog_file->get_attr_name(index_name);
     offset_t offset_file = 0;
     BPT_Info *bpt_info = new BPT_Info;
     char *buf;
     while(offset_file<bpt_info_file->offset_file)
     { 
           int ret = buffer->Read(bpt_info_file->fd, offset_file, 0, &buf);
           if(ret==0) break;
           for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            memcpy(bpt_info, buf, i);
            break;
         }
           offset_file += Block_Size;
           if(bpt_info->is_del==true) continue;
           if(bpt_info->table_name!=table_name) continue;
           if(bpt_info->attr_name!=attr_name) continue;
           bpt_info->is_del = true;
           buffer->Write(bpt_info_file->fd, offset_file-Block_Size, 0, bpt_info, sizeof(*bpt_info));
           break;
     }
}
void Index_Manager::create_key_index(std::string table_name, std::string attr_name)
{
     size_t key_size = catalog_file->get_attr_size(table_name, attr_name);
     data_type type = catalog_file->get_data_type(table_name, attr_name);
     BPT_Info *bpt_info = new BPT_Info;
     bpt_info->table_name = table_name;
     bpt_info->attr_name = attr_name;
     bpt_info->is_del = false;
     bpt_info->BPT = bpt_file->offset_file;
     bpt_file->offset_file += Block_Size;
     BPT new_bpt(key_size, bpt_info->BPT, type);
     new_bpt.write_bpt_to_file();
     buffer->Write(bpt_info_file->fd, bpt_info_file->offset_file, 0, bpt_info, sizeof(*bpt_info));
     bpt_info_file->offset_file += Block_Size;
}