#include "Index_Manager.h"
#include <sstream>
#include <string>
extern BPT_Info_File *bpt_info_file;
extern Catalog_File *catalog_file;
extern Data_Convert *data_convert;
BPT_Info * Index_Manager::find_primary_bpt_info(std::string table_name, primary_key key_name)
{
           offset_t offset_file = 0;
           BPT_Info *primary_bpt_info = nullptr;
           char *buf;
           while(offset_file<bpt_info_file->offset_file)
           {
                 buffer->Read(bpt_info_file->fd, offset_file, 0, &buf);
                 size_t len;
                 for(int i = 0; i < Block_Size; i++)
                 if((*(buf+i))=='\n')
                 {
                     len = i;
                     break;
                 }
                 primary_bpt_info = data_convert->parse_bpt_info(buf, len);
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
     All_Data *all_data = nullptr;
     BPT *primary_bpt = nullptr;
     char *buf = nullptr;
     size_t len;
     buffer->Read(bpt_file->fd, primary_bpt_info->BPT, 0, &buf);
     for(int i = 0; i < Block_Size; i++)
     if((*(buf+i))=='\n')
     {
         len = i;
         break;
     }
     primary_bpt = data_convert->parse_bpt(buf, len);
     all_data = primary_bpt->find_all_data();
     Record *record = nullptr;
     int pos = catalog_file->find_attr_pos(table_name, attr_name);
     int size = all_data->data.size();
     BPT *new_bpt = new BPT(key_size, bpt_info->BPT, type);
     for(int i = 0; i < size; i++)
     {
         buffer->Read(table_file->fd, all_data->data[i].offset_file, all_data->data[i].offset_block, &buf);
         for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
             len = i;
             break;
         }
         record = data_convert->parse_record(buf, len);
         new_bpt->insert(record->attr[pos], all_data->data[i]);
     }
     new_bpt->write_bpt_to_file();
     buf = data_convert->reverse_parse_bpt_info(bpt_info, &len);
     buffer->Write(bpt_info_file->fd, bpt_info_file->offset_file, 0, buf, len);
     if(buf!=nullptr) { delete [] buf; buf = nullptr; }
     if(new_bpt!=nullptr) { delete new_bpt; new_bpt = nullptr; }
     if(record!=nullptr) { delete record; record = nullptr; }
     if(primary_bpt!=nullptr) { delete primary_bpt; primary_bpt = nullptr; }
     if(primary_bpt_info!=nullptr) { delete primary_bpt_info; primary_bpt_info = nullptr; }
     if(all_data!=nullptr) { delete all_data; all_data = nullptr; }
     if(bpt_info!=nullptr) { delete bpt_info; bpt_info = nullptr; }
     bpt_info_file->offset_file += Block_Size;
}
void Index_Manager::delete_index(std::string index_name)
{
     std::string table_name = catalog_file->get_table_name(index_name);
     std::string attr_name = catalog_file->get_attr_name(index_name);
     offset_t offset_file = 0;
     BPT_Info *bpt_info = nullptr;
     char *buf = nullptr;
     size_t len;
     while(offset_file<bpt_info_file->offset_file)
     { 
           buffer->Read(bpt_info_file->fd, offset_file, 0, &buf);
           for(int i = 0; i < Block_Size; i++)
           if((*(buf+i))=='\n')
           {
              len = i;
              break;
           }
           if(bpt_info!=nullptr) { delete bpt_info; bpt_info = nullptr; }
           bpt_info = data_convert->parse_bpt_info(buf, len);
           offset_file += Block_Size;
           if(bpt_info->is_del==true) continue;
           if(bpt_info->table_name!=table_name) continue;
           if(bpt_info->attr_name!=attr_name) continue;
           bpt_info->is_del = true;
           buf = data_convert->reverse_parse_bpt_info(bpt_info, &len);
           buffer->Write(bpt_info_file->fd, offset_file-Block_Size, 0, buf, len);
           if(buf!=nullptr) { delete [] buf; buf = nullptr; }
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
     size_t len;
     char *buf = data_convert->reverse_parse_bpt_info(bpt_info, &len);
     buffer->Write(bpt_info_file->fd, bpt_info_file->offset_file, 0, buf, len);
     if(buf!=nullptr) { delete [] buf; buf = nullptr; }
     if(bpt_info!=nullptr) { delete bpt_info; bpt_info = nullptr; }
     bpt_info_file->offset_file += Block_Size;
}