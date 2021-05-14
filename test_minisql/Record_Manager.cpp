#include "Record_Manager.h"
#include "API.h"
#include <sstream>
extern Data_Convert *data_convert;
void Record_Manager::delete_one_record(std::string table_name, Record *des_record)
{
     Attr_Set *attr_set = catalog_file->get_all_index_attr(table_name);
     All_Attr *all_attr = catalog_file->get_all_attr(table_name);
     Attr_Num attr_num = des_record->attr.size();  
     BPT_Info *bpt_info;
     offset_t offset_file;  
     char *buf;
     BPT *bpt;
     for(int i = 0; i < attr_num; i++)
     {
         bool flag = false;
         for(int j = 0; j < attr_set->attr_name.size(); j++)
         if(attr_set->attr_name[j]==all_attr->attr_info[i].attr_name) 
         {
            flag = true;
            break;
         }
         if(flag==false) continue;
         offset_file = 0;
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
               bpt_info = data_convert->parse_bpt_info(buf, len);
               offset_file += Block_Size;
               if(bpt_info->is_del==true) continue;
               if(bpt_info->table_name!=table_name) continue;
               if(bpt_info->attr_name!=all_attr->attr_info[i].attr_name) continue;
               break;
         }
         buffer->Read(bpt_file->fd, bpt_info->BPT, 0, &buf);
         for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            len = i;
            break;
         }
         bpt = data_convert->parse_bpt(buf, len);
         bpt->del(des_record->attr[i]);
         bpt->write_bpt_to_file();
     } 
}
int Record_Manager::delete_all_record(std::string table_name)
{
     Record_Set *all_record = select_all_record(table_name);
     for(auto it = all_record->record.begin(); it != all_record->record.end(); it++)
     delete_one_record(table_name, (Record *)(&(*it)));
     return all_record->record.size();
}
int Record_Manager::delete_part_record(std::string table_name, std::vector<Predicate> &condition)
{
     Record_Set *all_record = select_part_record(table_name, condition);
     for(auto it = all_record->record.begin(); it != all_record->record.end(); it++)
     delete_one_record(table_name, (Record *)(&(*it)));
     return all_record->record.size();
}
using namespace std;
Record_Set * Record_Manager::select_all_record(std::string table_name)
{
             offset_t offset_file = 0;
             BPT_Info *bpt_info;
             char *buf;
             size_t len;
             BPT *bpt = nullptr;
             while(offset_file<bpt_info_file->offset_file)
             {
                   if(bpt_info!=nullptr) delete bpt;
                   buffer->Read(bpt_info_file->fd, offset_file, 0, &buf);
                   for(int i = 0; i < Block_Size; i++)
                   if((*(buf+i))=='\n')
                   {
                      len = i;
                      break;
                   }
                   bpt_info = data_convert->parse_bpt_info(buf, len);
                   offset_file += Block_Size;
                   if(bpt_info->is_del==true) continue;
                   if(bpt_info->table_name!=table_name) continue; 
                   buffer->Read(bpt_file->fd, bpt_info->BPT, 0, &buf);
                   for(int i = 0; i < Block_Size; i++)
                   if((*(buf+i))=='\n')
                   {
                      len = i;
                      break;
                   }
                   bpt = data_convert->parse_bpt(buf, len);
                   { Record_Set *record_set = bpt->find_all(); if(bpt!=nullptr) delete bpt; return record_set;} 
             }
             
}
bool Record_Manager::check_if_meet_condition(std::string key1, std::string key2, std::string op, data_type type)
{
     std::stringstream ss1, ss2;
     switch(type)
     {
           case Int:
           int int_key1;
           int int_key2;
           ss1 << key1;
           ss1 >> int_key1;
           ss2 << key2;
           ss2 >> int_key2;
           if(op=="=") return int_key1==int_key2;
           if(op=="<>") return int_key1!=int_key2;
           if(op=="<") return int_key1<int_key2;
           if(op=="<=") return int_key1<=int_key2;
           if(op==">") return int_key1>int_key2;
           if(op==">=") return int_key1>=int_key2;
           case Char:
           if(op=="=") return  key1==key2;
           if(op=="<>") return key1!=key2;
           case Float:
           float float_key1;
           float float_key2;
           ss1 << key1;
           ss1 >> float_key1;
           ss2 << key2;
           ss2 >> float_key2;
           if(op=="=") return float_key1==float_key2;
           if(op=="<>") return float_key1!=float_key2;
           if(op=="<") return float_key1<float_key2;
           if(op=="<=") return float_key1<=float_key2;
           if(op==">") return float_key1>float_key2;
           if(op==">=") return float_key1>=float_key2;
     }
}
bool Record_Manager::check(std::string table_name, std::vector<Predicate> &condition, Record *record, All_Attr *all_attr)
{
     std::string attr_name;
     std::string op;
     std::string val;
     int size = all_attr->attr_info.size();
     for(auto it = condition.begin(); it != condition.end(); it++)
     {
         attr_name = (*it).attr_name;
         op = (*it).op;
         val = (*it).val;
         for(int i = 0; i < size; i++)
         if(all_attr->attr_info[i].attr_name==attr_name)
         {
           if(check_if_meet_condition(record->attr[i], val, op, all_attr->attr_info[i].type)==false) return false;
           else break;
         }
     }
     return true;
}
using namespace std;
Record_Set * Record_Manager::select_part_record(std::string table_name, std::vector<Predicate> &condition)
{ 
             Attr_Set *attr_set = catalog_file->get_all_index_attr(table_name);
             Predicate *des_condition = nullptr;
             bool is_index;
             for(auto it = condition.begin(); it != condition.end(); it++)
             {
                 is_index = false;
                 for(auto tmp_it = attr_set->attr_name.begin(); tmp_it != attr_set->attr_name.end(); tmp_it++)
                 if((*tmp_it)==(*it).attr_name) { is_index = true; break; }
                 if(is_index==false) continue;
                 if(des_condition==nullptr) { des_condition = (Predicate *)(&(*it)); continue; }
                 if(des_condition->op=="=") break;
                 if((*it).op=="=") { des_condition = (Predicate *)(&(*it)); break; }
                 if((*it).op=="<>") continue;
                 if(des_condition->op!="<>") continue;
                 des_condition = (Predicate *)(&(*it)); 
             }
             if(des_condition==nullptr)
             {
                Record_Set *all_record;
                offset_t offset_file = 0;
                BPT_Info *bpt_info;
                char *buf;
                size_t len;
                BPT *bpt;
                while(offset_file<bpt_info_file->offset_file)
                {
                      buffer->Read(bpt_info_file->fd, offset_file, 0, &buf);
                      for(int i = 0; i < Block_Size; i++)
                      if((*(buf+i))=='\n')
                      {
                         len = i;
                         break;
                      }
                      bpt_info = data_convert->parse_bpt_info(buf, len);
                      offset_file += Block_Size;
                      if(bpt_info->is_del==true) continue;
                      if(bpt_info->table_name!=table_name) continue;
                      break;       
                }
                buffer->Read(bpt_file->fd, bpt_info->BPT, 0, &buf);
                for(int i = 0; i < Block_Size; i++)
                if((*(buf+i))=='\n')
                {
                   len = i;
                   break;
                }
                bpt = data_convert->parse_bpt(buf, len);
                all_record = bpt->find_all();
                if(all_record==nullptr) return nullptr;
                Record_Set *ans = new Record_Set;
                All_Attr *all_attr = catalog_file->get_all_attr(table_name);
                for(auto it = all_record->record.begin(); it != all_record->record.end(); it++)
                if(check(table_name, condition, (Record *)(&(*it)), all_attr)==true) ans->record.push_back(*it);
                if(ans->record.size()==0) return nullptr;
                return ans;
             }
             else
             {
                char *buf;
                std::string attr_name = des_condition->attr_name;
                std::string op = des_condition->op;
                std::string val = des_condition->val;
                offset_t offset_file = 0;
                BPT_Info *bpt_info = nullptr;
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
                      bpt_info = data_convert->parse_bpt_info(buf, len);
                      offset_file += Block_Size;
                      if(bpt_info->is_del==true) continue;
                      if(bpt_info->table_name!=table_name) continue;
                      if(bpt_info->attr_name!=attr_name) continue;
                      break;
                }
                if(bpt_info==nullptr) return nullptr;
                data_type type = catalog_file->get_data_type(table_name, attr_name);
                Record_Set *maybe_ans;
                string int_val;
                string float_val;
                BPT *float_bpt;
                BPT *int_bpt;
                BPT *string_bpt;
                switch(type)
                {
                      case Int:
                      buffer->Read(bpt_file->fd, bpt_info->BPT, 0, &buf);
                      for(int i = 0; i < Block_Size; i++)
                      if((*(buf+i))=='\n')
                      {
                         len = i;
                         break;
                      }
                      int_bpt = data_convert->parse_bpt(buf, len);
                      int_val = val;
                      if(op=="=") { Record *record = int_bpt->find_equal(int_val);  maybe_ans = new Record_Set; if(record!=nullptr) maybe_ans->record.push_back(*record); }
                      else if(op=="<>") maybe_ans = int_bpt->find_not_equal(int_val);
                      else if(op=="<") maybe_ans = int_bpt->find_smaller(int_val);
                      else if(op=="<=") maybe_ans = int_bpt->find_smaller_equal(int_val);
                      else if(op==">") maybe_ans = int_bpt->find_greater(int_val);
                      else if(op==">=") maybe_ans = int_bpt->find_greater_equal(int_val);
                      break;
                      case Char:
                      buffer->Read(bpt_file->fd, bpt_info->BPT, 0, &buf);
                      for(int i = 0; i < Block_Size; i++)
                      if((*(buf+i))=='\n')
                      {
                        len = i;
                        break;
                      }
                      string_bpt = data_convert->parse_bpt(buf, len);
                      if(op=="=") { Record *record = string_bpt->find_equal(val); maybe_ans = new Record_Set; if(record!=nullptr) maybe_ans->record.push_back(*record); }
                      else if(op=="<>") maybe_ans = string_bpt->find_not_equal(val);
                      break;
                      case Float:
                      buffer->Read(bpt_file->fd, bpt_info->BPT, 0, &buf);
                      for(int i = 0; i < Block_Size; i++)
                      if((*(buf+i))=='\n')
                      {
                         len = i;
                         break;
                      }
                      float_bpt = data_convert->parse_bpt(buf, len);
                      float_val = val;
                      if(op=="=") { Record *record = float_bpt->find_equal(float_val); maybe_ans = new Record_Set; if(record!=nullptr) maybe_ans->record.push_back(*record); }
                      else if(op=="<>") maybe_ans = float_bpt->find_not_equal(float_val);
                      else if(op=="<") maybe_ans = float_bpt->find_smaller(float_val);
                      else if(op=="<=") maybe_ans = float_bpt->find_smaller_equal(float_val);
                      else if(op==">") maybe_ans = float_bpt->find_greater(float_val);
                      else if(op==">=") maybe_ans = float_bpt->find_greater_equal(float_val);
                      break;
                }
                Record_Set *ans = new Record_Set;
                All_Attr *all_attr = catalog_file->get_all_attr(table_name);
                for(auto it = maybe_ans->record.begin(); it != maybe_ans->record.end(); it++)
                if(check(table_name, condition, (Record *)(&(*it)), all_attr)==true) ans->record.push_back(*it);
                if(ans->record.size()==0) return nullptr;
                return ans;
             } 
}
void Record_Manager::insert_one_record(std::string table_name, Record *des_record)
{
     Attr_Set *attr_set = catalog_file->get_all_index_attr(table_name);
     All_Attr *all_attr = catalog_file->get_all_attr(table_name);
     Attr_Num attr_num = des_record->attr.size();  
     size_t can_use_size = Block_Size-(table_file->cur_pos%Block_Size);
     size_t len;
     char *buf = data_convert->reverse_parse_record(des_record, &len);
     if(can_use_size<len) table_file->update(); 
     offset_t off_block = table_file->cur_pos%Block_Size;
     offset_t off_file = table_file->cur_pos-off_block;
     buffer->Write(table_file->fd, off_file, off_block, buf, len);
     if(buf!=nullptr) delete [] buf;
     table_file->cur_pos += len;
     data_pointer data_place;
     data_place.offset_block = off_block;
     data_place.offset_file = off_file;
     BPT_Info *bpt_info = nullptr;
     offset_t offset_file;  
     BPT *bpt = nullptr;
     for(int i = 0; i < attr_num; i++)
     {
         bool flag = false;
         for(int j = 0; j < attr_set->attr_name.size(); j++)
         if(attr_set->attr_name[j]==all_attr->attr_info[i].attr_name) 
         {
            flag = true;
            break;
         }
         if(flag==false) continue;
         offset_file = 0;
         while(offset_file<bpt_info_file->offset_file)
         {
               if(bpt_info!=nullptr) delete bpt_info;
               buffer->Read(bpt_info_file->fd, offset_file, 0, &buf);
               offset_file += Block_Size;
               for(int i = 0; i < Block_Size; i++)
               if((*(buf+i))=='\n')
               {
                  len = i;
                  break;
               }
               bpt_info = data_convert->parse_bpt_info(buf, len);
               if(bpt_info->is_del==true) continue;
               if(bpt_info->table_name!=table_name) continue;
               if(bpt_info->attr_name!=all_attr->attr_info[i].attr_name) continue;
               break;
         } 
         buffer->Read(bpt_file->fd, bpt_info->BPT, 0, &buf);
         for(int i = 0; i < Block_Size; i++)
         if((*(buf+i))=='\n')
         {
            len = i;
            break;
         }
         bpt = data_convert->parse_bpt(buf, len);
         bpt->insert(des_record->attr[i], data_place);
         bpt->write_bpt_to_file();
         if(bpt!=nullptr) delete bpt;
     } 
}