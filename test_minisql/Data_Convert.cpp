#include "Data_Convert.h"
void Data_Convert::parse_into_string(vector<string> & all_str, char *parse_start_place, size_t len)
{                 
                  int pos = 0;
                  string tmp;
                  while(true)
                  {  
                       while(pos<len&&parse_start_place[pos]==' ') pos++;
                       if(pos==len) break;
                       while(pos<len&&parse_start_place[pos]!=' ') tmp += parse_start_place[pos++];
                       all_str.push_back(tmp);
                       tmp.clear();
                  }
}
BPT_Node * Data_Convert::parse_bpt_node(char *parse_start_place, size_t len)
{
           vector<string> all_str;
           parse_into_string(all_str, parse_start_place, len);
           BPT_Node *new_node = new BPT_Node;
           stringstream *ss;
           ss = new stringstream;
           (*ss) << all_str[0];
           (*ss) >> new_node->key_num;
           delete ss;
           ss = new stringstream;
           (*ss) << all_str[1];
           (*ss) >> new_node->is_leaf;
           delete ss;
           ss = new stringstream;
           (*ss) << all_str[2];
           (*ss) >> new_node->parent;
           delete ss;
           ss = new stringstream;
           (*ss) << all_str[3];
           (*ss) >> new_node->pre_leaf;
           delete ss;
           ss = new stringstream;
           (*ss) << all_str[4];
           (*ss) >> new_node->next_leaf;
           if(new_node->is_leaf==false)
           {
              for(int i = 1; i <= new_node->key_num; i++)
              new_node->key.push_back(all_str[4+i]);
              for(int i = 1; i <= new_node->key_num+1; i++)
              {
                  delete ss;
                  ss = new stringstream;
                  (*ss) << all_str[4+new_node->key_num+i];
                  node_pointer cur_child;
                  (*ss) >> cur_child;
                  new_node->child.push_back(cur_child);
              }
           }
           else 
           {
              for(int i = 1; i <= new_node->key_num; i++)
              new_node->key.push_back(all_str[4+i]);
              int cur_pos = 4+new_node->key_num+1;
              for(int i = 1; i <= new_node->key_num; i++)
              {
                  delete ss;
                  ss = new stringstream;
                  (*ss) << all_str[cur_pos];
                  offset_t offset_file;
                  (*ss) >> offset_file;
                  delete ss;
                  ss = new stringstream;
                  cur_pos++;
                  (*ss) << all_str[cur_pos];
                  offset_t offset_block;
                  (*ss) >> offset_block;
                  cur_pos++;
                  data_pointer data_pos;
                  data_pos.offset_file = offset_file;
                  data_pos.offset_block = offset_block;
                  new_node->data.push_back(data_pos);
              }
           }
           return new_node;
}
BPT * Data_Convert::parse_bpt(char *parse_start_place, size_t len)
{
      vector<string> all_str;
      parse_into_string(all_str, parse_start_place, len);
      BPT *new_bpt;
      node_pointer root;
      Max_Son_Node_Num m;
      BPT_Pointer bpt;
      type_t type;
      stringstream *ss;
      ss = new stringstream;
      (*ss) << all_str[0];
      (*ss) >> root;
      delete ss;
      ss = new stringstream;
      (*ss) << all_str[1];
      (*ss) >> m;
      delete ss;
      ss = new stringstream;
      (*ss) << all_str[2];
      (*ss) >> type;
      data_type real_type;
      switch(type)
      {
           case 0:
           real_type = Int;
           break;
           case 1:
           real_type = Char;
           case 2:
           real_type = Float;
      }
      delete ss;
      ss = new stringstream;
      (*ss) << all_str[3];
      (*ss) >> bpt;
      new_bpt = new BPT(root, m, real_type, bpt);
      return new_bpt;
}
Record * Data_Convert::parse_record(char *parse_start_place, size_t len)
{
         Record * record = new Record;
         parse_into_string(record->attr, parse_start_place, len);
         return record;
}
Table_Info * Data_Convert::parse_table_info(char *parse_start_place, size_t len)
{
             vector<string> all_str;
             parse_into_string(all_str, parse_start_place, len);
             Table_Info *table_info = new Table_Info;
             stringstream *ss;
             ss = new stringstream;
             (*ss) << all_str[0];
             (*ss) >> table_info->if_del;
             table_info->table_name = all_str[1];
             delete ss;
             ss = new stringstream;
             (*ss) << all_str[2];
             (*ss) >> table_info->attr_num;
             table_info->primary_key = all_str[3];
             return table_info;
}
Attr_Info * Data_Convert::parse_attr_info(char *parse_start_place, size_t len)
{
            vector<string> all_str;
            parse_into_string(all_str, parse_start_place, len);
            Attr_Info *attr_info = new Attr_Info;
            stringstream *ss;
            ss = new stringstream;
            (*ss) << all_str[0];
            (*ss) >> attr_info->if_del;
            attr_info->table_name = all_str[1];
            int cur_pos = 2;
            int all_str_size = all_str.size();
            while(cur_pos<all_str_size)
            {
                 one_attr_info tmp_info;
                 tmp_info.attr_name = all_str[cur_pos++];
                 delete ss;
                 ss = new stringstream;
                 int type;
                 data_type real_type;
                 (*ss) << all_str[cur_pos++];
                 (*ss) >> type;
                 switch(type)
                 {
                       case 0:
                       real_type = Int;
                       break;
                       case 1:
                       real_type = Char;
                       break;
                       case 2:
                       real_type = Float;
                       break;
                 }
                 tmp_info.type = real_type;
                 delete ss;
                 ss = new stringstream;
                 (*ss) << all_str[cur_pos++];
                 (*ss) >> tmp_info.data_size;
                 delete ss;
                 ss = new stringstream;
                 (*ss) << all_str[cur_pos++];
                 (*ss) >> tmp_info.if_unique;
                 attr_info->attr.push_back(tmp_info); 
            }
            return attr_info;
}            
Index_Info * Data_Convert::parse_index_info(char *parse_start_place, size_t len)
{
             Index_Info *index_info = new Index_Info;
             vector<string> all_str;
             parse_into_string(all_str, parse_start_place, len);
             stringstream *ss;
             ss = new stringstream;
             (*ss) << all_str[0];
             (*ss) >> index_info->if_del;
             index_info->index_name = all_str[1];
             index_info->table_name = all_str[2];
             index_info->attr_name = all_str[3];
             delete ss;
             return index_info;
}
BPT_Info * Data_Convert::parse_bpt_info(char *parse_start_place, size_t len)
{
           vector<string> all_str;
           parse_into_string(all_str, parse_start_place, len);
           BPT_Info *bpt_info = new BPT_Info;
           stringstream *ss;
           ss = new stringstream;
           (*ss) << all_str[0];
           (*ss) >> bpt_info->is_del;
           bpt_info->table_name = all_str[1];
           bpt_info->attr_name = all_str[2];
           delete ss;
           ss = new stringstream;
           (*ss) << all_str[3];
           (*ss) >> bpt_info->BPT;
           return bpt_info;
} 
void Data_Convert::to_char(char *st_pos, string *str)
{
     size_t len = (*str).size();
     for(int i = 0; i < len; i++)
     (*(st_pos+i)) = (char)(*str)[i];
}
char * Data_Convert::reverse_parse_bpt_node(BPT_Node *bpt_node, size_t *len)
{
       char *buf = new char[Block_Size];
       int cnt_byte = 0;
       string *str;
       stringstream *ss;
       ss = new stringstream;
       (*ss) << bpt_node->key_num;
       str = new string;
       (*ss) >> (*str); 
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       delete str;
       delete ss;
       str = new string;
       ss = new stringstream;
       (*ss) << bpt_node->is_leaf;
       (*ss) >> (*str);
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       delete str;
       delete ss;
       str = new string;
       ss = new stringstream;
       (*ss) << bpt_node->parent;
       (*ss) >> (*str);
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       delete str;
       delete ss;
       str = new string;
       ss = new stringstream;
       (*ss) << bpt_node->pre_leaf;
       (*ss) >> (*str);
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       delete str;
       delete ss;
       str = new string;
       ss = new stringstream;
       (*ss) << bpt_node->next_leaf;
       (*ss) >> (*str);
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       for(auto it = bpt_node->key.begin(); it != bpt_node->key.end(); it++)
       {
           to_char(buf+cnt_byte, (string *)(&(*it)));
           cnt_byte += (*it).size();
           (*(buf+cnt_byte)) = ' ';
           cnt_byte++;
       }
       if(bpt_node->is_leaf==false)
       {
          for(auto it = bpt_node->child.begin(); it != bpt_node->child.end(); it++)
          {
             delete str;
             delete ss;
             str = new string;
             ss = new stringstream;
             (*ss) << (*it);
             (*ss) >> (*str);
             to_char(buf+cnt_byte, str);
             cnt_byte += (*str).size();
             (*(buf+cnt_byte)) = ' ';
             cnt_byte++;
          }
          (*(buf+cnt_byte-1)) = '\n';  
       }
       else 
       {
          for(auto it = bpt_node->data.begin(); it != bpt_node->data.end(); it++)
          {
              delete str;
              delete ss;
              str = new string;
              ss = new stringstream;
              (*ss) << (*it).offset_file;
              (*ss) >> (*str);
              to_char(buf+cnt_byte, str);
              cnt_byte += (*str).size();
              (*(buf+cnt_byte)) = ' ';
              cnt_byte++;
              delete str;
              delete ss;
              str = new string;
              ss = new stringstream;
              (*ss) << (*it).offset_block;
              (*ss) >> (*str);
              to_char(buf+cnt_byte, str);
              cnt_byte += (*str).size();
              (*(buf+cnt_byte)) = ' ';
              cnt_byte++;
          }
          (*(buf+cnt_byte-1)) = '\n';
       }
       (*len) = cnt_byte;
       return buf;
}
char * Data_Convert::reverse_parse_bpt(BPT *bpt, size_t *len)
{
       char *buf = new char[Block_Size/40];
       int cnt_byte = 0;
       string *str;
       stringstream *ss;
       str = new string;
       ss = new stringstream;
       (*ss) << bpt->root;
       (*ss) >> (*str);
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       delete str;
       delete ss;
       str = new string;
       ss = new stringstream;
       (*ss) << bpt->m;
       (*ss) >> (*str);
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       delete str;
       str = new string;
       switch(bpt->type)
       {
             case Int:
             (*str) = "0";
             break;
             case Char:
             (*str) = "1";
             break;
             case Float:
             (*str) = "2";
             break;
       }
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       delete str;
       delete ss;
       str = new string;
       ss = new stringstream;
       (*ss) << bpt->bpt;
       (*ss) >> (*str);
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = '\n';
       cnt_byte++;
       (*len) = cnt_byte;
       return buf;
}
char * Data_Convert::reverse_parse_record(Record *record, size_t *len)
{
       char *buf = new char[Block_Size];
       int cnt_byte = 0;
       for(auto it = record->attr.begin(); it != record->attr.end(); it++)
       {
           to_char(buf+cnt_byte, (string *)(&(*it)));
           cnt_byte += (*it).size();
           (*(buf+cnt_byte)) = ' ';
           cnt_byte++;
       }
       (*(buf+cnt_byte-1)) = '\n';
       (*len) = cnt_byte;
       return buf;
}
char * Data_Convert::reverse_parse_table_info(Table_Info *table_info, size_t *len)
{
       char *buf = new char[Block_Size/20];
       int cnt_byte = 0;
       string *str;
       stringstream *ss;
       str = new string;
       ss = new stringstream;
       (*ss) << table_info->if_del;
       (*ss) >> (*str);
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       to_char(buf+cnt_byte, &table_info->table_name);
       cnt_byte += table_info->table_name.size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       delete str;
       delete ss;
       ss = new stringstream;
       str = new string;
       (*ss) << table_info->attr_num;
       (*ss) >> (*str);
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       to_char(buf+cnt_byte, &table_info->primary_key);
       cnt_byte += table_info->primary_key.size();
       (*(buf+cnt_byte)) = '\n';
       cnt_byte++;
       (*len) = cnt_byte;
       return buf;  
}
char * Data_Convert::reverse_parse_attr_info(Attr_Info *attr_info, size_t *len)
{
       char *buf = new char[Block_Size];
       int cnt_byte = 0;
       string *str;
       stringstream *ss;
       delete str;
       delete ss;
       ss = new stringstream;
       str = new string;
       (*ss) << attr_info->if_del;
       (*ss) >> (*str);
       to_char(buf+cnt_byte, str);
       cnt_byte += (*str).size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       to_char(buf+cnt_byte, &attr_info->table_name);
       cnt_byte += attr_info->table_name.size();
       (*(buf+cnt_byte)) = ' ';
       cnt_byte++;
       for(auto it = attr_info->attr.begin(); it != attr_info->attr.end(); it++)
       {
                to_char(buf+cnt_byte, (string *)(&(*it).attr_name));
                cnt_byte += (*it).attr_name.size();
                (*(buf+cnt_byte)) = ' ';
                cnt_byte++;
                delete str;                
                str = new string;
                switch((*it).type)
                {
                     case Int:
                     (*str) = "0";
                     break;
                     case Char:
                     (*str) = "1";
                     break;
                     case Float:
                     (*str) = "2";
                     break;

                }
                to_char(buf+cnt_byte, str);
                cnt_byte += (*str).size();
                (*(buf+cnt_byte)) = ' ';
                cnt_byte++;
                delete ss;
                delete str;
                ss = new stringstream;
                str = new string;
                (*ss) << (*it).data_size;
                (*ss) >> (*str);
                to_char(buf+cnt_byte, str);
                cnt_byte += (*str).size();
                (*(buf+cnt_byte)) = ' ';
                cnt_byte++;
                delete ss;
                delete str;
                ss = new stringstream;
                str = new string;
                (*ss) << (*it).if_unique;
                (*ss) >> (*str);
                to_char(buf+cnt_byte, str);
                cnt_byte += (*str).size();
                (*(buf+cnt_byte)) = ' ';
                cnt_byte++;
       }
       (*(buf+cnt_byte-1)) = '\n';
       (*len) = cnt_byte;
       return buf;
}    
char * Data_Convert::reverse_parse_index_info(Index_Info *index_info, size_t *len)
{
                char *buf = new char[Block_Size/20];
                int cnt_byte = 0;
                stringstream *ss;
                string *str;
                ss = new stringstream;
                str = new string;
                (*ss) << index_info->if_del;
                (*ss) >> (*str);
                to_char(buf+cnt_byte, str);
                cnt_byte += (*str).size();
                (*(buf+cnt_byte)) = ' ';
                cnt_byte++;
                delete ss;
                delete str;
                to_char(buf+cnt_byte, &index_info->index_name);
                cnt_byte += index_info->index_name.size();
                (*(buf+cnt_byte)) = ' ';
                cnt_byte++;              
                to_char(buf+cnt_byte, &index_info->table_name);
                cnt_byte += index_info->table_name.size();
                (*(buf+cnt_byte)) = ' ';
                cnt_byte++;
                to_char(buf+cnt_byte, &index_info->attr_name);
                cnt_byte += index_info->attr_name.size();
                (*(buf+cnt_byte)) = '\n';
                cnt_byte++;
                (*len) = cnt_byte;
                return buf;
}    
char * Data_Convert::reverse_parse_bpt_info(BPT_Info *bpt_info, size_t *len)
{
                char *buf = new char[Block_Size/20];
                int cnt_byte = 0;
                stringstream *ss;
                string *str;  
                ss = new stringstream;
                str = new string;
                (*ss) << bpt_info->is_del;
                (*ss) >> (*str);
                to_char(buf+cnt_byte, str);
                cnt_byte += (*str).size();
                (*(buf+cnt_byte)) = ' ';
                cnt_byte++;         
                to_char(buf+cnt_byte, &bpt_info->table_name);
                cnt_byte += bpt_info->table_name.size();
                (*(buf+cnt_byte)) = ' ';
                cnt_byte++;     
                to_char(buf+cnt_byte, &bpt_info->attr_name);
                cnt_byte += bpt_info->attr_name.size();
                (*(buf+cnt_byte)) = ' ';
                cnt_byte++;    
                delete ss;
                delete str;       
                ss = new stringstream;
                str = new string;
                (*ss) << bpt_info->BPT;
                (*ss) >> (*str);
                to_char(buf+cnt_byte, str);
                cnt_byte += (*str).size();
                (*(buf+cnt_byte)) = '\n';
                cnt_byte++;    
                (*len) = cnt_byte;
                return buf;
}
