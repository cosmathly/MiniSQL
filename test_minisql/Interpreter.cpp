#include "Interpreter.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
using namespace std;
Index_Info *to_insert_index_info;
char c;
char cmd[cmd_max_size];
int cnt_char = 0;
char *err_info; // 记录全局的错误信息
Buffer *buffer;
Catalog_File *catalog_file;
BPT_Info_File *bpt_info_file;
Table_File *table_file;
BPT_File *bpt_file;
read_mode _read_mode_;
delete_type delete_option;
std::string *to_delete_record_table_name;
char *read_file_name;
File_Descriptor read_fd;
select_type select_option;
Record_Manager *record_manager;
std::string *to_select_table_name;
std::vector<Predicate> *condition;
std::string *to_insert_table_name;
Record *to_insert_record;
Table_Info *table_info;
Index_Info *index_info;
Attr_Info *attr_info;
std::string *to_drop_table_name;
Index_Manager *index_manager;
std::string *to_create_index_name, *to_create_index_table_name, *to_create_index_attr_name;
std::string *to_delete_index_name;
void initial()
{
     buffer = new Buffer(64);
     catalog_file = new Catalog_File;
     bpt_info_file = new BPT_Info_File;
     table_file = new Table_File;
     bpt_file = new BPT_File;
     index_manager = new Index_Manager;
     record_manager = new Record_Manager;
     _read_mode_ = keyboard;
}
using std::cout;
using std::endl;
void update_err_info() { err_info = (char *)"system call failed."; }
cmd_type parse_create_table(std::vector<std::string> &all_str)
{
         int size = all_str.size();
         if(size<8) { err_info = "command not integral."; return Error; }
         if(all_str[3]!="(") { err_info = "command format error."; return Error; }
         if(all_str[size-1]!=")") { err_info = "command format error."; return Error; }
         int cnt_attr = 0;
         int cur_id = 4;
         table_info = new Table_Info;
         table_info->if_del = false;
         table_info->table_name = all_str[2];
         attr_info = new Attr_Info;
         attr_info->table_name = all_str[2];
         attr_info->if_del = false;
         one_attr_info one_attr;
         while(true)
         {
              if(cur_id>=size) break;
                 if(all_str[cur_id]=="primary")
                 {
                    if(cur_id+1>=size) { err_info = "command not integral."; return Error; }
                    if(all_str[cur_id+1]!="key") { err_info = "command format error."; return Error; }
                    if(cur_id+2>=size) { err_info = "command not integral."; return Error; }
                    if(all_str[cur_id+2]!="(") { err_info = "command format error."; return Error; }
                    if(cur_id+4>=size) { err_info = "command not integral."; return Error; }
                    if(all_str[cur_id+4]!=")") { err_info = "command format error."; return Error; }
                    bool flag = false;
                    for(auto it = attr_info->attr.begin(); it != attr_info->attr.end(); it++)
                    if((*it).attr_name==all_str[cur_id+3]) { flag = true; break; }
                    if(flag==false) { err_info = "primary key not exist."; return Error; }
                    table_info->primary_key = all_str[cur_id+3];
                    table_info->attr_num = cnt_attr;
                    break;
                 }
                 if(cur_id+1>=size) { err_info = "command not integral."; return Error; }
                 if(all_str[cur_id+1]=="int") 
                 {
                    if(cur_id+2<size&&all_str[cur_id+2]=="unique")
                    {
                       cnt_attr++;
                       one_attr.attr_name = all_str[cur_id];
                       one_attr.data_size = sizeof(int);
                       one_attr.if_unique = true;
                       one_attr.type = Int;
                       attr_info->attr.push_back(one_attr);
                       cur_id += 3;
                       continue;
                    }
                    else 
                    {
                       cnt_attr++;
                       one_attr.attr_name = all_str[cur_id];
                       one_attr.data_size = sizeof(int);
                       one_attr.if_unique = false;
                       one_attr.type = Int;
                       attr_info->attr.push_back(one_attr);
                       cur_id += 2;
                       continue;
                    }
                 }
                 else if(all_str[cur_id+1]=="float")
                 { 
                    if(cur_id+2<size&&all_str[cur_id+2]=="unique")
                    {
                       cnt_attr++;
                       one_attr.attr_name = all_str[cur_id];
                       one_attr.data_size = sizeof(float);
                       one_attr.if_unique = true;
                       one_attr.type = Float;
                       attr_info->attr.push_back(one_attr);
                       cur_id += 3;
                       continue;
                    }
                    else 
                    {
                       cnt_attr++;
                       one_attr.attr_name = all_str[cur_id];
                       one_attr.data_size = sizeof(float);
                       one_attr.if_unique = false;
                       one_attr.type = Float;
                       attr_info->attr.push_back(one_attr);
                       cur_id += 2;
                       continue;
                    }
                 }
                 else if(all_str[cur_id+1].size()>=7)
                 {
                      if(all_str[cur_id+1][0]!='c') { err_info = "command format error."; return Error; }
                      if(all_str[cur_id+1][1]!='h') { err_info = "command format error."; return Error; }
                      if(all_str[cur_id+1][2]!='a') { err_info = "command format error."; return Error; }
                      if(all_str[cur_id+1][3]!='r') { err_info = "command format error."; return Error; }
                      if(all_str[cur_id+1][4]!='(') { err_info = "command format error."; return Error; }
                      int cur_str_size = all_str[cur_id+1].size();
                      if(all_str[cur_id+1][cur_str_size-1]!=')') { err_info = "command format error."; return Error; }
                      for(int i = 5; i < cur_str_size-1; i++)
                      if(all_str[cur_id+1][i]<='0'||all_str[cur_id+1][i]>='9') { err_info = "command format error."; return Error; }
                      int data_size = 0;
                      for(int i = 5; i < cur_str_size-1; i++)
                      data_size = (data_size*10+(int)(all_str[cur_id+1][i]-'0'));
                      if(data_size<0||data_size>255) { err_info = "data size error."; return Error; }
                         if(cur_id+2<size&&all_str[cur_id+2]=="unique")
                         {
                              cnt_attr++;
                              one_attr.attr_name = all_str[cur_id];
                              one_attr.data_size = data_size;
                              one_attr.if_unique = true;
                              one_attr.type = Char;
                              attr_info->attr.push_back(one_attr);
                              cur_id += 3;
                              continue;
                         }
                         else 
                         {
                              cnt_attr++;
                              one_attr.attr_name = all_str[cur_id];
                              one_attr.data_size = data_size;
                              one_attr.if_unique = false;
                              one_attr.type = Char;
                              attr_info->attr.push_back(one_attr);
                              cur_id += 2;
                              continue;
                         }
                 }
                 else { err_info = "command not integral."; return Error; }
         }
         if(cnt_attr==0) { err_info = "command not integral."; return Error; }
         if(catalog_file->check_table_if_exist(all_str[2])) { err_info = "table exists."; return Error; }
         return Create_Table;
}
cmd_type parse_create_index(std::vector<std::string> &all_str)
{
         to_create_index_name = new std::string; 
         to_create_index_table_name = new std::string;
         to_create_index_attr_name = new std::string;
         (*to_create_index_name) = all_str[2];
         (*to_create_index_table_name) = all_str[4];
         (*to_create_index_attr_name) = all_str[6];
         if(catalog_file->check_table_if_exist(all_str[4])==false) { err_info = "table not exists."; return Error; }
         if(catalog_file->check_index_if_exist(all_str[2])==true) { err_info = "index exists."; return Error; }
         if(catalog_file->check_attr_if_exist(all_str[4], all_str[6])==false) {  err_info = "index exists."; return Error; }
         if(catalog_file->check_table_attr_if_exist(all_str[4], all_str[6])==true) { err_info = "index about this atrribute on the table exists."; return Error; }
         to_insert_index_info = new Index_Info;
         (*to_insert_index_info).if_del = false;
         (*to_insert_index_info).index_name = all_str[2];
         (*to_insert_index_info).table_name = all_str[4];
         (*to_insert_index_info).attr_name = all_str[6];
         return Create_Index;
}
cmd_type parse_create(std::vector<std::string> &all_str)
{
         if(all_str.size()==1) { err_info = "command not integral."; return Error; }
         if(all_str[1]=="table") return parse_create_table(all_str);
         if(all_str[1]=="index") return parse_create_index(all_str);
         err_info = "command not exist.";
         return Error;
}
cmd_type parse_drop_table(std::vector<std::string> &all_str)
{
         to_drop_table_name = new std::string;
         (*to_drop_table_name) = all_str[2];
         if(catalog_file->check_table_if_exist(all_str[2])==false) { err_info = "table not exists."; return Error; }
         return Drop_Table;
}
cmd_type parse_drop_index(std::vector<std::string> &all_str)
{
         to_delete_index_name = new std::string;
         (*to_delete_index_name) = all_str[2];
         if(catalog_file->check_index_if_exist(all_str[2])==false) { err_info = "index not exists."; return Error; }
         return Drop_Index;
}
cmd_type parse_drop(std::vector<std::string> &all_str)
{
         if(all_str.size()<3) { err_info = "command not integral."; return Error; }
         if(all_str.size()>3) { err_info = "syntax error."; return Error; }
         if(all_str[1]=="table") return parse_drop_table(all_str);
         if(all_str[1]=="index") return parse_drop_index(all_str);
         err_info = "command not exist."; 
         return Error; 
}

cmd_type parse_select(std::vector<std::string> &all_str)
{
         if(all_str.size()<4) { err_info = "command not integral."; return Error; }
         if(all_str[1]!="*") { err_info = "syntax error."; return Error; }
         if(all_str[2]!="from") { err_info = "syntax error."; return Error; }
         to_select_table_name = new std::string;
         (*to_select_table_name) = all_str[3];
         if(catalog_file->check_table_if_exist(all_str[3])==false) { err_info = "table not exists."; return Error; }
         if(all_str.size()==4)
         {
            select_option = select_all;
            return Select;
         }
         select_option = select_part;
         if(all_str[4]!="where") { err_info = "syntax error."; return Error; }
         if(all_str.size()<8) { err_info = "command not integral."; return Error; }
         if((all_str.size()-4)%4!=0) { err_info = "syntax error."; return Error; }
         int cur_id = 5;
         int size = all_str.size();
         condition = new std::vector<Predicate>;
         Predicate cond;
         while(true)
         {
               if(cur_id>=size) break;
               cond.attr_name = all_str[cur_id];
               cond.op = all_str[cur_id+1];
               cond.val = all_str[cur_id+2];
               if(cur_id+3<size&&all_str[cur_id+3]!="and") { err_info = "syntax error."; return Error; }
               if(catalog_file->check_attr_if_exist(all_str[3], all_str[cur_id])==false) { err_info = "attribute not exists."; return Error; }
               if(cond.op=="="||cond.op=="<>"||cond.op=="<"||cond.op=="<="||cond.op==">"||cond.op==">=")
               {
                  data_type type = catalog_file->get_data_type(all_str[3], all_str[cur_id]);
                  size_t data_size = catalog_file->get_attr_size(all_str[3], all_str[cur_id]);
                  if(cond.val[0]=='\'')
                  {
                     if(cond.val[cond.val.size()-1]!='\'') { err_info = "syntax error."; return Error; }
                     if(type!=Char) { err_info = "data type error."; return Error; }
                     if(cond.val.size()-2!=data_size) { err_info = "data size error."; return Error; }
                     if(cond.op=="="||cond.op=="<>") (*condition).push_back(cond);
                     else { err_info = "string can't be compared."; return Error; }
                  }
                  else 
                  {
                     if(type==Char) { err_info = "data type error."; return Error; }
                     bool is_float = false; 
                     int tmp_size = cond.val.size();
                     for(int i = 0; i < tmp_size; i++)
                     if(cond.val[i]=='.') { is_float = true; break; }
                     if(is_float==true)
                     {
                        if(type!=Float) { err_info = "data type error."; return Error; }
                        (*condition).push_back(cond);
                     }
                     else (*condition).push_back(cond);
                  }
               }
               else { err_info = "op not exists."; return Error; }
               cur_id += 4;
         }
         return Select;
}
cmd_type parse_insert(std::vector<std::string> &all_str)
{
         int size = all_str.size();
         if(size<=6) { err_info = "command not integral."; return Error; }
         to_insert_table_name = new std::string;
         to_insert_record = new Record;
         (*to_insert_table_name) = all_str[2];
         if(catalog_file->check_table_if_exist(all_str[2])==false) { err_info = "table not exists."; return Error; }
         if(size!=6+catalog_file->get_attr_num(all_str[2])) { err_info = "command not integral."; return Error; }
         if(all_str[4]!="(") { err_info = "syntax error."; return Error; }
         if(all_str[size-1]!=")") {  err_info = "syntax error."; return Error; }
         All_Attr *all_attr = catalog_file->get_all_attr(all_str[2]);
         int attr_num = all_attr->attr_info.size();
         for(int i = 0; i < attr_num; i++)
         {
                  std::string attr_val = all_str[5+i];
                  data_type type = all_attr->attr_info[i].type;
                  size_t data_size = all_attr->attr_info[i].data_size;
                  if(attr_val[0]=='\'')
                  {
                     if(attr_val[attr_val.size()-1]!='\'') { err_info = "syntax error."; return Error; }
                     if(type!=Char) { err_info = "data type error."; return Error; }
                     if(attr_val.size()-2!=data_size) { err_info = "data size error."; return Error; }
                  }
                  else 
                  {
                     if(type==Char) { err_info = "data type error."; return Error; }
                     bool is_float = false; 
                     int tmp_size = attr_val.size();
                     for(int i = 0; i < tmp_size; i++)
                     if(attr_val[i]=='.') { is_float = true; break; }
                     if(is_float==true&&type!=Float) { err_info = "data type error."; return Error; }
                  }
         }
         for(int i = 0; i < attr_num; i++)
         if(all_str[i+5][0]=='\'')
         {
                all_str[i+5].erase(all_str[i+5].begin());
                all_str[i+5].erase(all_str[i+5].end()-1);
         }
         int cnt_unique = 0;
         bool flag;
         Attr_Set *attr_set = catalog_file->get_all_index_attr(all_str[2]);
         int cnt_index = attr_set->attr_name.size(); 
         for(int i = 0; i < attr_num; i++)
         if(catalog_file->check_attr_if_unique(all_str[2], all_attr->attr_info[i].attr_name)==true)
         {
            cnt_unique++;
            flag = false;    
            for(int j = 0; j < cnt_index; j++)
            if(all_attr->attr_info[i].attr_name==attr_set->attr_name[j]) { flag = true; break; }
            if(flag==false) continue; 
            std::vector<Predicate> cond;
            Predicate tmp;
            tmp.attr_name = all_attr->attr_info[i].attr_name;
            tmp.op = "=";
            tmp.val = all_str[i+5];
            cond.push_back(tmp);
            //Record_Set *record_set = record_manager->select_part_record(all_str[2], cond);
            //if(record_set!=nullptr) { err_info = "conflict with some unique attribute error."; return Error; }
         } 
         if(cnt_unique==cnt_index)
         {  
            for(int i = 0; i < attr_num; i++)
            (*to_insert_record).attr.push_back(all_str[i+5]);
            return Insert;
         }
         else
         {
            Record_Set *all_record = record_manager->select_all_record(all_str[2]);
            for(auto it = all_record->record.begin(); it != all_record->record.end(); it++)
            {
                for(int i = 0; i < attr_num; i++)
                if(catalog_file->check_attr_if_unique(all_str[2], all_attr->attr_info[i].attr_name)==true)
                {
                   if((*it).attr[i]==all_str[i+5]) { err_info = "conflict with some unique attribute error."; return Error; }
                }
            } 
            for(int i = 0; i < attr_num; i++)
            (*to_insert_record).attr.push_back(all_str[i+5]);
            return Insert;
         }
}
cmd_type parse_delete(std::vector<std::string> &all_str)
{
         if(all_str.size()<3) { err_info = "command not integral."; return Error; }
         if(all_str[1]!="from") { err_info = "syntax error."; return Error; }
         to_delete_record_table_name = new std::string;
         (*to_delete_record_table_name) = all_str[2];
         if(catalog_file->check_table_if_exist(all_str[2])==false) { err_info = "table not exists."; return Error; }
         if(all_str.size()==3)
         {
            delete_option = delete_all;
            return Delete;
         }
         delete_option = delete_part;
         if(all_str[3]!="where") { err_info = "syntax error."; return Error; }
         if(all_str.size()<7) { err_info = "command not integral."; return Error; }
         if((all_str.size()-3)%4!=0) { err_info = "syntax error."; return Error; }
         int cur_id = 4;
         int size = all_str.size();
         condition = new std::vector<Predicate>;
         Predicate cond;
         while(true)
         {
               if(cur_id>=size) break;
               cond.attr_name = all_str[cur_id];
               cond.op = all_str[cur_id+1];
               cond.val = all_str[cur_id+2];
               if(cur_id+3<size&&all_str[cur_id+3]!="and") { err_info = "syntax error."; return Error; }
               if(catalog_file->check_attr_if_exist(all_str[2], all_str[cur_id])==false) { err_info = "attribute not exists."; return Error; }
               if(cond.op=="="||cond.op=="<>"||cond.op=="<"||cond.op=="<="||cond.op==">"||cond.op==">=")
               {
                  data_type type = catalog_file->get_data_type(all_str[2], all_str[cur_id]);
                  size_t data_size = catalog_file->get_attr_size(all_str[2], all_str[cur_id]);
                  if(cond.val[0]=='\'')
                  {
                     if(cond.val[cond.val.size()-1]!='\'') { err_info = "syntax error."; return Error; }
                     if(type!=Char) { err_info = "data type error."; return Error; }
                     if(cond.val.size()-2!=data_size) { err_info = "data size error."; return Error; }
                     if(cond.op=="="||cond.op=="<>") (*condition).push_back(cond);
                     else { err_info = "string can't be compared."; return Error; }
                  }
                  else 
                  {
                     if(type==Char) { err_info = "data type error."; return Error; }
                     bool is_float = false; 
                     int tmp_size = cond.val.size();
                     for(int i = 0; i < tmp_size; i++)
                     if(cond.val[i]=='.') { is_float = true; break; }
                     if(is_float==true)
                     {
                        if(type!=Float) { err_info = "data type error."; return Error; }
                        (*condition).push_back(cond);
                     }
                     else (*condition).push_back(cond);
                  }
               }
               else { err_info = "op not exists."; return Error; }
               cur_id += 4;
         }
         return Delete;
}
cmd_type parse_quit(std::vector<std::string> &all_str)
{
         if(all_str.size()!=1) { err_info = "syntax error."; return Error; }
         return Quit;
}
cmd_type parse_execfile(std::vector<std::string> &all_str)
{
         if(all_str.size()!=2) { err_info = "syntax error."; return Error; }
         read_file_name = (char *)all_str[1].c_str();
         return Execfile;
}
cmd_type parse()
{
         std::vector<std::string> all_str;
         std::string cur_str; 
         int pos = 0;
         while(true) 
         {
              while(pos<cnt_char&&(cmd[pos]==' '||cmd[pos]=='\n')) pos++;
              if(pos>=cnt_char) break;
              while(pos<cnt_char&&(cmd[pos]!=' '&&cmd[pos]!='\n')) cur_str += cmd[pos++];
              all_str.push_back(cur_str);
              cur_str.clear();
         }
         for(auto it = all_str.begin(); it != all_str.end(); it++)
         if((*it)[(*it).size()-1]==',') (*it).erase((*it).end()-1);
         if(all_str.size()==0) { err_info = "command not found."; return Error; }
         if(all_str[0]=="create") return parse_create(all_str);
         if(all_str[0]=="drop") return parse_drop(all_str);
         if(all_str[0]=="select") return parse_select(all_str);
         if(all_str[0]=="insert") return parse_insert(all_str);
         if(all_str[0]=="delete") return parse_delete(all_str);
         if(all_str[0]=="quit") return parse_quit(all_str);
         if(all_str[0]=="execfile") return parse_execfile(all_str);
         err_info = "command not exist."; 
         return Error; 
}
int main()
{
    initial();
    while(true)
    {
         cnt_char = 0;
         switch(_read_mode_)
         {
              case keyboard:
              std::cout<<"please enter command to continue..."<<std::endl;
              while(true)
              {
                   c = getchar();
                   if(c==';') break;
                   cmd[cnt_char++] = c;
                   if(cnt_char==cmd_max_size) break; 
              }
              break;
              case file:
              while(true)
              {
                   int len = read(read_fd, &c, sizeof(c));
                   if(len==0) { _read_mode_ = keyboard; close(read_fd); break; }
                   if(c==';') break;
                   cmd[cnt_char++] = c;
                   if(cnt_char==cmd_max_size) break;
              }
         }
         if(cnt_char==0) { std::cout<<"command not integral."<<std::endl; continue; }
         if(cnt_char==cmd_max_size) { std::cout<<"command is too long."<<std::endl; continue; }
         cmd_type Cmd_Type = parse();
         switch(Cmd_Type)
         {
               case Create_Table:
               catalog_file->insert_table_info(table_info);
               catalog_file->insert_attr_info(attr_info);
               index_manager->create_key_index(table_info->table_name, table_info->primary_key);
               std::cout<<"create table success."<<std::endl;
               break;
               case Drop_Table:
               catalog_file->drop_table(*to_drop_table_name);
               catalog_file->drop_attr(*to_drop_table_name);
               std::cout<<"drop table success."<<std::endl;
               break;
               case Create_Index:
               index_manager->create_index(*to_create_index_name, *to_create_index_table_name, *to_create_index_attr_name);
               catalog_file->insert_index_info(to_insert_index_info);
               std::cout<<"create index success."<<std::endl;
               break;
               case Drop_Index:
               index_manager->delete_index(*to_delete_index_name);
               std::cout<<"drop index success."<<std::endl;
               delete to_delete_index_name;
               break;
               case Select:
               Record_Set *record_set;
               All_Attr *all_attr;
               switch(select_option)
               {
                    case select_all:
                    record_set = record_manager->select_all_record(*to_select_table_name);
                    if(record_set==nullptr) std::cout<<"record not found."<<std::endl;
                    else 
                    {
                       all_attr = catalog_file->get_all_attr(*to_select_table_name);
                       for(auto it = all_attr->attr_info.begin(); it != all_attr->attr_info.end(); it++)
                       std::cout<<(*it).attr_name<<" ";
                       std::cout<<std::endl;
                       for(auto it = record_set->record.begin(); it != record_set->record.end(); it++)
                       {
                           for(auto tmp_it = (*it).attr.begin(); tmp_it != (*it).attr.end(); tmp_it++)
                           std::cout<<(*tmp_it)<<" ";
                           std::cout<<std::endl;
                       }
                    }
                    break;
                    case select_part:
                    record_set = record_manager->select_part_record(*to_select_table_name, *condition);
                    if(record_set==nullptr) std::cout<<"record not found."<<std::endl;
                    else 
                    {
                       all_attr = catalog_file->get_all_attr(*to_select_table_name);
                       for(auto it = all_attr->attr_info.begin(); it != all_attr->attr_info.end(); it++)
                       std::cout<<(*it).attr_name<<" ";
                       std::cout<<std::endl;
                       for(auto it = record_set->record.begin(); it != record_set->record.end(); it++)
                       {
                           for(auto tmp_it = (*it).attr.begin(); tmp_it != (*it).attr.end(); tmp_it++)
                           std::cout<<(*tmp_it)<<" ";
                           std::cout<<std::endl;
                       }
                    }
                    break;
               }
               break;
               case Insert:
               record_manager->insert_one_record(*to_insert_table_name, to_insert_record);
               std::cout<<"insert success."<<std::endl;
               break;
               case Delete:
               switch(delete_option)
               {
                    case delete_all:
                    std::cout<<"delete success."<<std::endl<<record_manager->delete_all_record(*to_delete_record_table_name)<<" records been deleted."<<std::endl; 
                    break;
                    case delete_part:
                    std::cout<<"delete success."<<std::endl<<record_manager->delete_part_record(*to_delete_record_table_name, *condition)<<" records been deleted."<<std::endl;
                    break;
               }
               break;
               case Quit:
               delete buffer;
               delete catalog_file;
               delete bpt_file;
               delete bpt_info_file;
               delete index_manager;
               delete record_manager; 
               delete table_file;
               exit(0);
               break;
               case Execfile:
               read_fd = open(read_file_name, O_RDWR);
               if(read_fd==-1) std::cout<<"file not exist."<<std::endl;
               else _read_mode_ = file;
               break;
               case Error:
               std::cout<<err_info<<std::endl;
               break;
         }
    } 
    return 0;
}