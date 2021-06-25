#ifndef _Interpreter_
#define _Interpreter_

class Predicate;
#include "API.h"
#include "Record_Manager.h"
#include "Index_Manager.h"
#define cmd_max_size 4096
typedef enum Cmd_Type {Create_Table, Drop_Table, Create_Index, Drop_Index, Select, Insert, Delete, Quit, Execfile, Error} cmd_type;
typedef enum Select_Type { select_all, select_part } select_type;
typedef enum Delete_Type { delete_all, delete_part } delete_type;
typedef enum Read_Mode { keyboard, file } read_mode;
bool flag;
Index_Info *to_insert_index_info = nullptr;
char c;
char cmd[cmd_max_size];
std::string cmd_str;
int cnt_char = 0;
char *err_info = nullptr; // 记录全局的错误信息
Data_Convert *data_convert = nullptr;
Buffer *buffer = nullptr;
Catalog_File *catalog_file = nullptr;
BPT_Info_File *bpt_info_file = nullptr;
Table_File *table_file = nullptr;
BPT_File *bpt_file = nullptr;
read_mode _read_mode_;
delete_type delete_option;
std::string *to_delete_record_table_name = nullptr;
char *read_file_name = nullptr;
File_Descriptor read_fd;
select_type select_option;
Record_Manager *record_manager = nullptr;
std::string *to_select_table_name = nullptr;
std::vector<Predicate> *condition = nullptr;
std::string *to_insert_table_name;
Record *to_insert_record = nullptr;
Table_Info *table_info = nullptr;
Index_Info *index_info = nullptr;
Attr_Info *attr_info = nullptr;
std::string *to_drop_table_name = nullptr;
Index_Manager *index_manager = nullptr;
std::string *to_create_index_name = nullptr, *to_create_index_table_name = nullptr, *to_create_index_attr_name = nullptr;
std::string *to_delete_index_name = nullptr;
void initial();
void update_err_info(); 

#endif