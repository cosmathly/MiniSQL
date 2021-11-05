#ifndef _Record_
#define _Record_

#include "API.h"
#include "Buffer_Manager.h"
#include "Index_Manager.h"
//#include "Interpreter.h"
#include "Catalog_Manager.h"
class Record_Manager;
typedef int Attr_Num;
extern Catalog_File *catalog_file;
extern BPT_Info_File *bpt_info_file;
extern Buffer *buffer;
extern BPT_File *bpt_file;
extern Table_File *table_file;
class Predicate
{
      public:
         std::string attr_name;
         std::string op;
         std::string val;
         Predicate() { }
         ~Predicate() { }
};
class Record_Manager
{
      public:
         bool check_if_meet_condition(std::string key1, std::string key2, std::string op, data_type type);
         bool check(std::string table_name, std::vector<Predicate> &condition, Record *record, All_Attr *all_attr);
         void insert_one_record(std::string table_name, Record *des_record);
         void delete_one_record(std::string table_name, Record *des_record);
         Record_Set * select_all_record(std::string table_name);
         Record_Set * select_part_record(std::string table_name, std::vector<Predicate> &condition);
         int delete_all_record(std::string table_name);
         int delete_part_record(std::string table_name, std::vector<Predicate> &condition);
};

#endif