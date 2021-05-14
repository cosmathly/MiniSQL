#ifndef _Data_Convert_
#define _Data_Convert_

#include "API.h"
#include "Catalog_Manager.h"
#include <string>
using namespace std;
class BPT_Node;
class BPT;
class Record;
class BPT_Info;
class Data_Convert
{
      public:
         Data_Convert() { }
         ~Data_Convert() { }
         void parse_into_string(vector<string> & all_str, char *parse_start_place, size_t len);
         BPT_Node * parse_bpt_node(char *parse_start_place, size_t len); 
         BPT * parse_bpt(char *parse_start_place, size_t len);
         Record * parse_record(char *parse_start_place, size_t len);
         Table_Info * parse_table_info(char *parse_start_place, size_t len);
         Attr_Info * parse_attr_info(char *parse_start_place, size_t len);
         Index_Info * parse_index_info(char *parse_start_place, size_t len);
         BPT_Info * parse_bpt_info(char *parse_start_place, size_t len);
         void to_char(char *st_pos, string *str);
         char * reverse_parse_bpt_node(BPT_Node *bpt_node, size_t *len);
         char * reverse_parse_bpt(BPT *bpt, size_t *len);
         char * reverse_parse_record(Record *record, size_t *len);
         char * reverse_parse_table_info(Table_Info *table_info, size_t *len);
         char * reverse_parse_attr_info(Attr_Info *attr_info, size_t *len);
         char * reverse_parse_index_info(Index_Info *index_info, size_t *len);
         char * reverse_parse_bpt_info(BPT_Info *bpt_info, size_t *len);
};

#endif