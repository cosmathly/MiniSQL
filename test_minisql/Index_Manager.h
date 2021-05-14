#ifndef _Index_
#define _Index_

#include "Buffer_Manager.h"
#include "Catalog_Manager.h"
#include <bits/stdc++.h>
#include <cstring>
#include <vector>
#include "API.h"
extern BPT_Info_File *bpt_info_file;
extern Catalog_File *catalog_file;
extern Buffer *buffer;
extern BPT_File *bpt_file;
extern Table_File *table_file;
class Index_Manager
{
      public:
      Index_Manager() { }
      ~Index_Manager() { }
      BPT_Info * find_primary_bpt_info(std::string table_name, primary_key key_name);
      void create_index(std::string index_name, std::string table_name, std::string attr_name);
      void delete_index(std::string index_name);
      void create_key_index(std::string table_name, std::string attr_name);
};  
     
#endif