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
void initial();
void update_err_info(); 

#endif