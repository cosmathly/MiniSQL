#ifndef _Buffer_
#define _Buffer_

#define Block_Size 4096
#include <string.h>
#include <time.h>
#include <stddef.h>
typedef char block_t;
typedef int File_Descriptor;
typedef int offset_t;
class Block
{
      public:
         File_Descriptor fd; // 块所对应的文件的文件描述符
         offset_t offset_file; // 块在文件当中的起始位置    
         bool if_write;
         block_t buf[Block_Size];
         time_t last_use_time;
         bool if_pin;
         Block();
         ~Block() { }
};
class Buffer
{
      public:
         int Block_Num; // 缓冲区所含块的数量
         Block *buffer; // 缓冲区
         Buffer(int Block_Num = 64); // 默认块的数量为64个
         int free_block_num; // 当前空闲块的数量
         ~Buffer();
         int write_to_file(int block_id);
         int Read(File_Descriptor fd, offset_t offset_file, offset_t offset_block, char **read_start_place);
         /* 将指定位置的len个字节写入到文件 */
         int Write(File_Descriptor fd, offset_t offset_file, offset_t offset_block, void *write_start_place, size_t len);
         int find_not_used_block();
         int find_LRU_block();
};

#endif