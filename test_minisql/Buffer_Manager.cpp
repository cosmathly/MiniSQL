#include "Buffer_Manager.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>
Block::Block()
{
       fd = -1;
       offset_file = -1;
       if_write = false;
       last_use_time = -1;
       if_pin = false;
}
Buffer::Buffer(int Block_Num)
{
        this->Block_Num = Block_Num;
        this->buffer = new Block[Block_Num];
        this->free_block_num = Block_Num;
}
Buffer::~Buffer()
{ 
        for(int i = 0; i < Block_Num; i++)
        if(buffer[i].if_write==true) write_to_file(i);
        delete [] buffer;
}
int Buffer::find_not_used_block()
{
            for(int i = 0; i < Block_Num; i++)
            if(buffer[i].fd==-1) return i;
}
int Buffer::find_LRU_block()
{        
            time_t min_time = buffer[0].last_use_time;
            int LRU_block_id = 0;
            for(int i = 1; i < Block_Num; i++)
            if(buffer[i].if_pin==false&&buffer[i].last_use_time<min_time)
            {
               min_time = buffer[i].last_use_time;
               LRU_block_id = i;
            }
            return LRU_block_id;
}
int Buffer::write_to_file(int block_id)
{  
            int ret = lseek(buffer[block_id].fd, buffer[block_id].offset_file, SEEK_SET);
            if(ret==-1) return -1;
            ret = write(buffer[block_id].fd, buffer[block_id].buf, Block_Size);
            if(ret==-1) return -1;
} 
using namespace std;
int Buffer::Read(File_Descriptor fd, offset_t offset_file, offset_t offset_block, char **read_start_place)
{
            bool if_found = false;
            for(int i = 0; i < Block_Num; i++)
            {    
                if(buffer[i].fd==fd&&buffer[i].offset_file==offset_file)
                {
                   if_found = true;
                   (*read_start_place) = (buffer[i].buf+offset_block);
                   buffer[i].last_use_time = time(NULL);
                   break;
                }
            }
            if(if_found==true) return 0;
            else 
            {
               if(free_block_num>0)
               {
                  int not_used_block_id = find_not_used_block();
                  free_block_num--;
                  int ret = lseek(fd, offset_file, SEEK_SET);
                  if(ret==-1) return -1;
                  ret = read(fd, buffer[not_used_block_id].buf, Block_Size);
                  if(ret==-1) return -1;
                  else if(ret==0) return -1;
                  buffer[not_used_block_id].fd = fd;
                  buffer[not_used_block_id].offset_file = offset_file;
                  buffer[not_used_block_id].if_write = false; 
                  buffer[not_used_block_id].last_use_time = time(NULL);
                  (*read_start_place) = buffer[not_used_block_id].buf+offset_block;
                  return 0;
               }
               else 
               {
                  int LRU_block_id = find_LRU_block();
                  int ret;
                  if(buffer[LRU_block_id].if_write==true) ret = write_to_file(LRU_block_id);
                  if(ret==-1) return -1;
                  ret = lseek(fd, offset_file, SEEK_SET);
                  if(ret==-1) return -1;
                  ret = read(fd, buffer[LRU_block_id].buf, Block_Size);
                  if(ret==-1) return -1;
                  else if(ret==0) return -1;
                  buffer[LRU_block_id].fd = fd;
                  buffer[LRU_block_id].offset_file = offset_file;
                  buffer[LRU_block_id].if_write = false;
                  buffer[LRU_block_id].last_use_time = time(NULL);
                  (*read_start_place) = buffer[LRU_block_id].buf+offset_block;
                  return 0;
               }
            }
}
int Buffer::Write(File_Descriptor fd, offset_t offset_file, offset_t offset_block, char *write_start_place, size_t len)
{
            bool if_found = false;
            for(int i = 0; i < Block_Num; i++)
            {
                if(buffer[i].fd==fd&&buffer[i].offset_file==offset_file)
                {
                   if_found = true;
                   memcpy(buffer[i].buf+offset_block, write_start_place, len);
                   buffer[i].if_write = true;
                   buffer[i].last_use_time = time(NULL);
                   break;
                }
            }
            if(if_found==true) return 0;
            else 
            {
                if(free_block_num>0)
                {
                   int not_used_block_id = find_not_used_block();
                   free_block_num--;
                   int ret = lseek(fd, offset_file, SEEK_SET);
                   if(ret==-1) return -1;
                   ret = read(fd, (void *)buffer[not_used_block_id].buf, Block_Size);
                   if(ret==-1) return -1;
                   buffer[not_used_block_id].fd = fd;
                   buffer[not_used_block_id].offset_file = offset_file;
                   memcpy(buffer[not_used_block_id].buf+offset_block, write_start_place, len);
                   buffer[not_used_block_id].last_use_time = time(NULL);
                   buffer[not_used_block_id].if_write = true;
                   return 0;
                }
                else 
                {
                   int LRU_block_id = find_LRU_block();
                   int ret;
                   if(buffer[LRU_block_id].if_write==true) ret = write_to_file(LRU_block_id);
                   if(ret==-1) return -1;
                   ret = lseek(fd, offset_file, SEEK_SET);
                   if(ret==-1) return -1;
                   ret = read(fd, buffer[LRU_block_id].buf, Block_Size);
                   if(ret==-1) return -1;
                   buffer[LRU_block_id].fd = fd;
                   buffer[LRU_block_id].offset_file = offset_file;
                   memcpy(buffer[LRU_block_id].buf+offset_block, write_start_place, len);
                   buffer[LRU_block_id].last_use_time = time(NULL);
                   buffer[LRU_block_id].if_write = true;
                   return 0;
                }
            }
}