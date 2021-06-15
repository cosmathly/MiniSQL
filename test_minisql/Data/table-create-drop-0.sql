create table person (  height float unique, 
pid int, 
name char(32),
identity char(128) unique,
age int unique,
primary key ( pid )
 );

drop table person;
