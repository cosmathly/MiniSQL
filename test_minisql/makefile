minisql:API.o Buffer_Manager.o Catalog_Manager.o Index_Manager.o Record_Manager.o Interpreter.o Data_Convert.o
	g++ $^ -o minisql -std=c++11 -w -g
%.o:%.cpp 
	g++ -o $@ -c $^ -std=c++11 -w -g