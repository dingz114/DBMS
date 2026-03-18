CXX = g++
CXXFLAGS = -std=c++17 -g -Isrc
FLEX = flex
BISON = bison

all: minidb

minidb: src/main.o src/dbms.o src/storage.o src/lexer.o src/parser.o
	$(CXX) $(CXXFLAGS) -o $@ $^

src/parser.cpp src/parser.h: src/parser.y
	$(BISON) -d -o src/parser.cpp --defines=src/parser.h $<

src/lexer.cpp src/lexer.h: src/lexer.l src/parser.h
	$(FLEX) --header-file=src/lexer.h -o src/lexer.cpp $<

src/main.o: src/main.cpp src/parser.h src/lexer.h src/dbms.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

src/dbms.o: src/dbms.cpp src/dbms.h src/storage.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

src/storage.o: src/storage.cpp src/storage.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

src/lexer.o: src/lexer.cpp src/lexer.h src/parser.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

src/parser.o: src/parser.cpp src/parser.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f src/*.o src/lexer.cpp src/lexer.h src/parser.cpp src/parser.h minidb