#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "map_reduce.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include<fstream>
#include<string>
#include <chrono>
#include <ctime>

using namespace std;
using namespace std::chrono;

struct wcChunk{
    char* first;
    uint64_t length;
    uint64_t line_num;
};

struct wcKey{
    char* first;

    bool operator<(wcKey const& comp) const{
        return strcmp(first, comp.first) < 0;
    }
    bool operator==(wcKey const& comp) const{
        return strcmp(first, comp.first) == 0;
    }
};

struct wcHash{
    //using Fowler-Noll-Vo hash function
    size_t operator()(wcKey const& key) const{
        char* hash = key.first;
        uint64_t v = 14695981039346656037ULL;
        while(*hash != 0){
            v = (v^(size_t)(*(hash++))) * 1099511628211ULL;
        }
        return v;
    }
};
//Inherit MapReduceSort class from mapReduce Set combiner to buffer type
class WordCounter : public MapReduceSort<WordCounter, wcChunk, wcKey, uint64_t, hash_container<wcKey, uint64_t, buffer_combiner, wcHash>>{
    
    char* data;
    uint64_t dataSize;
    uint64_t splitPos;
    uint64_t line_num;
    public:

    explicit WordCounter(char* inputData, uint64_t dataLength) :
        data(inputData),
        dataSize(dataLength),
        splitPos(0),
        line_num(0){}
    
    void* locate(data_type* str, uint64_t length) const{
        return str->first;
    }

    void map(data_type const& s, map_container& out) const{
        for(auto i = 0; i < s.length; i++){
            s.first[i] = tolower(s.first[i]);
        }
        auto i =0;
        //Getting words out of line obtained from split function
        while(i < s.length){
            while(i < s.length && (s.first[i] < 'a' || s.first[i] > 'z')){
                i++;
            }
            auto start = i;
            while(i < s.length && ((s.first[i] >= 'a' && s.first[i] <= 'z') || s.first[i] == '\'')){
                i++;
            }
            //Emmiting the obtained word and line number for hashing
            if(i > start){
                s.first[i]=0;
                wcKey newWord = {s.first+start};
                emit_intermediate(out, newWord, s.line_num);
            }
        }
    }
    //Split function which is used to split the input file data 
    //into lines of data. Split on line breaks and update line number
    int split(wcChunk& out){
        if((uint64_t)splitPos >= dataSize){
            return 0;
        }
        auto end = splitPos;
        while(end < dataSize && data[end] != '\n'){
            end++;
        }
        out.first = data + splitPos;
        out.length = end - splitPos;
        out.line_num = line_num;
        splitPos = ++end;
        line_num++;
        return 1;
    }

    bool sort(keyval const& a, keyval const& b) const{
        return a.val < b.val || (a.val == b.val && strcmp(a.key.first, b.key.first) > 0);
    }
};

int main(){
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    char * fileName = "Dracula.txt";
    struct stat finfo;
    int fd;
    fd = open(fileName, O_RDONLY);
    fstat(fd, &finfo);
    char * data = (char *)malloc (finfo.st_size);

    ifstream stopWordsFile;
    stopWordsFile.open("Stop_Words.txt");
    string stop_words;

    //Populating string with all the stop words
    getline(stopWordsFile, stop_words);
    
    
    uint64_t r = 0;
    while(r < (uint64_t)finfo.st_size){
        r += pread(fd, data + r, finfo.st_size,r);
    }
    cout << "starting map reduce" << endl;
    vector<WordCounter::keyval> result;
    WordCounter mapReduce(data, finfo.st_size);

    high_resolution_clock::time_point t3 = high_resolution_clock::now();

    mapReduce.run(result);
    
    high_resolution_clock::time_point t4 = high_resolution_clock::now();

    duration<double> time_span1 = duration_cast<duration<double>>(t4 - t3);

    cout<<"Results matrix size: "<<result.size()<<endl;
    cout << "completed map reduce" << endl;
    auto printed = 0;
    auto x = 0;
    //Printing out the results vector while ignoring the stop words
    while(printed < 50){
        if(stop_words.find(result[x].key.first) == std::string::npos){
            printf("%15s - %lu\n", result[x].key.first, result[x].val);
            printed++;
        }
        x++;
    }
    cout << "completed map reduce" << endl;
    high_resolution_clock::time_point t2 = high_resolution_clock::now();

    duration<double> time_span2 = duration_cast<duration<double>>(t2 - t1);

    std::cout << "The map reduction took: " << time_span1.count() << " seconds to run."<<endl;
    std::cout << "The entire program took: " << time_span2.count() << " seconds to run."<<endl;
    free(data);
}