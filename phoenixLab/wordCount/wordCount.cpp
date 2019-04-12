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
#include <chrono>
#include <ctime>

#define DEFAULT_DISP_NUM 10

using namespace std;
using namespace std::chrono;
//struct that will hold the data that is sent to each bin
struct wcChunk{
    char* first;
    uint64_t length;
};
//struct that holds a single element that is worked on
struct wcKey{
    char* first;

    bool operator<(wcKey const& comp) const{
        return strcmp(first, comp.first) < 0;
    }
    bool operator==(wcKey const& comp) const{
        return strcmp(first, comp.first) == 0;
    }
};
//struct that holds the hash function used in map reduce
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

class WordCounter : public MapReduceSort<WordCounter, wcChunk, wcKey, uint64_t, hash_container<wcKey, uint64_t, sum_combiner, wcHash>>{
    
    //all data
    char* data;
    //size of data
    uint64_t dataSize;
    //current position in data 
    uint64_t splitPos;
    public:

    explicit WordCounter(char* inputData, uint64_t dataLength) :
        data(inputData),
        dataSize(dataLength),
        splitPos(0){}
    //tells map reduce how to get to the data that is to be worked on
    void* locate(data_type* str, uint64_t length) const{
        return str->first;
    }
    //piece of code executed by each bin. What you want to be distributed
    void map(data_type const& s, map_container& out) const{
        for(auto i = 0; i < s.length; i++){
            s.first[i] = tolower(s.first[i]);
        }
        auto i =0;
        while(i < s.length){
            while(i < s.length && (s.first[i] < 'a' || s.first[i] > 'z')){
                i++;
            }
            auto start = i;
            while(i < s.length && ((s.first[i] >= 'a' && s.first[i] <= 'z') || s.first[i] == '\'')){
                i++;
            }
            if(i > start){
                s.first[i]=0;
                wcKey newWord = {s.first+start};
                emit_intermediate(out, newWord, 1);
            }
        }
    }

    //how to split the original data to send to each bin
    int split(wcChunk& out){
        //check we haven't reached the end of the data
        if((uint64_t)splitPos >= dataSize){
            return 0;
        }
        auto end = splitPos;
        //grab a single line from the file
        while(end < dataSize && 
            data[end] != '\t' &&
            data[end] != '\r' && data[end] != '\n'){
            end++;
        }
        //update location in file and put the line into the chunk
        out.first = data + splitPos;
        out.length = end - splitPos;
        splitPos = ++end;
        return 1;
    }

    bool sort(keyval const& a, keyval const& b) const{
        return a.val < b.val || (a.val == b.val && strcmp(a.key.first, b.key.first) > 0);
    }
};

int main(int argc, char *argv[]){
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    char * fileName = argv[1];
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
    while(printed < result.size()){
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