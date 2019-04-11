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

#define DEFAULT_DISP_NUM 10

using namespace std;
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
        return a.val > b.val || (a.val == b.val && strcmp(a.key.first, b.key.first) > 0);
    }
};

int main(){
    char * fileName = "Dracula.txt";
    ifstream stopFile;
    stopFile.open("stopwords.txt");
    string temp;
    vector<string> stopWords;
    while(getline(stopFile, temp)){
        int length = temp.size();
        stopWords.push_back(temp.substr(0, length - 1));
    }
    struct stat finfo;
    int fd;
    fd = open(fileName, O_RDONLY);
    fstat(fd, &finfo);
    char * data = (char *)malloc (finfo.st_size);

    uint64_t r = 0;
    while(r < (uint64_t)finfo.st_size){
        r += pread(fd, data + r, finfo.st_size,r);
    }
    vector<WordCounter::keyval> result;
    WordCounter mapReduce(data, finfo.st_size, 1024);
    mapReduce.run(result);
    auto print = 0;
    auto index = 0;
    while(print < 50{
        auto isStop = false;
        for(auto x = 0; x < stopWords.size(); x++){
            if(stopWords.at(x) == string(result[index].key.first)){
                isStop = true;
            }
        }
        if(!isStop){
            printf("%15s - %lu\n", result[index].key.first, result[index].val);
            print++;
        }
        index++;
    }
}