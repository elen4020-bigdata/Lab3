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

#define DEFAULT_DISP_NUM 10

using namespace std;

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

class WordCounter : public MapReduceSort<WordCounter, wcChunk, wcKey, uint64_t, hash_container<wcKey, uint64_t, buffer_combiner, wcHash>>{
    
    char* data;
    uint64_t dataSize;
    uint64_t splitPos;
    uint64_t chunkSize;
    uint64_t line_num;
    string stop_words;
    public:

    explicit WordCounter(char* inputData, uint64_t dataLength, uint64_t chunkSize, string stop_words) :
        data(inputData),
        dataSize(dataLength),
        chunkSize(chunkSize),
        splitPos(0),
        line_num(0),
        stop_words(stop_words){}
    
    void* locate(data_type* str, uint64_t length) const{
        return str->first;
    }

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
            
            //string word(s.first, i);

            if(i > start){
                s.first[i]=0;
                wcKey newWord = {s.first+start};
                emit_intermediate(out, newWord, s.line_num);
            }
        }
    }

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
    char * fileName = "Dracula.txt";
    struct stat finfo;
    int fd;
    fd = open(fileName, O_RDONLY);
    fstat(fd, &finfo);
    char * data = (char *)malloc (finfo.st_size);

    ifstream stopWordsFile;
    stopWordsFile.open("Stop_Words.txt");
    string stop_words;

    while(!stopWordsFile.eof()){
        getline(stopWordsFile, stop_words);
    }
    
    uint64_t r = 0;
    while(r < (uint64_t)finfo.st_size){
        r += pread(fd, data + r, finfo.st_size,r);
    }
    cout << "starting map reduce" << endl;
    vector<WordCounter::keyval> result;
    WordCounter mapReduce(data, finfo.st_size, 56, stop_words);
    mapReduce.run(result);
    cout<<"Results matrix size: "<<result.size()<<endl;
    cout << "completed map reduce" << endl;
    auto printed = 0;
    auto x = 0;
    while(printed < 50){
        if(stop_words.find(result[x].key.first) == std::string::npos){
            printf("%15s - %lu\n", result[x].key.first, result[x].val);
            printed++;
        }
        x++;
    }
    cout << "completed map reduce" << endl;
}