#include <iostream>
#include <vector>
#include <string>
#include <filesystem>
#include <regex>
#include <stdlib.h>
#include <fstream>

using std::filesystem::recursive_directory_iterator;
using namespace std;

int main(int argc, char* argv[])
{
    // argv[1] = ES, HG, ZB
    // argv[2], argv[3] = SecurityID
    string data_folder = argv[1];
    string inst1 = argv[3];
    string inst2 = argv[4];
    string fn;
    size_t tp;
    int result;
    
    string mkdir = "mkdir parse_log";
    const char* mkdirx = mkdir.c_str();
    system(mkdirx);

    ofstream ofs("parse_log_" + argv[2] + "_failed_parse_log.txt", ios::app);

    for (const auto &file : recursive_directory_iterator(data_folder))
    {	
    	fn = file.path().string();
        if (regex_match(fn, regex("(LogFile)(.*)")) && stoi(fn.substr(fn.find_first_of("2"), 6)) >= 202203)
        {     
            string cmd = "./parser_two_vm " + fn + " " + argv[2] + " " + inst1 + " " + inst2; 
            const char *cmdx = cmd.c_str();
            result = system(cmdx);
            if (result != 0){
                ofs << fn << "\n"; 
            }
        }
    }
    
    ofs.flush();    
    ofs.close();

    return 0;
}
