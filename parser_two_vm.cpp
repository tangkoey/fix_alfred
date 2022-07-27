#include <string>
#include <fstream>
#include <vector>
#include <utility>   // std::pair
#include <stdexcept> // std::runtime_error
#include <sstream>   // std::stringstream
#include <iostream>
#include <string>
#include <regex>
#include <map>
#include <array>
#include <queue>
#include <typeinfo>

#include <zlib.h>

using namespace std;

struct data_e
{
    string instrument;
    string time;
    string orderID;
    string price;
    string quantity;
    string status;
    string side;
    string priority;
    string trade_present;
    string rptSeq;
    string referenceID;
};

struct ps
{
    string price;
    string side;
};


void string_split_optim(vector<string> &output, const string &s, const char delimiter)
{
    output.clear();

    size_t start = 0;
    size_t end = s.find_first_of(delimiter);

    while (end <= std::string::npos)
    {
        output.push_back(s.substr(start, end - start));

        if (end == std::string::npos)
            break;

        start = end + 1;
        end = s.find_first_of(delimiter, start);
    }
};

int main(int argc, char *argv[])
{

    cout << "Started running..." << endl;

    clock_t clkStart;
    clock_t clkFinish;

    data_e data_x;
    ps ps_x;
    vector<string> load_string;

    size_t widx = 0;
    struct gzFile_s *myfile;

    if (argc != 5)
    {
    	cout << "Enter one path with two instruments!" << endl;
        exit(1);
    }

    string fut = argv[2];

    int s = 10000000;
    
    myfile = gzopen(argv[1], "rb");
    if (!myfile)
    {
        cout << "Failed reading." << endl;
        exit(1);
    }

    if (gzbuffer(myfile, 8192))
    {
        cout << "Failed of buffering." << endl;
        exit(1);
    }

    string data;
    vector<string> lines;

    unsigned char *pindata = new unsigned char[s];
    memset(pindata, '\0', sizeof(pindata));
    while (!gzeof(myfile))
    {
        widx = gzread(myfile, voidp(pindata), s);
        for (int i = 0; i < s; i++)
        {
            data += *(pindata + i);
        }
    }

    string_split_optim(lines, data, '\n');
    data = "0";

    cout << "Finished processing raw file." << endl;

    string kv0;
    // ticker, time, order id, price, volume, status, side, priority, trade, trade_id
    int num_order = 0;
    int no_blocks = 0;
    size_t len5799 = 0;
    string f4 = "0";
    
    string wi1 = argv[3];
    string wi2 = argv[4];
    
    size_t dps;
    string 
    dps = raw_file.find_first_of("2");

    string sfn_1 = "parsed_" + fut + "/" + fut + "_orders_" + raw_file.substr(dps, 27) + wi1 + ".txt";
    string sfn_2 = "parsed_" + fut + "/" + fut + "_orders_" + raw_file.substr(dps, 27) + wi2 + ".txt";

    ofstream savefile1;
    savefile1.open(sfn_1.c_str(), ios::app);

    ofstream savefile2;
    savefile2.open(sfn_2.c_str(), ios::app);

    vector<vector<string>> md{};
    vector<vector<string>> order{};

    vector<string> fields{};
    vector<string> key_value{};

    const float block_size = 1e5;
    int counter = 0;
    int block_count = 0;

    clkStart = clock();

    // loop of the block of fetched lines
    // three-level split: raw data -> line : lines -> field : line -> kv : field
    for (auto &line : lines)
    {

        // only those lines starting with [FIX] are order entries
        if (line[0] == '[')
        {
            if (line.substr(57, 5) == "5799=")
            {
                f4 = line.substr(62, 2);

                // example: [FIX] 8=FIXT.1.19=035=X60=20220407-12:50:19.3318316115799=4268=637=841005353643937707=17759690408270=131.8437537706=148=469915279=2269=037=841005353644137707=17759690411270=131.8437537706=148=469915279=2269=037=841005353645437707=17759690430270=131.8437537706=148=469915279=2269=037=841005353649337707=17759690492270=131.8437537706=148=469915279=2269=037=841005353651337707=17759690514270=131.8437537706=148=469915279=2269=037=841005353652237707=17759690525270=131.8437537706=148=469915279=2269=010=000
                // example: [FIX] 8=FIXT.1.19=035=X60=20211129-07:58:38.0426461775799=132268=137=64901326261437707=11264523368270=46135037706=148=8858279=1269=010=000
                if ((f4 == "13" || f4 == "4\x01") && regex_match(line.substr(70, 6), regex("(.*)(37=)(.*)")))
                {
                    string_split_optim(fields, line, '\x01');

                    for (auto &field : fields)
                    {
                        string_split_optim(key_value, field, '=');
                        kv0 = key_value[0];
                        // key sequence: [60, 5799, 268, 37, 37707, 270, 37706, 48, 279, 269]
                        if (kv0 == "[FIX] 8")
                        {
                            continue;
                        }
                        else if (kv0 == "9")
                        {
                            continue;
                        }
                        else if (kv0 == "35")
                        {
                            continue;
                        }
                        else if (kv0 == "60")
                        {
                            data_x.time = key_value[1];
                        }
                        else if (kv0 == "268")
                        { // number of orders on this line
                            num_order = stoi(key_value[1]);

                            data_x.trade_present = "0";
                            data_x.referenceID = "";
                            // 2+7 fields
                        }
                        else if (kv0 == "37")
                        { // order ID
                            data_x.orderID = key_value[1];
                        }
                        else if (kv0 == "37707")
                        { // order priority
                            data_x.priority = key_value[1];
                        }
                        else if (kv0 == "270")
                        { // order price
                            data_x.price = key_value[1];
                        }
                        else if (kv0 == "37706")
                        { // display quantity
                            data_x.quantity = key_value[1];
                        }
                        else if (kv0 == "48")
                        { // instrument code/ticker
                            data_x.instrument = key_value[1];
                        }
                        else if (kv0 == "279")
                        {
                            data_x.status = key_value[1];
                        }
                        else if (kv0 == "269")
                        { // side; message key: [37, 37707, 270, 37706, 269]
                            data_x.side = key_value[1];
                            md.push_back({data_x.instrument, data_x.time, data_x.orderID, data_x.price, data_x.quantity, data_x.status, data_x.side, data_x.priority, data_x.trade_present, data_x.referenceID});
                        }
                    }

                    for (int i = 0; i < md.size(); i++)
                    {
                        if (md[i][0] == wi1)
                        {
                            for (int j = 0; j < 9; j++)
                            {
                                savefile1 << md[i][j];
                                // create csv format manually
                                if (j < 8)
                                {
                                    savefile1 << ",";
                                }
                                else
                                {
                                    savefile1 << "\n";
                                }
                            }
                        }
                        else if (md[i][0] == wi2)
                        {
                            for (int j = 0; j < 9; j++)
                            {
                                savefile2 << md[i][j];
                                // create csv format manually
                                if (j < 8)
                                {
                                    savefile2 << ",";
                                }
                                else
                                {
                                    savefile2 << "\n";
                                }
                            }
                        }
                    }
                    // finished parsing one line (of messages)
                    md.clear();
                }

                // example: [FIX] 8=FIXT.1.19=035=X60=20211129-07:58:38.0340943275799=132268=1270=463525271=1048=885883=1570754346=81023=1279=1269=137705=137=64901329890337707=1126452336737706=19633=137708=010=000
                else if (f4 == "13")
                {
                    string_split_optim(fields, line, '\x01');

                    for (auto &field : fields)
                    {
                        string_split_optim(key_value, field, '=');
                        // key sequence: [60, 5799, 268, 270, 48, 83, 279, 269, 37705, 37, 37707, 37706, 9633, 37708]
                        kv0 = key_value[0];
                        if (kv0 == "[FIX] 8")
                        {
                            continue;
                        }
                        else if (kv0 == "9")
                        {
                            continue;
                        }
                        else if (kv0 == "35")
                        {
                            continue;
                        }
                        else if (kv0 == "60")
                        {
                            data_x.time = key_value[1];
                        }
                        else if (kv0 == "268")
                        { // number of orders on this line
                            num_order = stoi(key_value[1]);
                            data_x.trade_present = "0";
                            data_x.rptSeq = "";
                        }
                        else if (kv0 == "270")
                        { // order price
                            ps_x.price = key_value[1];
                        }
                        else if (kv0 == "48")
                        { // instrument code/ticker
                            data_x.instrument = key_value[1];
                        }
                        else if (kv0 == "269")
                        { // side; message key: [37, 37707, 270, 37706, 269]
                            ps_x.side = key_value[1];
                            order.push_back({ps_x.price, ps_x.side});
                        }
                        else if (kv0 == "37705")
                        { // number of orders
                            num_order = stoi(key_value[1]);
                        }
                        else if (kv0 == "37")
                        { // order ID
                            data_x.orderID = key_value[1];
                        }
                        else if (kv0 == "37707")
                        { // order priority
                            data_x.priority = key_value[1];
                        }
                        else if (kv0 == "37706")
                        { // display quantity
                            data_x.quantity = key_value[1];
                        }
                        else if (kv0 == "9633")
                        { // reference ID
                            data_x.referenceID = key_value[1];
                        }
                        else if (kv0 == "37708")
                        {
                            // no price, no side
                            data_x.status = key_value[1];
                            md.push_back({data_x.instrument, data_x.time, data_x.orderID, "", data_x.quantity, data_x.status, "", data_x.priority, data_x.trade_present, data_x.referenceID});
                        }
                    }
                    // finished fetching one line; write the line of streamed data to output file
                    // number of orders in this message = num_order (key = 268)
                    // only 10 field needed for further analysis
                    for (int i = 0; i < num_order; i++)
                    {
                        if (md[i][0] == wi1)
                        {
                            for (int j = 0; j < 9; j++)
                            {
                                switch (j)
                                {
                                case 3: // price; md[i][10] = referenceID
                                    savefile1 << order[stoi(md[i][9]) - 1][0];
                                case 6: // side
                                    savefile1 << order[stoi(md[i][9]) - 1][1];
                                default:
                                    savefile1 << md[i][j];
                                }
                                // create csv format manually
                                if (j < 8)
                                {
                                    savefile1 << ",";
                                }
                                else
                                {
                                    savefile1 << "\n";
                                }
                            }
                        }
                        else if (md[i][0] == wi2)
                        {
                            for (int j = 0; j < 9; j++)
                            {
                                switch (j)
                                {
                                case 3: // price; md[i][10] = referenceID
                                    savefile2 << order[stoi(md[i][9]) - 1][0];
                                case 6: // side
                                    savefile2 << order[stoi(md[i][9]) - 1][1];
                                default:
                                    savefile2 << md[i][j];
                                }
                                // create csv format manually
                                if (j < 8)
                                {
                                    savefile2 << ",";
                                }
                                else
                                {
                                    savefile2 << "\n";
                                }
                            }
                        }
                    }
                    md.clear();
                    order.clear();
                }

                else if (f4 == "4\x01")
                {

                    string_split_optim(fields, line, '\x01');

                    // fetch field on this line
                    for (auto &field : fields)
                    {
                        // key_value = split(field, '=');
                        string_split_optim(key_value, field, '=');
                        kv0 = key_value[0];
                        // key sequence: [60, 5799, 268, 37, 37707, 270, 37706, 48, 279, 269]
                        if (kv0 == "[FIX] 8")
                        {
                            continue;
                        }
                        else if (kv0 == "9")
                        {
                            continue;
                        }
                        else if (kv0 == "35")
                        {
                            continue;
                        }
                        else if (kv0 == "60")
                        {
                            data_x.time = key_value[1];
                        }
                        else if (kv0 == "268")
                        { // number of orders on this line
                            num_order = stoi(key_value[1]);
                            data_x.trade_present = "0";
                            data_x.rptSeq = "";
                            data_x.referenceID = "";
                        }
                        /////////////////////
                        else if (kv0 == "270")
                        { // order price
                            data_x.price = key_value[1];
                        }
                        else if (kv0 == "48")
                        { // instrument code/ticker
                            data_x.instrument = key_value[1];
                        }
                        else if (kv0 == "279")
                        {
                            data_x.status = key_value[1];
                        }
                        else if (kv0 == "269")
                        { // side; message key: [37, 37707, 270, 37706, 269]
                            data_x.side = key_value[1];
                            md.push_back({data_x.instrument, "", "", data_x.price, "", data_x.status, data_x.side, "", data_x.trade_present, data_x.referenceID});
                        }
                        /////////////////////
                        else if (kv0 == "37")
                        { // order ID
                            data_x.orderID = key_value[1];
                        }
                        else if (kv0 == "37707")
                        { // order priority
                            data_x.priority = key_value[1];
                        }
                        else if (kv0 == "37706")
                        { // display quantity
                            data_x.quantity = key_value[1];
                        }
                    }

                    for (int i = 0; i < md.size(); i++)
                    {
                        if (md[i][0] == wi1)
                        {
                            for (int j = 0; j < 9; j++)
                            {
                                switch (j)
                                {
                                case 1:
                                    savefile1 << data_x.time;
                                    break;
                                case 2:
                                    savefile1 << data_x.orderID;
                                    break;
                                case 4:
                                    savefile1 << data_x.quantity;
                                    break;
                                case 7:
                                    savefile1 << data_x.priority;
                                    break;
                                default:
                                    savefile1 << md[i][j];
                                    break;
                                }
                                // create csv format manually
                                if (j < 8)
                                {
                                    savefile1 << ",";
                                }
                                else
                                {
                                    savefile1 << "\n";
                                }
                            }
                        }
                        else if (md[i][0] == wi2)
                        {
                            for (int j = 0; j < 9; j++)
                            {
                                switch (j)
                                {
                                case 1:
                                    savefile2 << data_x.time;
                                    break;
                                case 2:
                                    savefile2 << data_x.orderID;
                                    break;
                                case 4:
                                    savefile2 << data_x.quantity;
                                    break;
                                case 7:
                                    savefile2 << data_x.priority;
                                    break;
                                default:
                                    savefile2 << md[i][j];
                                    break;
                                }
                                // create csv format manually
                                if (j < 8)
                                {
                                    savefile2 << ",";
                                }
                                else
                                {
                                    savefile2 << "\n";
                                }
                            }
                        }
                    }
                    // finished parsing one line (of messages)
                    md.clear();
                }

                // example: [FIX] 8=FIXT.1.19=035=X60=20220407-12:50:19.3440182415799=1268=1270=0.04296875271=248=9830483=10140059346=25797=2279=0269=237711=7137987537705=237=841005353658832=237=841005351326832=210=000
                else if ((f4 == "1\x01"))
                {
                    data_x.status = "";
                    data_x.side = "";
                    data_x.priority = "";
                    data_x.referenceID = "";

                    string_split_optim(fields, line, '\x01');

                    for (auto &field : fields)
                    {
                        data_x.trade_present = "1";
                        data_x.status = "";
                        data_x.priority = "";
                        data_x.referenceID = "";

                        string_split_optim(key_value, field, '=');
                        kv0 = key_value[0];
                        if (kv0 == "[FIX] 8")
                        {
                            continue;
                        }
                        else if (kv0 == "9")
                        {
                            continue;
                        }
                        else if (kv0 == "35")
                        {
                            continue;
                        }
                        else if (kv0 == "60")
                        {
                            data_x.time = key_value[1];
                        }
                        else if (kv0 == "270")
                        { // order price
                            data_x.price = key_value[1];
                        }
                        else if (kv0 == "48")
                        { // instrument code/ticker
                            data_x.instrument = key_value[1];
                        }
                        else if (kv0 == "5797")
                        { // side of aggressor
                            data_x.side = key_value[1];
                        }
                        else if (kv0 == "37705")
                        { // number of orders
                            num_order = stoi(key_value[1]);
                        }
                        else if (kv0 == "37")
                        { // order ID
                            data_x.orderID = key_value[1];
                        }
                        else if (kv0 == "32")
                        {
                            data_x.quantity = key_value[1];
                            md.push_back({data_x.instrument, data_x.time, data_x.orderID, data_x.price, data_x.quantity, data_x.status, data_x.side, data_x.priority, data_x.trade_present, data_x.referenceID});
                        }
                    }

                    for (int i = 0; i < num_order; i++)
                    {
                        if (md[i][0] == wi1)
                        {
                            for (int j = 0; j < 9; j++)
                            {
                                savefile1 << md[i][j];
                                if (j < 8)
                                {
                                    savefile1 << ",";
                                }
                                else
                                {
                                    savefile1 << "\n";
                                }
                            }
                        }
                        else if (md[i][0] == wi2)
                        {
                            for (int j = 0; j < 9; j++)
                            {
                                savefile2 << md[i][j];
                                if (j < 8)
                                {
                                    savefile2 << ",";
                                }
                                else
                                {
                                    savefile2 << "\n";
                                }
                            }
                        }
                    };
                    md.clear();
                }
                // }

                counter++;
                if (counter % 10000 == 0)
                {
                    cout << "Handled " << counter << " rows so far." << endl;
                    savefile1.flush();
                    savefile2.flush();
                }
            }
        }
    }

    savefile1.close();
    savefile2.close();
    clkFinish = clock();
    std::cout << "Parsing loop finished in: " << (clkFinish - clkStart) / 1000000.00 << "seconds." << endl;

    return 0;
}
