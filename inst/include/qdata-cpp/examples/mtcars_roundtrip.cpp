#include "qdata.h"

#include <fstream>
#include <iostream>

int main(int argc, char** argv) {
    const char* path = argc >= 2 ? argv[1] : "mtcars.qdata";

    if(!std::ifstream(path)) {
        std::cerr << "missing " << path
                  << " (run: Rscript examples/write_mtcars_qdata.R";
        if(argc >= 2) {
            std::cerr << ' ' << path;
        }
        std::cerr << ")\n";
        return 1;
    }

    auto obj = qdata::read(path);
    auto& frame = std::get<qdata::list_vector>(obj.data);
    auto& mpg = std::get<qdata::real_vector>(frame.values[0]->data);

    std::cout << "columns: " << frame.values.size() << "\n";
    std::cout << "rows: " << mpg.values.size() << "\n";
    std::cout << "first mpg: " << mpg.values[0] << "\n";

    qdata::save("mtcars_cpp.qdata", obj);
}
