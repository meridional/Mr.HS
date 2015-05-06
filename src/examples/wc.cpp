/*
 * sequential version of wc in cpp for performance comparison
 */
#include <stdlib.h>
#include <map>
#include <string>
#include <iostream>
#include <fstream>

using namespace std;

int main (int argc, char *argv[]) {
  map<string, int> m;
  cout << argc << endl;
  for (int i = 1; i < argc; i++) {
    ifstream file;
    file.open(argv[i]);
    while (!file.eof()) {
      string tmp;
      file >> tmp;
      if (m.find(tmp) != m.end()) {
        m[tmp] += 1;
      } else {
        m[tmp] = 1;
      }
    }
    file.close();
  }
  map<string, int>::iterator iter;
  for (iter = m.begin(); iter != m.end(); iter++) {
    cout << iter->first << " " << iter->second << endl;
  }
  return 0;
}

