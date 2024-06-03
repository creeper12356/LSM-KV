// void regular_test(uint64_t max)
// 	{
// 		uint64_t i;

// 		// Test a single key_
// 		EXPECT(not_found, store.get(1));
// 		store.put(1, "SE");
// 		EXPECT("SE", store.get(1));
// 		EXPECT(true, store.del(1));
// 		EXPECT(not_found, store.get(1));
// 		EXPECT(false, store.del(1));
#include <iostream>
#include <fstream>
#define MAX 1024
int main() {
    std::ofstream out("script_1024.txt");
    out << "reset\n";
    for (int i = 0; i < MAX; ++i) {
        out << "put " << i << " " << i << "\n";
    }
    // test delete
    for (int i = 0; i < MAX; i += 2) {
        out << "del " << i << "\n";
    }
    for (int i = 0; i < MAX; ++i) {
        out << "get " << i << "\n";
    }
    for (int i = 1; i < MAX; ++i) {
        out << "del " << i << "\n";
    }

    out.close();
    return 0;
}