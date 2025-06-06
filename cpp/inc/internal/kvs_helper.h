#include <sstream>
#include <string>

uint32_t parse_hash_adler32(std::istream& in);
uint32_t calculate_hash_adler32(const std::string& data);
std::array<uint8_t,4> get_hash_bytes_adler32(uint32_t hash);
std::array<uint8_t,4> get_hash_bytes(const std::string& data);
