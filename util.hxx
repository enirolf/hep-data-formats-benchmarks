#ifndef UTIL__HXX
#define UTIL__HXX

#include <string>
#include <vector>
#include <memory>

#include <TH1D.h>

#include <arrow/api.h>

using AnalysisTime_t = std::pair<std::uint64_t, std::uint64_t>;

enum class FileFormat { rntuple, orc, parquet };

void split_path(std::string_view path, std::string *basename,
                std::string *suffix);
std::string get_path_suffix(std::string_view path);
std::string get_path_basename(std::string_view path);
FileFormat get_file_format(std::string_view suffix);

std::vector<std::string> get_column_names(const std::string &basename);

std::shared_ptr<arrow::Table> open_arrow(const std::string &input_path, FileFormat fmt);

void save_histogram(TH1D *hist, const std::string &output_path);

#endif // UTIL__HXX
