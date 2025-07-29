#ifndef UTIL__HXX
#define UTIL__HXX

#include <ROOT/RVec.hxx>
#include <arrow/io/api.h>

#include <string>
#include <vector>
#include <memory>
#include <iostream>

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

template <typename T>
struct RootConversionTraits {};

#define ROOT_ARROW_STL_CONVERSION(c_type, ArrowType_)  \
   template <>                                         \
   struct RootConversionTraits<c_type> {               \
   using ArrowType = ::arrow::ArrowType_;              \
   };

ROOT_ARROW_STL_CONVERSION(bool, BooleanType)
ROOT_ARROW_STL_CONVERSION(std::int8_t, Int8Type)
ROOT_ARROW_STL_CONVERSION(std::int16_t, Int16Type)
ROOT_ARROW_STL_CONVERSION(std::int32_t, Int32Type)
ROOT_ARROW_STL_CONVERSION(std::int64_t, Int64Type)
ROOT_ARROW_STL_CONVERSION(std::uint8_t, UInt8Type)
ROOT_ARROW_STL_CONVERSION(std::uint16_t, UInt16Type)
ROOT_ARROW_STL_CONVERSION(std::uint32_t, UInt32Type)
ROOT_ARROW_STL_CONVERSION(std::uint64_t, UInt64Type)
ROOT_ARROW_STL_CONVERSION(float, FloatType)
ROOT_ARROW_STL_CONVERSION(double, DoubleType)
ROOT_ARROW_STL_CONVERSION(std::string, StringType)

template <typename T>
void fill_vector_from_arrow(std::int32_t entryId, const arrow::ListArray &src, ROOT::RVec<T> &dest) {
  using ArrowType = typename RootConversionTraits<T>::ArrowType;
  using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;

  auto entrySlice = std::static_pointer_cast<arrow::ListArray>(src.Slice(entryId, 1));
  auto entryArray = std::static_pointer_cast<ArrayType>(entrySlice->value_slice(0));
  ROOT::RVec<T> tmp(reinterpret_cast<T *>((void *)entryArray->raw_values()), entryArray->length());
  std::swap(tmp, dest);
}

template <typename T>
void print_vec(const ROOT::RVec<T> &vec) {
  std::cout << "{ ";
  for (unsigned i = 0; i < vec.size(); ++i) {
    std::cout << vec[i];
    if (i < vec.size() - 1)
      std::cout << ", ";
  }
  std::cout << " }" << std::endl;
}

template <typename T>
void print_vec(const std::vector<T> &vec) {
  std::cout << "{ ";
  for (unsigned i = 0; i < vec.size(); ++i) {
    std::cout << vec[i];
    if (i < vec.size() - 1)
      std::cout << ", ";
  }
  std::cout << " }" << std::endl;
}
#endif // UTIL__HXX
