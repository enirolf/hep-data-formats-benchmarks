cmake_minimum_required(VERSION 3.25)

project(RNTupleFmtBench)

find_package(ROOT 6.36 CONFIG REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_library(util SHARED util.cxx util.hxx)
target_link_libraries(util PRIVATE ROOT::Hist)

add_executable(lhcb lhcb.cxx)
target_link_libraries(lhcb PRIVATE util ROOT::RIO ROOT::ROOTDataFrame Arrow::arrow_shared Parquet::parquet_shared)

add_executable(cms cms.cxx)
target_link_libraries(cms PRIVATE util ROOT::RIO ROOT::ROOTDataFrame Arrow::arrow_shared Parquet::parquet_shared)
