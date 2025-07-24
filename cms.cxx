#include <ROOT/RArrowDS.hxx>
#include <ROOT/RDataFrame.hxx>

#include <TCanvas.h>
#include <TH1D.h>
#include <TROOT.h>
#include <TStyle.h>
#include <TSystem.h>

#include <chrono>
#include <iostream>
#include <string>

#include "util.hxx"

static AnalysisTime_t analysis(ROOT::RDataFrame &df, const std::string &histo_path) {
  auto ts_init = std::chrono::steady_clock::now();
  std::chrono::steady_clock::time_point ts_first;
  bool ts_first_set = false;

  auto df_timing = df.Define("TIMING", [&ts_first, &ts_first_set]() {
                       if (!ts_first_set)
                         ts_first = std::chrono::steady_clock::now();
                       ts_first_set = true;
                       return ts_first_set;
                     }).Filter([](bool b) { return b; }, {"TIMING"});
  auto df_2mu =
      df_timing.Filter([](unsigned int s) { return s == 2; }, {"nMuon"});
  auto df_os = df_2mu.Filter(
      [](const ROOT::VecOps::RVec<int> &c) { return c[0] != c[1]; },
      {"Muon_charge"});
  auto df_mass = df_os.Define("Dimuon_mass", ROOT::VecOps::InvariantMass<float>,
                              {"Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass"});
  auto hMass = df_mass.Histo1D<float>(
      {"Dimuon_mass", "Dimuon_mass", 2000, 0.25, 300}, "Dimuon_mass");

  *hMass;
  auto ts_end = std::chrono::steady_clock::now();
  auto runtime_init =
      std::chrono::duration_cast<std::chrono::microseconds>(ts_first - ts_init)
          .count();
  auto runtime_analyze =
      std::chrono::duration_cast<std::chrono::microseconds>(ts_end - ts_first)
          .count();

  if (!histo_path.empty())
    save_histogram(hMass.GetPtr(), histo_path);

  return std::make_pair(runtime_init, runtime_analyze);
}

static void Usage(const char *progname) {
  printf("%s INPUT_PATH [HISTO_PATH]\n", progname);
}

int main(int argc, char **argv) {
  auto ts_init = std::chrono::steady_clock::now();

  if (argc < 2) {
    Usage(argv[0]);
    return 1;
  }

  std::string input_path = argv[1];
  std::string basename, suffix;
  split_path(input_path, &basename, &suffix);
  auto fmt = get_file_format(suffix);

  std::string histo_path{};
  if (argc >= 3)
    histo_path = argv[2];

  AnalysisTime_t runtime_analysis;
  switch (fmt) {
  case FileFormat::rntuple: {
    ROOT::RDataFrame df("Events", input_path);
    runtime_analysis = analysis(df, histo_path);
  } break;
  case FileFormat::orc:
  case FileFormat::parquet: {
    std::shared_ptr<arrow::Table> table = open_arrow(input_path, fmt);
    auto colNames = get_column_names(basename);
    auto df = ROOT::RDF::FromArrow(table, colNames);
    runtime_analysis = analysis(df, histo_path);
  } break;
  default:
    std::cerr << "Invalid file format: " << suffix << std::endl;
    return 1;
  }

  auto ts_end = std::chrono::steady_clock::now();
  auto runtime_main =
      std::chrono::duration_cast<std::chrono::microseconds>(ts_end - ts_init)
          .count();

  std::cout << runtime_analysis.first << ", " << runtime_analysis.second << ", " << runtime_main << std::endl;

  return 0;
}
