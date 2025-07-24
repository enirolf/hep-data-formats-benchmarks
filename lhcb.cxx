#include <ROOT/RArrowDS.hxx>
#include <ROOT/RDataFrame.hxx>

#include <TCanvas.h>
#include <TH1D.h>
#include <TROOT.h>
#include <TStyle.h>
#include <TSystem.h>

#include <iostream>
#include <chrono>
#include <string>
#include <vector>

#include "util.hxx"

constexpr double kKaonMassMeV = 493.677;

static double GetP2(double px, double py, double pz) {
  return px * px + py * py + pz * pz;
}

static double GetKE(double px, double py, double pz) {
  double p2 = GetP2(px, py, pz);
  return sqrt(p2 + kKaonMassMeV * kKaonMassMeV);
}

static AnalysisTime_t analysis(ROOT::RDataFrame &frame, const std::string &histo_path) {
  auto ts_init = std::chrono::steady_clock::now();
  std::chrono::steady_clock::time_point ts_first;

  auto fn_muon_cut_and_stopwatch = [&](unsigned int slot, ULong64_t entry,
                                       int is_muon) {
    if (entry == 0) {
      ts_first = std::chrono::steady_clock::now();
    }
    return !is_muon;
  };
  auto fn_muon_cut = [](int is_muon) { return !is_muon; };
  auto fn_k_cut = [](double prob_k) { return prob_k > 0.5; };
  auto fn_pi_cut = [](double prob_pi) { return prob_pi < 0.5; };
  auto fn_sum = [](double p1, double p2, double p3) { return p1 + p2 + p3; };
  auto fn_mass = [](double B_E, double B_P2) {
    double r = sqrt(B_E * B_E - B_P2);
    return r;
  };

  auto df_muon_cut = frame
                         .Filter(fn_muon_cut_and_stopwatch,
                                 {"rdfslot_", "rdfentry_", "H1_isMuon"})
                         .Filter(fn_muon_cut, {"H2_isMuon"})
                         .Filter(fn_muon_cut, {"H3_isMuon"});
  auto df_k_cut = df_muon_cut.Filter(fn_k_cut, {"H1_ProbK"})
                      .Filter(fn_k_cut, {"H2_ProbK"})
                      .Filter(fn_k_cut, {"H3_ProbK"});
  auto df_pi_cut = df_k_cut.Filter(fn_pi_cut, {"H1_ProbPi"})
                       .Filter(fn_pi_cut, {"H2_ProbPi"})
                       .Filter(fn_pi_cut, {"H3_ProbPi"});
  auto df_mass = df_pi_cut.Define("B_PX", fn_sum, {"H1_PX", "H2_PX", "H3_PX"})
                     .Define("B_PY", fn_sum, {"H1_PY", "H2_PY", "H3_PY"})
                     .Define("B_PZ", fn_sum, {"H1_PZ", "H2_PZ", "H3_PZ"})
                     .Define("B_P2", GetP2, {"B_PX", "B_PY", "B_PZ"})
                     .Define("K1_E", GetKE, {"H1_PX", "H1_PY", "H1_PZ"})
                     .Define("K2_E", GetKE, {"H2_PX", "H2_PY", "H2_PZ"})
                     .Define("K3_E", GetKE, {"H3_PX", "H3_PY", "H3_PZ"})
                     .Define("B_E", fn_sum, {"K1_E", "K2_E", "K3_E"})
                     .Define("B_m", fn_mass, {"B_E", "B_P2"});
  auto hMass = df_mass.Histo1D<double>({"B_mass", "", 500, 5050, 5500}, "B_m");

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
    ROOT::RDataFrame df("DecayTree", input_path);
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
